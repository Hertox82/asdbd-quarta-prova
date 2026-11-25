"""Aggregazioni in event time e persistenza degli aggiornamenti."""
from uuid import uuid4

from pyspark.sql import functions as F

from src.config import hdfs_access, utc_now
from src.trips import VEHICLES


def tumbling(trips):
    counts = [
        F.count(F.when(F.col("vehicle_type") == vehicle, 1)).alias(f"n_{vehicle.lower()}")
        for vehicle in VEHICLES
    ]
    totals = trips.groupBy(F.window("start_time", "1 minute")).agg(
        F.count("*").alias("n_total"), *counts
    )
    shares = [
        (100.0 * F.col(f"n_{vehicle.lower()}") / F.col("n_total")).alias(f"share_{vehicle.lower()}")
        for vehicle in VEHICLES
    ]
    return totals.select(
        F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
        "n_total", *[f"n_{vehicle.lower()}" for vehicle in VEHICLES], *shares,
    )


def sliding(trips, watermark_seconds=120):
    if trips.isStreaming:
        trips = trips.withWatermark("start_time", f"{watermark_seconds} seconds")
    totals = trips.groupBy(F.window("start_time", "2 minutes", "30 seconds"), "vehicle_type").agg(
        F.count("*").alias("n"),
        F.sum("revenue_cents").alias("revenue_sum_cents"),
        F.sum("distance_milli_miles").alias("distance_sum_milli_miles"),
    )
    return totals.select(
        F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
        "vehicle_type", "n", "revenue_sum_cents", "distance_sum_milli_miles",
        F.expr("try_divide(revenue_sum_cents, 100D * n)").alias("avg_revenue_per_trip_eur"),
        F.expr("try_divide(10D * revenue_sum_cents, distance_sum_milli_miles)")
        .alias("avg_revenue_per_mile_eur"),
    )


def snapshot_writer(spark, directory):
    fs, hadoop_path = hdfs_access(spark, directory)
    if not fs.mkdirs(hadoop_path(directory)):
        raise OSError(f"Impossibile creare {directory}")

    def persist(batch, batch_id):
        target = hadoop_path(f"{directory}/batch_id={batch_id}")
        if fs.exists(target):
            # A stateful update still needs to consume the batch on replay.
            batch.count()
            return
        staging = hadoop_path(f"{directory}/._batch_{batch_id}_{uuid4().hex}")
        try:
            (
                batch.withColumn("batch_id", F.lit(batch_id).cast("long"))
                .withColumn("processed_at_utc", F.lit(utc_now()))
                .coalesce(1).write.parquet(staging.toString())
            )
            if not fs.rename(staging, target):
                raise OSError(f"Pubblicazione snapshot fallita per {target}")
        finally:
            if fs.exists(staging):
                fs.delete(staging, True)

    return persist


def start_updates(metrics, spark, config, paths, name):
    return (
        metrics.writeStream.outputMode("update")
        .queryName(f"{name}_{paths.run_id}")
        .option("checkpointLocation", paths.checkpoint(name))
        .trigger(processingTime=f"{config.trigger_seconds} seconds")
        .foreachBatch(snapshot_writer(spark, paths.snapshots(name)))
        .start()
    )


def read_snapshots(spark, paths, name):
    return spark.read.option("recursiveFileLookup", "true").parquet(paths.snapshots(name))
