"""Schema e generazione delle corse con espressioni Spark."""
from pyspark.sql import functions as F

from src.config import hdfs_access

VEHICLES = ("HVFHV", "Yellow", "Green")
TRIP_SCHEMA = """
    trip_id LONG, start_time TIMESTAMP, vehicle_type STRING,
    distance_milli_miles INT, duration_seconds INT, revenue_cents LONG
"""


def make_trips(source):
    value = F.col("value")
    block = F.expr("value DIV 10")
    offset = F.pmod(value, F.lit(10))
    # The coprime factors permute all ten positions within each block.
    factor_index = (F.pmod(F.xxhash64(block, F.lit(53)), F.lit(4)) + 1).cast("int")
    factor = F.element_at(F.array(F.lit(1), F.lit(3), F.lit(7), F.lit(9)), factor_index)
    shift = F.pmod(F.xxhash64(block, F.lit(79)), F.lit(10))
    bucket = F.pmod(factor * offset + shift, F.lit(10))
    distance = 500 + F.pmod(F.xxhash64(value, F.lit(17)), F.lit(24_501))
    jitter = F.pmod(F.xxhash64(value, F.lit(31)), F.lit(421)) - 90

    trips = (
        source.select(
            value.cast("long").alias("trip_id"),
            F.col("timestamp").alias("start_time"),
            F.when(bucket < 6, "HVFHV").when(bucket < 9, "Yellow").otherwise("Green")
            .alias("vehicle_type"),
            distance.cast("int").alias("distance_milli_miles"),
            jitter.cast("int").alias("jitter"),
        )
        .withColumn(
            "duration_seconds",
            F.expr("least(5400, greatest(180, 180 + (distance_milli_miles * 165 DIV 1000) + jitter))")
            .cast("int"),
        )
        .drop("jitter")
    )
    vehicle = F.col("vehicle_type")
    return (
        trips.withColumn("base", F.when(vehicle == "HVFHV", 350).when(vehicle == "Yellow", 300).otherwise(275))
        .withColumn("per_mile", F.when(vehicle == "HVFHV", 225).when(vehicle == "Yellow", 250).otherwise(235))
        .withColumn("per_minute", F.when(vehicle == "HVFHV", 45).when(vehicle == "Yellow", 50).otherwise(48))
        .withColumn(
            "revenue_cents",
            F.expr("base + ((distance_milli_miles * per_mile + 500) DIV 1000)"
                   " + ((duration_seconds * per_minute + 30) DIV 60)"),
        )
        .drop("base", "per_mile", "per_minute")
    )


def rate_source(spark, config):
    return (
        spark.readStream.format("rate")
        .option("rowsPerSecond", config.rows_per_second)
        .option("numPartitions", 1)
        .load()
    )


def prepare_archive(spark, paths):
    fs, hadoop_path = hdfs_access(spark, paths.hdfs)
    if fs.exists(hadoop_path(paths.hdfs)):
        raise FileExistsError(paths.hdfs)
    if not fs.mkdirs(hadoop_path(paths.trips)):
        raise OSError(f"Impossibile creare {paths.trips}")


def start_generator(spark, config, paths):
    source = rate_source(spark, config).where(F.col("value") < config.target_trip_count)
    return (
        make_trips(source).coalesce(1)
        .observe("archive", F.count("*").alias("rows"))
        .writeStream.format("parquet")
        .queryName(f"generator_{paths.run_id}")
        .outputMode("append")
        .option("checkpointLocation", paths.checkpoint("generator"))
        .trigger(processingTime=f"{config.trigger_seconds} seconds")
        .start(paths.trips)
    )


def read_trip_stream(spark, paths):
    return spark.readStream.schema(TRIP_SCHEMA).parquet(paths.trips)
