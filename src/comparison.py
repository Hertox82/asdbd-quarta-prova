"""Consolidamento delle revisioni e confronto completo per chiave."""
from pyspark.sql import Window
from pyspark.sql import functions as F

from src.batch import export_csv, run_batch
from src.config import save_manifest, utc_now
from src.demo import publish_outputs
from src.streaming import run_streams
from src.trips import VEHICLES
from src.windows import read_snapshots

WINDOW_KEYS = ("window_start", "window_end")
KEYS = (*WINDOW_KEYS, "vehicle_type")
TOLERANCE = 1e-6
METRICS = {
    "tumbling": (("n", "n_total"), ("market_share_pct",)),
    "sliding": (("n", "revenue_sum_cents", "distance_sum_milli_miles"),
                ("avg_revenue_per_trip_eur", "avg_revenue_per_mile_eur")),
}


def latest_updates(spark, paths, name):
    keys = WINDOW_KEYS if name == "tumbling" else KEYS
    revisions = Window.partitionBy(*keys).orderBy(F.col("batch_id").desc())
    return (
        read_snapshots(spark, paths, name)
        .withColumn("revision", F.row_number().over(revisions))
        .where("revision = 1").drop("revision")
    )


def tumbling_long(frame):
    categories = [
        F.struct(
            F.lit(vehicle).alias("vehicle_type"),
            F.col(f"n_{vehicle.lower()}").alias("n"),
            F.col(f"share_{vehicle.lower()}").alias("market_share_pct"),
        )
        for vehicle in VEHICLES
    ]
    return frame.select(*WINDOW_KEYS, "n_total", F.explode(F.array(*categories)).alias("category")).select(
        *WINDOW_KEYS, "category.*", "n_total"
    )


def compare_windows(name, stream, batch):
    if name == "tumbling":
        stream, batch = tumbling_long(stream), tumbling_long(batch)
    exact, approximate = METRICS[name]
    fields = (*exact, *approximate)
    left = stream.select(*KEYS, *fields, F.lit(True).alias("present")).alias("s")
    right = batch.select(*KEYS, *fields, F.lit(True).alias("present")).alias("b")
    joined = left.join(right, list(KEYS), "full_outer")
    checks = {}
    for field in exact:
        checks[field] = F.col(f"s.{field}") == F.col(f"b.{field}")
    for field in approximate:
        checks[field] = F.abs(F.col(f"s.{field}") - F.col(f"b.{field}")) <= TOLERANCE
    mismatches = F.filter(
        F.array(*[F.when(~F.coalesce(check, F.lit(False)), F.lit(field)) for field, check in checks.items()]),
        lambda field: field.isNotNull(),
    )
    result = (
        F.when(F.col("s.present").isNull(), "missing_stream")
        .when(F.col("b.present").isNull(), "missing_batch")
        .when(F.size(mismatches) > 0, "value_mismatch")
        .otherwise("PASS")
    )
    return joined.select(
        F.lit(name).alias("kind"), *KEYS, result.alias("result"),
        F.when(result == "value_mismatch", F.concat_ws(", ", mismatches)).alias("metrics_mismatched"),
        *[F.col(f"{side}.{field}").alias(f"{field}_{label}")
          for field in fields for side, label in (("s", "stream"), ("b", "batch"))],
    )


def compare_run(spark, paths, manifest, batch):
    stream = {}
    reports = []
    for name, batch_frame in batch.items():
        target = paths.final(f"streaming_{name}")
        latest_updates(spark, paths, name).coalesce(1).write.parquet(target)
        stream[name] = spark.read.parquet(target)
        export_csv(stream[name], paths.local / f"streaming_{name}.csv")
        reports.append(compare_windows(name, stream[name], batch_frame))
    results = reports[0].unionByName(reports[1], allowMissingColumns=True).withColumn(
        "partial_window",
        (F.col("window_start") < F.to_timestamp(F.lit(manifest["first_event_utc"])))
        | (F.col("window_end") > F.to_timestamp(F.lit(manifest["last_event_utc"]))),
    )
    target = paths.final("comparison")
    results.coalesce(1).write.parquet(target)
    results = spark.read.parquet(target)
    export_csv(results, paths.local / "comparison.csv")

    stats = {}
    for row in results.groupBy("kind", "result").count().collect():
        stats.setdefault(row.kind, {})[row.result] = row["count"]
    differences = sum(count for counts in stats.values() for key, count in counts.items() if key != "PASS")
    partial = {
        row.kind: row["count"]
        for row in results.where("partial_window").select("kind", *WINDOW_KEYS).distinct().groupBy("kind").count().collect()
    }
    tumbling_counts = stream["tumbling"].selectExpr(
        "sum(n_total) AS trips",
        "count_if(n_hvfhv + n_yellow + n_green != n_total OR "
        f"abs(share_hvfhv + share_yellow + share_green - 100D) > {TOLERANCE}) AS invalid_windows",
        *[f"sum(n_{v.lower()}) AS {v.lower()}" for v in VEHICLES],
    ).first()
    sliding_count = stream["sliding"].select(F.sum("n")).first()[0]
    checks = {
        "all_keys_match": differences == 0 and all(stats.get(name) for name in METRICS),
        "target_trip_count": manifest["trip_count"] == manifest["target_trip_count"],
        "tumbling_trip_count": tumbling_counts.trips == manifest["trip_count"],
        "tumbling_shares": tumbling_counts.invalid_windows == 0,
        "sliding_membership_count": sliding_count == 4 * manifest["trip_count"],
        "no_duplicate_ids": manifest["duplicate_ids"] == 0,
        "no_watermark_drops": manifest["rows_dropped_by_watermark"] == 0,
    }
    manifest.update(
        status="COMPLETE",
        comparison="PASS" if all(checks.values()) else "FAIL",
        checks=checks,
        differences=differences,
        comparison_rows=stats,
        partial_windows=partial,
        category_counts={vehicle: tumbling_counts[vehicle.lower()] for vehicle in VEHICLES},
        sliding_memberships=sliding_count,
        finished_at_utc=utc_now().isoformat(),
    )
    manifest["paths"]["comparison"] = target
    return results


def run_lab(spark, config, on_update=None):
    paths, manifest, trips = run_streams(spark, config, on_update)
    try:
        batch = run_batch(spark, paths)
        compare_run(spark, paths, manifest, batch)
        publish_outputs(spark, paths, manifest, trips)
    except BaseException as error:
        manifest.update(status="INCOMPLETE", comparison=None, finished_at_utc=utc_now().isoformat(),
                        error=f"{type(error).__name__}: {error}")
        save_manifest(paths, manifest)
        raise
    return paths, manifest, trips
