"""Ricalcolo indipendente degli indicatori con Spark SQL."""
import csv

TUMBLING_SQL = """
WITH counts AS (
    SELECT window(start_time, '1 minute') AS w,
           COUNT(*) AS n_total,
           COUNT_IF(vehicle_type = 'HVFHV') AS n_hvfhv,
           COUNT_IF(vehicle_type = 'Yellow') AS n_yellow,
           COUNT_IF(vehicle_type = 'Green') AS n_green
    FROM trips
    GROUP BY window(start_time, '1 minute')
)
SELECT w.start AS window_start, w.end AS window_end,
       n_total, n_hvfhv, n_yellow, n_green,
       100D * n_hvfhv / n_total AS share_hvfhv,
       100D * n_yellow / n_total AS share_yellow,
       100D * n_green / n_total AS share_green
FROM counts
""".strip()

SLIDING_SQL = """
WITH totals AS (
    SELECT window(start_time, '2 minutes', '30 seconds') AS w,
           vehicle_type,
           COUNT(*) AS n,
           SUM(revenue_cents) AS revenue_sum_cents,
           SUM(distance_milli_miles) AS distance_sum_milli_miles
    FROM trips
    GROUP BY window(start_time, '2 minutes', '30 seconds'), vehicle_type
)
SELECT w.start AS window_start, w.end AS window_end,
       vehicle_type, n, revenue_sum_cents, distance_sum_milli_miles,
       TRY_DIVIDE(revenue_sum_cents, 100D * n) AS avg_revenue_per_trip_eur,
       TRY_DIVIDE(10D * revenue_sum_cents, distance_sum_milli_miles) AS avg_revenue_per_mile_eur
FROM totals
""".strip()


def export_csv(frame, filename):
    order = [key for key in ("kind", "window_start", "window_end", "vehicle_type") if key in frame.columns]
    with filename.open("w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(frame.columns)
        writer.writerows(frame.orderBy(*order).toLocalIterator())


def run_batch(spark, paths):
    spark.read.parquet(paths.trips).createOrReplaceTempView("trips")
    batch = {"tumbling": spark.sql(TUMBLING_SQL), "sliding": spark.sql(SLIDING_SQL)}
    for name, frame in batch.items():
        target = paths.final(f"batch_{name}")
        frame.coalesce(1).write.parquet(target)
        batch[name] = spark.read.parquet(target)
        export_csv(batch[name], paths.local / f"batch_{name}.csv")
    return batch
