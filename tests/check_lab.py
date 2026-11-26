"""Le tre verifiche essenziali del laboratorio (un solo processo Spark)."""
from collections import Counter
import csv
from datetime import datetime
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

from pyspark.sql import functions as F

from src.comparison import compare_windows
from src.config import LabConfig, spark_session
from src.demo import export_final_charts, scenario_summary
from src.live import CHART_FILES, ProgressLog, StreamingCharts
from src.trips import TRIP_SCHEMA, make_trips, traffic_phase
from src.windows import sliding, tumbling


class LabChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_01_generation(self):
        cases = [(2, 6000, 1800, 4200), (4, 12000, 3600, 8400), (6, 18000, 5400, 12600),
                 (8, 24000, 7200, 16800), (10, 30000, 9000, 21000)]
        for minutes, target, first, second in cases:
            config = LabConfig.from_minutes(minutes)
            self.assertEqual((config.rows_per_second, config.target_trip_count), (50, target))
            ids = self.spark.createDataFrame([(i,) for i in (0, first - 1, first, second - 1, second, target - 1)], ["id"])
            phases = ids.orderBy("id").select(traffic_phase(F.col("id"), target)).collect()
            self.assertEqual([row[0] for row in phases], [1, 1, 2, 2, 3, 3])
        with self.assertRaisesRegex(ValueError, "Durata non valida"):
            LabConfig.from_minutes(3)
        with self.assertRaisesRegex(ValueError, "50 corse"):
            LabConfig(rows_per_second=10)

        target = LabConfig().target_trip_count
        boundaries = {8999: "NORMAL", 9000: "HIGH_TRAFFIC", 20999: "HIGH_TRAFFIC",
                      21000: "NORMAL", 29999: "NORMAL"}
        ids = self.spark.range(40).union(self.spark.createDataFrame([(key,) for key in boundaries], ["id"]))
        source = ids.selectExpr(
            "id AS value", "timestamp_micros(unix_micros(TIMESTAMP '2026-01-01 00:00:00') + id * 20000) AS timestamp"
        )
        generated = make_trips(source, target)
        trips = generated.orderBy("trip_id").collect()
        self.assertEqual([row.trip_id for row in trips], list(range(40)) + list(boundaries))
        sequences = [tuple(row.vehicle_type for row in trips[start:start + 10]) for start in range(0, 40, 10)]
        for sequence in sequences:
            self.assertEqual(Counter(sequence), {"HVFHV": 6, "Yellow": 3, "Green": 1})
        self.assertGreater(len(set(sequences)), 1)
        self.assertEqual(trips, make_trips(source.repartition(2), target).orderBy("trip_id").collect())
        jitter = {row.value: row.jitter for row in source.selectExpr(
            "value", "pmod(xxhash64(value, 31), 421) - 90 AS jitter"
        ).collect()}
        tariffs = {"HVFHV": (350, 225, 45), "Yellow": (300, 250, 50), "Green": (275, 235, 48)}
        for row in trips:
            self.assertIsNotNone(row.start_time)
            expected_scenario = boundaries.get(row.trip_id, "NORMAL")
            self.assertEqual(row.traffic_scenario, expected_scenario)
            high_traffic = expected_scenario == "HIGH_TRAFFIC"
            self.assertTrue(500 <= row.distance_milli_miles <= (8000 if high_traffic else 15000))
            self.assertTrue(180 <= row.duration_seconds <= 5400)
            base_duration = 180 + row.distance_milli_miles * 165 // 1000 + jitter[row.trip_id]
            duration = base_duration * 3 // 2 if high_traffic else base_duration
            self.assertEqual(row.duration_seconds, min(5400, max(180, duration)))
            base, mile, minute = tariffs[row.vehicle_type]
            cents = base + (row.distance_milli_miles * mile + 500) // 1000 + (row.duration_seconds * minute + 30) // 60
            self.assertEqual(row.revenue_cents, cents)
        phases = scenario_summary(generated, target)
        self.assertEqual([(row["phase"], row["trip_count"]) for row in phases], [(1, 41), (2, 2), (3, 2)])
        self.assertEqual(phases[1]["first_event_utc"], "2026-01-01T00:03:00+00:00")
        self.assertEqual(phases[2]["first_event_utc"], "2026-01-01T00:07:00+00:00")
        self.assertEqual(sum(phase["total_revenue_cents"] for phase in phases), sum(row.revenue_cents for row in trips))
        traffic = [row for row in trips if row.traffic_scenario == "HIGH_TRAFFIC"]
        expected = sum(row.duration_seconds for row in traffic) / 60 / (sum(row.distance_milli_miles for row in traffic) / 1000)
        self.assertAlmostEqual(phases[1]["avg_minutes_per_mile"], expected)

    def test_02_windows(self):
        midnight = datetime(2026, 1, 1)
        trips = self.spark.createDataFrame([
            (0, midnight.replace(second=5), "HVFHV", 1000, 200, 200, "NORMAL"),
            (1, midnight.replace(second=25), "HVFHV", 3000, 400, 900, "HIGH_TRAFFIC"),
            (2, midnight.replace(second=35), "Yellow", 2000, 400, 700, "HIGH_TRAFFIC"),
            (3, midnight.replace(minute=1), "Green", 2500, 500, 1000, "NORMAL"),
        ], TRIP_SCHEMA)
        first, second = tumbling(trips).orderBy("window_start").collect()
        self.assertEqual((first.n_total, first.n_hvfhv, first.n_yellow, first.n_green), (3, 2, 1, 0))
        self.assertAlmostEqual(first.share_hvfhv, 200 / 3)
        self.assertAlmostEqual(first.share_yellow, 100 / 3)
        self.assertEqual(first.share_green, 0)
        self.assertEqual((second.n_total, second.n_green, second.share_green), (1, 1, 100.0))

        sliding_df = sliding(trips)
        rows = sliding_df.collect()
        self.assertEqual(sum(row.n for row in rows), 16)
        combined = next(row for row in rows if row.vehicle_type == "HVFHV" and row.window_start == midnight)
        self.assertEqual((combined.n, combined.revenue_sum_cents, combined.distance_sum_milli_miles), (2, 1100, 4000))
        self.assertAlmostEqual(combined.avg_revenue_per_trip_eur, 5.50)
        self.assertAlmostEqual(combined.avg_revenue_per_mile_eur, 2.75)
        self.assertEqual(sum(row.vehicle_type == "Green" for row in rows), 4)

        key = (F.col("vehicle_type") == "HVFHV") & (F.col("window_start") == midnight)
        modified = sliding_df.withColumn("revenue_sum_cents", F.col("revenue_sum_cents") + F.when(key, 1).otherwise(0))
        mismatch = compare_windows("sliding", sliding_df, modified).where("result != 'PASS'").collect()
        self.assertEqual([(r.result, r.metrics_mismatched) for r in mismatch], [("value_mismatch", "revenue_sum_cents")])

    def test_03_chart_exports(self):
        start = "2026-01-01 00:00:00"
        tumbling = dict(window_start=start, window_end="2026-01-01 00:01:00", batch_id=1,
                        n_hvfhv=6, n_yellow=3, n_green=1, share_hvfhv=60, share_yellow=30, share_green=10)
        sliding = dict(window_start=start, window_end="2026-01-01 00:02:00", batch_id=2,
                       vehicle_type="Green", avg_revenue_per_trip_eur=5, avg_revenue_per_mile_eur=2)
        with TemporaryDirectory() as folder:
            paths = SimpleNamespace(local=Path(folder), run_id="chart-check")
            charts = StreamingCharts(paths, 240)
            progress = ProgressLog(paths)
            charts.merge("tumbling", [tumbling])
            self.assertEqual(charts.exported, {"tumbling": 0, "sliding": 0})
            charts.export_due(None, progress, lambda: 59)
            self.assertFalse((paths.local / "snapshots").exists())
            charts.export_due(None, progress, lambda: 60)
            revised = dict(tumbling, batch_id=2, n_green=2)
            new_window = dict(tumbling, window_start="2026-01-01 00:01:00", window_end="2026-01-01 00:02:00", batch_id=2)
            charts.merge("tumbling", [revised, tumbling, new_window])
            charts.merge("sliding", [dict(sliding, batch_id=1, avg_revenue_per_trip_eur=3), sliding])
            charts.export_due(None, progress, lambda: 122)
            charts.export_due(None, progress, lambda: 122)
            charts.export_due(None, progress, lambda: 181)
            self.assertEqual(charts.rows("tumbling"), [revised, new_window])
            self.assertEqual(charts.rows("sliding"), [sliding])
            directory = paths.local / "snapshots"
            self.assertEqual(sorted(p.name for p in directory.glob("*/*.png")),
                             ["sliding_001.png", "tumbling_001.png", "tumbling_002.png"])
            log = [json.loads(line) for line in (directory / "exports.jsonl").read_text().splitlines()]
            self.assertEqual([record["status"] for record in log], ["exported"] * 3 + ["unchanged"])
            self.assertEqual(log[1]["delay_seconds"], 2)

            for name in CHART_FILES:
                rows = charts.rows(name)
                with (paths.local / f"streaming_{name}.csv").open("w", newline="", encoding="utf-8") as output:
                    writer = csv.DictWriter(output, fieldnames=rows[0].keys())
                    writer.writeheader()
                    writer.writerows(rows)
            export_final_charts(paths)
            for files in CHART_FILES.values():
                for filename in files:
                    self.assertTrue((paths.local / "charts" / filename).read_bytes().startswith(b"\x89PNG"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
