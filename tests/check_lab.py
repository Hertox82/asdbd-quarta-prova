"""Le tre verifiche essenziali del laboratorio (un solo processo Spark)."""
from collections import Counter
import csv
from datetime import datetime
import json
import unittest

from pyspark.sql import functions as F

from src.comparison import compare_windows, run_lab
from src.config import LabConfig, spark_session
from src.trips import TRIP_SCHEMA, make_trips
from src.windows import sliding, tumbling


class LabChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_01_generation(self):
        source = self.spark.range(40).selectExpr(
            "id AS value", "TIMESTAMP '2026-01-01 00:00:00' AS timestamp"
        )
        trips = make_trips(source).orderBy("trip_id").collect()
        self.assertEqual([row.trip_id for row in trips], list(range(40)))
        sequences = [tuple(row.vehicle_type for row in trips[start:start + 10]) for start in range(0, 40, 10)]
        for sequence in sequences:
            self.assertEqual(Counter(sequence), {"HVFHV": 6, "Yellow": 3, "Green": 1})
        self.assertGreater(len(set(sequences)), 1)
        self.assertEqual(trips, make_trips(source.repartition(2)).orderBy("trip_id").collect())
        tariffs = {"HVFHV": (350, 225, 45), "Yellow": (300, 250, 50), "Green": (275, 235, 48)}
        for row in trips:
            self.assertIsNotNone(row.start_time)
            self.assertTrue(500 <= row.distance_milli_miles <= 25000)
            self.assertTrue(180 <= row.duration_seconds <= 5400)
            base, mile, minute = tariffs[row.vehicle_type]
            cents = base + (row.distance_milli_miles * mile + 500) // 1000 + (row.duration_seconds * minute + 30) // 60
            self.assertEqual(row.revenue_cents, cents)

    def test_02_windows(self):
        midnight = datetime(2026, 1, 1)
        trips = self.spark.createDataFrame([
            (0, midnight.replace(second=5), "HVFHV", 1000, 200, 200),
            (1, midnight.replace(second=25), "HVFHV", 3000, 400, 900),
            (2, midnight.replace(second=35), "Yellow", 2000, 400, 700),
            (3, midnight.replace(minute=1), "Green", 2500, 500, 1000),
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

    def test_03_smoke(self):
        self.assertEqual(LabConfig().target_trip_count, 6000)
        config = LabConfig(duration_seconds=45)
        paths, state, _ = run_lab(self.spark, config)
        saved = json.loads((paths.local / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(saved, state)
        self.assertEqual((saved["status"], saved["comparison"], saved["differences"]), ("COMPLETE", "PASS", 0))
        target = config.target_trip_count
        self.assertEqual(saved["target_trip_count"], target)
        self.assertEqual(saved["trip_count"], target)
        self.assertEqual(saved["distinct_ids"], target)
        self.assertEqual((saved["first_trip_id"], saved["last_trip_id"]), (0, target - 1))
        self.assertEqual((saved["input_rows"]["tumbling"], saved["input_rows"]["sliding"]), (target, target))
        self.assertGreater(saved["generation_duration_seconds"], 0)
        self.assertFalse(self.spark.streams.active)
        self.assertEqual(self.spark.read.parquet(paths.trips).count(), saved["trip_count"])
        diagnostics = self.spark.read.parquet(paths.final("comparison"))
        self.assertEqual(diagnostics.where("result != 'PASS'").count(), 0)
        with (paths.local / "comparison.csv").open(encoding="utf-8", newline="") as record:
            rows = list(csv.DictReader(record))
        self.assertEqual(len(rows), diagnostics.count())
        self.assertEqual({row["result"] for row in rows}, {"PASS"})
        print(f"Smoke {paths.run_id}: {saved['trip_count']} corse, {len(rows)} chiavi, confronto {saved['comparison']}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
