"""Pubblicazione degli artefatti e lettura locale in modalità demo."""
import csv
from datetime import timezone
import json
from uuid import uuid4

from pyspark.sql import functions as F

from src.config import hdfs_access, save_manifest
from src.live import final_figures, save_figure
from src.trips import traffic_phase


def read_csv(paths, name):
    filename = paths.local / f"{name}.csv"
    if not filename.is_file():
        return []
    with filename.open(encoding="utf-8", newline="") as source:
        return list(csv.DictReader(source))


def read_manifest(paths):
    filename = paths.local / "manifest.json"
    if not filename.is_file():
        raise FileNotFoundError(f"Manifest assente per la run {paths.run_id}.")
    return json.loads(filename.read_text(encoding="utf-8"))


def read_progress(paths):
    filename = paths.local / "progress.jsonl"
    if not filename.is_file():
        return []
    with filename.open(encoding="utf-8") as source:
        return [json.loads(line) for line in source]


def scenario_summary(trips, target_trip_count):
    phase = traffic_phase(F.col("trip_id"), target_trip_count).alias("phase")
    rows = (
        trips.groupBy(phase, "traffic_scenario").agg(
            F.count("*").alias("trip_count"),
            (F.avg("distance_milli_miles") / 1000).alias("avg_distance_miles"),
            (F.avg("duration_seconds") / 60).alias("avg_duration_minutes"),
            F.try_divide(F.sum("duration_seconds") / 60, F.sum("distance_milli_miles") / 1000)
            .alias("avg_minutes_per_mile"),
            (F.avg("revenue_cents") / 100).alias("avg_revenue_eur"),
            F.sum("revenue_cents").alias("total_revenue_cents"),
            F.min("start_time").alias("first_event_utc"),
            F.max("start_time").alias("last_event_utc"),
        ).orderBy("phase").collect()
    )
    return [dict(row.asDict(), first_event_utc=row.first_event_utc.replace(tzinfo=timezone.utc).isoformat(),
                 last_event_utc=row.last_event_utc.replace(tzinfo=timezone.utc).isoformat()) for row in rows]


def export_final_charts(paths):
    directory = paths.local / "charts"
    directory.mkdir(exist_ok=True)
    for filename, figure in final_figures(read_csv(paths, "streaming_tumbling"),
                                         read_csv(paths, "streaming_sliding")):
        save_figure(figure, directory / filename)


def publish_outputs(spark, paths, manifest, trips):
    manifest["category_counts"] = {
        row.vehicle_type: row["count"] for row in trips.groupBy("vehicle_type").count().collect()
    }
    manifest["scenario_summary"] = scenario_summary(trips, manifest["target_trip_count"])
    export_final_charts(paths)

    manifest["paths"].update(
        manifest_hdfs=f"{paths.hdfs}/manifest.json",
        progress_hdfs=f"{paths.hdfs}/progress.jsonl",
    )
    fs, hadoop_path = hdfs_access(spark, paths.hdfs)
    progress = (paths.local / "progress.jsonl").read_bytes()
    payload = (json.dumps(manifest, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    for name, data in (("progress.jsonl", progress), ("manifest.json", payload)):
        target = hadoop_path(f"{paths.hdfs}/{name}")
        if fs.exists(target):
            raise FileExistsError(target.toString())
        staging = hadoop_path(f"{paths.hdfs}/._{name}_{uuid4().hex}")
        try:
            stream = fs.create(staging, False)
            try:
                stream.write(bytearray(data))
            finally:
                stream.close()
            if not fs.rename(staging, target):
                raise OSError(f"Pubblicazione fallita per {target}")
        finally:
            if fs.exists(staging):
                fs.delete(staging, False)

    save_manifest(paths, manifest)
