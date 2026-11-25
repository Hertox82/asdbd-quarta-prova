"""Pubblicazione degli artefatti e lettura locale in modalità demo."""
import csv
import json
from uuid import uuid4

from matplotlib import pyplot as plt

from src.config import hdfs_access, save_manifest
from src.live import trend_figure


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


def publish_outputs(spark, paths, manifest, trips):
    manifest["category_counts"] = {
        row.vehicle_type: row["count"] for row in trips.groupBy("vehicle_type").count().collect()
    }
    figure_path = paths.local / "trend.png"
    fig = trend_figure(read_csv(paths, "streaming_tumbling"), read_csv(paths, "streaming_sliding"))
    try:
        fig.savefig(figure_path, dpi=135)
    finally:
        plt.close(fig)

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
