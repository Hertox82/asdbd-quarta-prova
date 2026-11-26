"""Configurazione e percorsi isolati per ogni simulazione."""
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import platform
import re
from uuid import uuid4

from pyspark.sql import SparkSession

HDFS_ROOT = "hdfs://namenode:8020/lab/runs"
RESULTS_ROOT = Path(__file__).resolve().parents[1] / "results"
ALLOWED_DURATION_MINUTES = (2, 4, 6, 8, 10)
ROWS_PER_SECOND = 50


def utc_now():
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class LabConfig:
    duration_seconds: int = 600
    rows_per_second: int = ROWS_PER_SECOND
    trigger_seconds: int = 10
    watermark_seconds: int = 120
    live_seconds: int = 25
    drain_timeout_seconds: int = 120

    def __post_init__(self):
        if type(self.duration_seconds) is not int or self.duration_seconds not in (
            minutes * 60 for minutes in ALLOWED_DURATION_MINUTES
        ):
            raise ValueError("Durata non valida. Scegli 2, 4, 6, 8 o 10 minuti.")
        if type(self.rows_per_second) is not int or self.rows_per_second != ROWS_PER_SECOND:
            raise ValueError("La frequenza è fissata a 50 corse al secondo.")
        for key, value in asdict(self).items():
            if type(value) is not int or value < 1:
                raise ValueError(f"{key} deve essere un intero positivo.")

    @classmethod
    def from_minutes(cls, minutes, **settings):
        if type(minutes) is not int or minutes not in ALLOWED_DURATION_MINUTES:
            raise ValueError("Durata non valida. Scegli 2, 4, 6, 8 o 10 minuti.")
        return cls(duration_seconds=minutes * 60, **settings)

    @property
    def target_trip_count(self):
        return self.duration_seconds * self.rows_per_second


@dataclass(frozen=True)
class RunPaths:
    run_id: str

    def __post_init__(self):
        if not re.fullmatch(r"[0-9]{8}_[0-9]{6}_[a-f0-9]{8}", self.run_id):
            raise ValueError("run_id non valido. Selezionare una directory esistente in results/.")

    @classmethod
    def new(cls):
        return cls(f"{utc_now():%Y%m%d_%H%M%S}_{uuid4().hex[:8]}")

    @property
    def hdfs(self):
        return f"{HDFS_ROOT}/{self.run_id}"

    @property
    def trips(self):
        return f"{self.hdfs}/trips"

    @property
    def local(self):
        return RESULTS_ROOT / self.run_id

    def checkpoint(self, name):
        return f"{self.hdfs}/checkpoints/{name}"

    def snapshots(self, name):
        return f"{self.hdfs}/snapshots/{name}"

    def final(self, name):
        return f"{self.hdfs}/final/{name}"


def list_runs():
    if not RESULTS_ROOT.exists():
        return []
    return sorted(
        (path.name for path in RESULTS_ROOT.iterdir() if (path / "manifest.json").is_file()),
        reverse=True,
    )


def spark_session():
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("Quarta prova - Spark Structured Streaming")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.stopTimeout", "30000")
        .config("spark.sql.streaming.numRecentProgressUpdates", "1000")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def hdfs_access(spark, path):
    jvm = spark.sparkContext._jvm
    path_type = jvm.org.apache.hadoop.fs.Path
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    return path_type(path).getFileSystem(conf), path_type


def save_manifest(paths, manifest):
    temp = paths.local / ".manifest.tmp"
    temp.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temp.replace(paths.local / "manifest.json")


def begin_run(spark, config):
    paths = RunPaths.new()
    paths.local.mkdir(parents=True, exist_ok=False)
    jvm = spark.sparkContext._jvm
    manifest = {
        "run_id": paths.run_id,
        "status": "RUNNING",
        "comparison": None,
        "started_at_utc": utc_now().isoformat(),
        "finished_at_utc": None,
        "config": asdict(config),
        "versions": {
            "spark": spark.version,
            "python": platform.python_version(),
            "java": jvm.java.lang.System.getProperty("java.version"),
            "hadoop_client": jvm.org.apache.hadoop.util.VersionInfo.getVersion(),
        },
        "paths": {"hdfs": paths.hdfs, "trips": paths.trips, "local": f"results/{paths.run_id}"},
        "target_trip_count": config.target_trip_count,
        "trip_count": None,
        "generation_duration_seconds": None,
    }
    save_manifest(paths, manifest)
    return paths, manifest
