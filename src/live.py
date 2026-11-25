"""Progressi persistiti e visualizzazioni leggere del notebook."""
from datetime import datetime, timezone
from html import escape
import json

from matplotlib import dates, pyplot as plt

from src.config import utc_now
from src.trips import VEHICLES
from src.windows import read_snapshots


class ProgressLog:
    def __init__(self, paths):
        self.path = paths.local / "progress.jsonl"
        self.records = {}

    def capture(self, queries):
        new = []
        for name, query in queries.items():
            for progress in query.recentProgress:
                batch_id = progress["batchId"]
                metrics = progress.get("observedMetrics", {}).get("archive")
                operators = progress["stateOperators"]
                if not (metrics is not None or progress["numInputRows"] or operators):
                    continue
                key = (name, batch_id)
                if key in self.records:
                    continue
                times = progress.get("eventTime", {})
                record = {
                    "query": name,
                    "batch_id": batch_id,
                    "trigger_at_utc": progress["timestamp"],
                    "observed_at_utc": utc_now().isoformat(),
                    "input_rows": progress["numInputRows"],
                    "archived_rows": metrics["rows"] if metrics is not None else None,
                    "event_min_utc": times.get("min"),
                    "event_max_utc": times.get("max"),
                    "watermark_utc": times.get("watermark"),
                    "rows_dropped_by_watermark": sum(op["numRowsDroppedByWatermark"] for op in operators),
                }
                self.records[key] = record
                new.append(record)

        if new:
            with self.path.open("a", encoding="utf-8") as output:
                for record in new:
                    output.write(json.dumps(record) + "\n")

    def for_query(self, name):
        return [row for (query, _), row in self.records.items() if query == name]


def live_rows(spark, paths, progress, name):
    batches = [row["batch_id"] for row in progress.for_query(name) if row["input_rows"]]
    if not batches:
        return []
    keys = ("window_start", "window_end")
    if name == "sliding":
        keys += ("vehicle_type",)
    files = [f"{paths.snapshots(name)}/batch_id={batch_id}" for batch_id in batches[-5:]]
    snapshots = spark.read.parquet(*files).orderBy("batch_id", *keys)
    latest = {}
    for row in snapshots.toLocalIterator():
        record = row.asDict()
        latest[tuple(record[key] for key in keys)] = record
    return [latest[key] for key in sorted(latest)][-24:]


def report_live(spark, paths, progress):
    generator = progress.for_query("generator")
    sliding = progress.for_query("sliding")
    latest = [rows[-1] for name in ("generator", "tumbling", "sliding") if (rows := progress.for_query(name))]
    return {
        "run_id": paths.run_id,
        "archived_rows": sum(row["archived_rows"] or 0 for row in generator),
        "tumbling": live_rows(spark, paths, progress, "tumbling"),
        "sliding": live_rows(spark, paths, progress, "sliding"),
        "watermark_utc": next((row["watermark_utc"] for row in reversed(sliding) if row["watermark_utc"]), None),
        "rows_dropped_by_watermark": sum(row["rows_dropped_by_watermark"] for row in sliding),
        "progress": latest,
    }


def table_html(rows):
    if not rows:
        return "<p>Nessuna riga disponibile.</p>"

    def display_value(value):
        if value is None:
            return ""
        if isinstance(value, float):
            return f"{value:.4f}"
        if isinstance(value, datetime):
            return value.isoformat(sep=" ", timespec="seconds")
        return str(value)

    columns = list(rows[0])
    header = "".join(f"<th>{escape(key)}</th>" for key in columns)
    body = [
        "<tr>" + "".join(f"<td>{escape(display_value(row.get(key)))}</td>" for key in columns) + "</tr>"
        for row in rows
    ]
    return f"<table><thead><tr>{header}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def trend_figure(tumbling, sliding):
    fig, axes = plt.subplots(2, 1, figsize=(9, 5), layout="constrained")
    axes[0].set(ylabel="Quota (%)", title="Tumbling · quota di mercato")
    axes[1].set(ylabel="€/miglio", title="Sliding · ricavo per miglio", xlabel="Inizio finestra (UTC)")

    def event_start(row):
        value = row["window_start"]
        if isinstance(value, str):
            value = datetime.fromisoformat(value)
        return value.replace(tzinfo=timezone.utc)

    for vehicle, color in zip(VEHICLES, ("#095a86", "#9b7000", "#198152")):
        share_key = f"share_{vehicle.lower()}"
        if tumbling:
            axes[0].plot([event_start(r) for r in tumbling],
                         [float(r[share_key]) for r in tumbling], ".-", color=color, label=vehicle)
        selected = [r for r in sliding if r["vehicle_type"] == vehicle]
        if selected:
            axes[1].plot([event_start(r) for r in selected],
                         [float(r["avg_revenue_per_mile_eur"]) for r in selected], ".-", color=color, label=vehicle)
    for axis in axes:
        axis.xaxis.set_major_formatter(dates.DateFormatter("%H:%M:%S", tz=timezone.utc))
        axis.grid(alpha=0.25)
        if axis.lines:
            axis.legend(loc="best")
    return fig
