"""Progressi persistiti e visualizzazioni leggere del notebook."""
from datetime import datetime, timedelta, timezone
from html import escape
import json

from matplotlib import dates, pyplot as plt
from matplotlib.ticker import MaxNLocator

from src.config import utc_now
from src.trips import VEHICLES

CATEGORY_COLORS = dict(zip(VEHICLES, ("#095a86", "#9b7000", "#198152")))
SNAPSHOT_PERIODS = {"tumbling": 60, "sliding": 120}
CHART_FILES = {
    "tumbling": ("tumbling_counts.png", "tumbling_market_share.png"),
    "sliding": ("sliding_revenue_per_trip.png", "sliding_revenue_per_mile.png"),
}


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


class StreamingCharts:
    def __init__(self, paths, duration_seconds):
        self.paths = paths
        self.duration_seconds = duration_seconds
        self.loaded = {name: set() for name in SNAPSHOT_PERIODS}
        self.latest = {name: {} for name in SNAPSHOT_PERIODS}
        self.last_slot = dict.fromkeys(SNAPSHOT_PERIODS, 0)
        self.exported = dict.fromkeys(SNAPSHOT_PERIODS, 0)
        self.last_signature = dict.fromkeys(SNAPSHOT_PERIODS)

    def merge(self, name, revisions):
        keys = ("window_start", "window_end")
        if name == "sliding":
            keys += ("vehicle_type",)
        latest = self.latest[name]
        for row in revisions:
            key = tuple(row[field] for field in keys)
            if key not in latest or row["batch_id"] > latest[key]["batch_id"]:
                latest[key] = row

    def rows(self, name):
        return [row for _, row in sorted(self.latest[name].items())]

    def refresh(self, spark, progress, name):
        batches = {r["batch_id"] for r in progress.for_query(name) if r["input_rows"]} - self.loaded[name]
        if batches:
            files = [f"{self.paths.snapshots(name)}/batch_id={batch}" for batch in sorted(batches)]
            self.merge(name, (row.asDict() for row in spark.read.parquet(*files).collect()))
            self.loaded[name].update(batches)
        return self.rows(name)

    def export_due(self, spark, progress, elapsed):
        for name, period in SNAPSHOT_PERIODS.items():
            slot = int(min(elapsed(), self.duration_seconds) // period)
            if slot <= self.last_slot[name]:
                continue
            first_due = self.last_slot[name] + 1
            self.last_slot[name] = slot
            rows = self.refresh(spark, progress, name)
            batch = max((r["batch_id"] for r in rows), default=None)
            latest_trigger = next((r["trigger_at_utc"] for r in progress.for_query(name)
                                   if r["batch_id"] == batch), None)
            signature = [{k: v for k, v in row.items() if k not in ("batch_id", "processed_at_utc")} for row in rows]
            now = utc_now()
            seconds = elapsed()
            record = {
                "query": name, "scheduled_seconds": slot * period, "coalesced_slots": slot - first_due,
                "elapsed_seconds": round(seconds, 3), "delay_seconds": round(max(0, seconds - slot * period), 3),
                "observed_at_utc": now.isoformat(), "batch_id": batch, "latest_trigger_utc": latest_trigger,
                "batch_age_seconds": (round((now - datetime.fromisoformat(latest_trigger)).total_seconds(), 3)
                                      if latest_trigger else None),
                "status": "unchanged" if rows else "no_data",
            }
            directory = self.paths.local / "snapshots"
            directory.mkdir(exist_ok=True)
            if rows and signature != self.last_signature[name]:
                number = self.exported[name] + 1
                filename = directory / name / f"{name}_{number:03d}.png"
                filename.parent.mkdir(exist_ok=True)
                figure = snapshot_figure(name, rows, self.paths.run_id, number, seconds, now, batch)
                save_figure(figure, filename)
                self.exported[name] = number
                self.last_signature[name] = signature
                record.update(status="exported", file=f"{name}/{filename.name}", exported_at_utc=now.isoformat())
            with (directory / "exports.jsonl").open("a", encoding="utf-8") as log:
                log.write(json.dumps(record) + "\n")


def report_live(spark, paths, progress, charts):
    generator = progress.for_query("generator")
    sliding = progress.for_query("sliding")
    latest = [rows[-1] for name in ("generator", "tumbling", "sliding") if (rows := progress.for_query(name))]
    return {
        "run_id": paths.run_id,
        "archived_rows": sum(row["archived_rows"] or 0 for row in generator),
        "tumbling": charts.refresh(spark, progress, "tumbling"),
        "sliding": charts.refresh(spark, progress, "sliding"),
        "snapshots": charts.exported.copy(),
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


CHART_LABELS = {
    "tumbling": (("Corse per categoria · Tumbling 1 min", "Corse"),
                 ("Quote di mercato · Tumbling 1 min", "Quota (%)")),
    "sliding": (("Ricavo medio per corsa · Sliding 2 min / 30 s", "€/corsa"),
                ("Ricavo medio per miglio · Sliding 2 min / 30 s", "€/miglio")),
}


def _draw_chart(axis, name, index, rows):
    title, unit = CHART_LABELS[name][index]
    time_key = "window_end" if name == "sliding" else "window_start"
    xlabel = "Fine finestra Sliding (UTC)" if name == "sliding" else "Inizio finestra (UTC)"
    axis.set(title=title, xlabel=xlabel, ylabel=unit)
    axis.xaxis.set_major_formatter(dates.DateFormatter("%H:%M:%S", tz=timezone.utc))
    axis.set_axisbelow(True)
    axis.grid(axis="y", alpha=0.22)
    if not rows:
        axis.text(0.5, 0.5, "In attesa di aggiornamenti", ha="center", transform=axis.transAxes)
        return

    times = sorted({row[time_key] for row in rows})
    x = [datetime.fromisoformat(time) if isinstance(time, str) else time for time in times]
    if name == "tumbling":
        by_start = {row["window_start"]: row for row in rows}
        bottom = [0] * len(times)
        for (vehicle, color), reference in zip(CATEGORY_COLORS.items(), (60, 30, 10)):
            key = vehicle.lower()
            if index == 0:
                values = [int(by_start[start][f"n_{key}"]) for start in times]
                axis.bar(x, values, width=timedelta(seconds=52), bottom=bottom, align="edge",
                         color=color, label=vehicle)
                bottom = [total + value for total, value in zip(bottom, values)]
            else:
                axis.plot(x, [float(by_start[start][f"share_{key}"]) for start in times],
                          ".-", color=color, label=vehicle)
                axis.axhline(reference, color=color, linestyle="--", alpha=0.48, linewidth=1)
        if index == 0:
            axis.yaxis.set_major_locator(MaxNLocator(integer=True))
        else:
            axis.set_ylim(0, 100)
    else:
        metric = ("avg_revenue_per_trip_eur", "avg_revenue_per_mile_eur")[index]
        for vehicle, color in CATEGORY_COLORS.items():
            values = {row[time_key]: float(row[metric]) for row in rows if row["vehicle_type"] == vehicle}
            if values:
                axis.plot(x, [values.get(end, float("nan")) for end in times], ".-", color=color, label=vehicle)
        axis.margins(y=0.16)
    axis.legend(loc="best")


def paired_figure(name, rows):
    fig, axes = plt.subplots(2, 1, figsize=(9.5, 6.8), layout="constrained")
    for index, axis in enumerate(axes):
        _draw_chart(axis, name, index, rows)
    return fig


def snapshot_figure(name, rows, run_id, number, seconds, exported_at, batch):
    figure = paired_figure(name, rows)
    figure.suptitle(
        f"{run_id} · Snapshot {number:03d} · {seconds:.1f} s dalla partenza\n"
        f"{exported_at:%Y-%m-%d %H:%M:%S UTC} · Ultimo micro-batch {batch} · Risultati provvisori"
    )
    return figure


def final_figures(tumbling, sliding):
    for name, rows in (("tumbling", tumbling), ("sliding", sliding)):
        if rows:
            for index, filename in enumerate(CHART_FILES[name]):
                fig, axis = plt.subplots(figsize=(9.5, 3.4), layout="constrained")
                _draw_chart(axis, name, index, rows)
                yield filename, fig


def save_figure(figure, filename):
    try:
        figure.savefig(filename, dpi=135)
    finally:
        plt.close(figure)
