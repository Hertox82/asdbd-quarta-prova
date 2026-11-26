"""Avvio, limite di generazione e drenaggio della pipeline."""
from datetime import timezone
from queue import Empty, Queue
from threading import Thread
from time import monotonic, sleep

from src.config import begin_run, hdfs_access, save_manifest, utc_now
from src.live import ProgressLog, StreamingCharts, report_live
from src.trips import prepare_archive, read_trip_stream, start_generator
from src.windows import sliding, start_updates, tumbling


def drain(queries, timeout):
    results = Queue()

    def drain_query(query):
        try:
            query.processAllAvailable()
        except Exception as error:
            results.put((query, error))
        else:
            results.put((query, None))

    for query in queries:
        Thread(target=drain_query, args=(query,), daemon=True).start()
    deadline = monotonic() + timeout
    for _ in queries:
        try:
            query, error = results.get(timeout=max(0, deadline - monotonic()))
        except Empty as exc:
            raise TimeoutError("Drenaggio del Parquet oltre il tempo limite.") from exc
        if error is not None or not query.isActive:
            raise RuntimeError(f"Drenaggio non completato per {query.name}") from error


def archive_summary(trips):
    row = trips.selectExpr(
        "count(*) AS trip_count", "count(DISTINCT trip_id) AS distinct_ids",
        "min(trip_id) AS first_trip_id", "max(trip_id) AS last_trip_id",
        "min(start_time) AS first_event_utc", "max(start_time) AS last_event_utc",
    ).first()
    summary = row.asDict()
    for field in ("first_event_utc", "last_event_utc"):
        if row[field] is not None:
            summary[field] = row[field].replace(tzinfo=timezone.utc).isoformat()
    summary["duplicate_ids"] = row.trip_count - row.distinct_ids
    return summary


def run_streams(spark, config, on_update=None):
    paths, manifest = begin_run(spark, config)
    queries = {}
    progress = ProgressLog(paths)
    charts = StreamingCharts(paths, config.duration_seconds)
    target = config.target_trip_count
    generation_started = None
    failure = None
    try:
        prepare_archive(spark, paths)
        generation_started = monotonic()

        def elapsed():
            return monotonic() - generation_started

        manifest["generation_started_at_utc"] = utc_now().isoformat()
        queries["generator"] = start_generator(spark, config, paths)
        trips = read_trip_stream(spark, paths)
        queries["tumbling"] = start_updates(tumbling(trips), spark, config, paths, "tumbling")
        queries["sliding"] = start_updates(
            sliding(trips, config.watermark_seconds), spark, config, paths, "sliding"
        )

        next_display = monotonic()
        publish_deadline = generation_started + config.duration_seconds + max(
            config.drain_timeout_seconds, config.trigger_seconds + 10
        )
        while True:
            for name, query in queries.items():
                if not query.isActive:
                    raise RuntimeError(f"La query {name} si è fermata: {query.exception()}")
            progress.capture(queries)
            now = monotonic()
            observed = sum(row["archived_rows"] or 0 for row in progress.for_query("generator"))
            manifest["archived_rows_observed"] = observed
            charts.export_due(spark, progress, elapsed)
            if observed >= target:
                break
            if now >= publish_deadline:
                raise TimeoutError(f"Pubblicazione incompleta: confermate {observed} corse su {target}.")
            if on_update and now >= next_display and all(progress.for_query(name) for name in queries):
                on_update(report_live(spark, paths, progress, charts))
                next_display = monotonic() + config.live_seconds
            sleep(0.5)

        # The progress record is emitted after the sink commits. Check the
        # committed Parquet itself before stopping the capped generator.
        trips = spark.read.parquet(paths.trips)
        manifest.update(archive_summary(trips))
        if not (
            observed == manifest["trip_count"] == manifest["distinct_ids"] == target
            and manifest["first_trip_id"] == 0 and manifest["last_trip_id"] == target - 1
            and manifest["last_event_utc"] is not None
        ):
            raise RuntimeError(f"L'archivio HDFS non contiene esattamente gli ID da 0 a {target - 1}.")
        manifest["generation_duration_seconds"] = round(monotonic() - generation_started, 3)
        manifest["generation_verified_at_utc"] = utc_now().isoformat()
        progress.capture(queries)
        charts.export_due(spark, progress, elapsed)
        queries["generator"].stop()
        save_manifest(paths, manifest)
        drain([queries["tumbling"], queries["sliding"]], config.drain_timeout_seconds)
        for name in ("tumbling", "sliding"):
            queries[name].stop()
        progress.capture(queries)

        manifest["input_rows"] = {
            name: sum(row["input_rows"] for row in progress.for_query(name)) for name in queries
        }
        gen = progress.for_query("generator")
        manifest["archived_rows_observed"] = sum(row["archived_rows"] or 0 for row in gen)
        manifest["rate_rows_beyond_target"] = manifest["input_rows"]["generator"] - manifest["archived_rows_observed"]
        manifest["rows_dropped_by_watermark"] = sum(
            row["rows_dropped_by_watermark"] for row in progress.for_query("sliding")
        )
        fs, hadoop_path = hdfs_access(spark, paths.hdfs)
        manifest["checkpoints"] = {
            name: {
                "path": paths.checkpoint(name), "active": query.isActive,
                "commits_present": fs.exists(hadoop_path(paths.checkpoint(name) + "/commits")),
            }
            for name, query in queries.items()
        }
        if not (
            target == manifest["archived_rows_observed"]
            == manifest["input_rows"]["tumbling"] == manifest["input_rows"]["sliding"]
            and all(state["commits_present"] and not state["active"] for state in manifest["checkpoints"].values())
        ):
            raise RuntimeError("I conteggi dell'archivio, dei lettori o gli ID non confermano il drenaggio.")
        manifest["status"] = "DRAINED"
        return paths, manifest, trips
    except BaseException as error:
        failure = error
        manifest.update(status="INCOMPLETE", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        cleanup = []
        for name, query in queries.items():
            try:
                if query.isActive:
                    query.stop()
            except Exception as error:
                cleanup.append(f"{name}: {error}")
        if cleanup:
            manifest.update(status="INCOMPLETE", cleanup_errors=cleanup)
        manifest["streams_stopped_at_utc"] = utc_now().isoformat()
        if manifest["status"] == "INCOMPLETE":
            manifest["finished_at_utc"] = manifest["streams_stopped_at_utc"]
            if generation_started is not None and manifest["generation_duration_seconds"] is None:
                manifest["generation_duration_seconds"] = round(monotonic() - generation_started, 3)
        save_manifest(paths, manifest)
        if cleanup and failure is None:
            raise RuntimeError("Arresto incompleto: " + "; ".join(cleanup))
