# Quarta prova intermedia · Spark Structured Streaming

Il laboratorio simula corse HVFHV, Yellow e Green dalla sorgente Spark `rate` e le salva in Parquet su HDFS. Calcola quote di mercato su finestre tumbling e ricavi medi su finestre sliding, poi verifica gli stessi indicatori in Spark SQL batch.

## Prerequisiti

- Docker Desktop avviato in modalità container Linux, con Docker Compose.
- Un browser per JupyterLab.
- Docker deve avere margine oltre i 6 GiB complessivi dei container. Il laboratorio è stato verificato con circa 7,58 GiB e 12 CPU logiche visibili a Docker. Spark usa `local[4]` e un driver da 2 GiB.

L’ambiente usa Python 3.11.14, PySpark 3.5.8 e Java 17.0.17. Il client Hadoop di Spark è 3.3.4, mentre NameNode e DataNode usano Hadoop 3.4.2.

## Avvio

Dalla radice del repository esegui questi comandi.

```sh
docker compose up -d --build
docker compose ps
```

Quando `notebook`, `namenode` e `datanode` risultano `healthy`, apri `http://127.0.0.1:8888/lab`. Recupera il token della sessione con il comando seguente e inseriscilo quando JupyterLab lo richiede.

```sh
docker compose exec -T notebook jupyter server list
```

Nel file browser di JupyterLab apri `notebooks/04_streaming_lab.ipynb`.

## Nuova simulazione

1. Nella cella di configurazione imposta `RUN_MODE = "run"`.
2. I valori predefiniti sono `duration_seconds=600` e `rows_per_second=10`. Modificali nella stessa cella per una prova più breve o una frequenza diversa.
3. Dal menu **Run** scegli **Run All Cells**. Il notebook assegna un nuovo `run_id`, mostra gli aggiornamenti live e avvia il confronto dopo il drenaggio.

Il target è `duration_seconds * rows_per_second`, pari a 6.000 corse con i valori predefiniti. Il generatore termina dopo la verifica degli ID attesi su HDFS. La durata reale può superare quella nominale per l’avvio di Spark e la pubblicazione dei micro-batch. Dopo lo stop seguono drenaggio e confronto. Se interrompi il kernel, controlla lo stato nel manifest.

## Consultare una run precedente

Nella cella di configurazione imposta `RUN_MODE = "demo"` e riesegui tutte le celle. Con `RUN_ID = None` il notebook apre la run più recente. Per sceglierne un’altra, assegna a `RUN_ID` il nome di una directory in `results/`.

La demo mostra gli artefatti locali, inclusi grafico, tabelle e progressi. Con `INSPECT_HDFS = True` mostra anche un campione di corse Parquet e il riepilogo del confronto letto da HDFS.

## Esiti

- `results/<run_id>/manifest.json` riepiloga configurazione, corse attese (`target_trip_count`), corse archiviate (`trip_count`), durata reale della generazione (`generation_duration_seconds`), checkpoint ed esito.
- `results/<run_id>/comparison.csv` contiene tutte le chiavi, i valori di entrambi i lati e le eventuali differenze. Nella stessa directory trovi le tabelle streaming e batch, `progress.jsonl` e `trend.png`.
- In HDFS, `hdfs://namenode:8020/lab/runs/<run_id>/trips` contiene le corse. `final/` contiene i risultati Parquet e il confronto, mentre `snapshots/` conserva le revisioni streaming.

`PASS` indica che tutte le chiavi e le metriche coincidono e che i controlli globali sono passati. `FAIL` indica differenze nelle chiavi o nei valori, oppure un controllo globale non superato. `INCOMPLETE`, o uno stato senza un confronto concluso nel manifest, indica una run da verificare.

## Arresto

```sh
docker compose down
```

I volumi HDFS e gli export locali restano disponibili. **Attenzione:** `docker compose down -v` elimina i volumi persistenti HDFS e i relativi dati.
