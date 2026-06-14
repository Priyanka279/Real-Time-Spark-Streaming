# ENGR 5785G - Stream Processing Assignment

**Scenario B - Hospital Patient Monitoring**

Name: Priyankakumari Gupta  
Student ID: 101008820

Dataset: IoMT Health Monitoring (Kaggle) - used `data/iomt_health_sample.csv`, 50,000 rows, mapped down to 20 patient ids (P001-P020)

Window: Tumbling 2 minute window
Alert: avg heart rate > 100 bpm in 2 consecutive windows for the same patient -> clinical alert

---

## Project structure

```
spark-streaming-assignment/
├── README.md
├── requirements.txt
├── data/
│   ├── iomt_health_sample.csv   <- the dataset, already prepared
│   ├── stream_input/            <- simulator drops files here, spark reads from here
│   ├── stream_output/           <- alert csv output goes here
│   └── checkpoints/             <- spark checkpoints (auto created)
├── screenshot/
│   └── alert_console_output.png   <- screenshot of the alert output firing
└── src/
    ├── patient_monitor_stream.py   <- the actual streaming job
    ├── stream_simulator.py         <- fakes a live feed by dropping csv chunks
    ├── prepare_dataset.py          <- one-time script that made iomt_health_sample.csv
    └── test_pipeline_logic.py      <- batch test of the window/alert logic
```

## What you need

- Python 3.11 (pyspark 4.x doesn't play nicely with Python 3.13+/3.14 yet)
- Java 11 or 17 (pyspark needs this, check with `java -version`)
- pyspark, pandas, openpyxl, pyarrow, numpy - all in requirements.txt

## Setup

```bash
python -m venv venv
venv\Scripts\activate          # on windows
# source venv/bin/activate     # on mac/linux

pip install -r requirements.txt

mkdir data\stream_input data\stream_output data\checkpoints
```

The dataset csv is already in the repo so you don't need to run `prepare_dataset.py` again (that was just used once to convert the original excel file to a proper csv with timestamps).

### Windows-only note

Spark needs `winutils.exe`/`hadoop.dll` to do local file `readStream`/`writeStream` on Windows. These are bundled in `hadoop/bin/` in this repo, and `patient_monitor_stream.py` points `HADOOP_HOME` at that folder automatically - no extra setup needed.

The script also pins `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` to the venv's `python.exe` so Spark's worker processes use the same Python version as the driver (otherwise you can get a `PYTHON_VERSION_MISMATCH` if there's another Python on your PATH).

## Optional: quick batch test first

Before dealing with the streaming stuff, this just runs the same window + alert logic on the static csv so you can check it actually works:

```bash
python src/test_pipeline_logic.py
```

Should print something like:

```
rows loaded: 50000
num windows: ...
sustained alerts (2 windows in a row > 100bpm):
+----------+-------------------+--------------+--------+
|patient_id|window_end         |avg_heart_rate|prev_avg|
...
total alerts: 1003
distinct patients: 5
```

## Running the actual streaming pipeline

Need 2 terminals - one runs the spark job, the other drops csv files into the watch folder to simulate a live stream.

**Terminal 1 - start the streaming job:**

```bash
python src/patient_monitor_stream.py data/stream_input data/stream_output data/checkpoints
```

It'll just sit there printing "watching: data/stream_input" until files start showing up.

**Terminal 2 - start dropping data:**

```bash
python src/stream_simulator.py --source data/iomt_health_sample.csv --output data/stream_input --chunk-size 500 --interval 15
```

This drops a 500 row csv file every 15 seconds. Each batch covers about 8 minutes of patient data (4 tumbling windows), so after ~2-3 batches you should start seeing window aggregates and after that, alerts.

In terminal 1 you'll see two kinds of output:

- `WindowedSummary` - avg heart rate per patient per 2-min window (this is just for sanity checking)
- `ClinicalAlerts_Console` - only fires when alert == true, this is the one to screenshot

Stop everything with Ctrl+C. Alert rows also get written as csv to `data/stream_output/`.

## Screenshot

`screenshot/alert_console_output.png` - screenshot of the `ClinicalAlerts_Console` output, showing `alert=true` rows with the clinical alert messages.

## Why a tumbling window?

The assignment says we need to catch *sustained* high heart rate, not just one bad reading. A tumbling window (non-overlapping, fixed 2 min buckets) gives us clean, separate "slices" of time per patient. Then we just check: was the average over the threshold in this window AND the previous one? If yes -> sustained -> alert.

If I used a sliding window instead, the windows overlap and share data points, so "two consecutive windows" doesn't really mean two independent observations anymore - the same elevated readings could count twice. Tumbling keeps it simple and each window is a genuinely separate measurement period, which makes more sense for a clinical "is this patient in trouble for a while" check.

## Where does state come in?

Two places:

1. **Spark's own windowing state** - because of `withWatermark`, spark has to hold onto events for each (patient, window) combo across multiple micro-batches until the watermark passes the window end + 3 min, then it finalizes and emits the aggregate.

2. **Custom state with `applyInPandasWithState`** - this is the bigger one. To check "high in this window AND high in the previous window" you need to remember what happened in the *last* window, which a normal aggregation can't do (it only sees the current window). So for each patient I keep a small state object: `prev_avg_hr`, `prev_window_end`, `alert_count`. Every time a new window finishes for a patient, the function compares the new avg to the stored previous avg, decides if it's a sustained alert, then overwrites the stored value with the current one for next time. State times out after 10 minutes of inactivity so it doesn't pile up forever.

## Why two streaming queries instead of one?

Spark doesn't allow chaining two stateful operators (the windowed aggregation, and `applyInPandasWithState`) in a single streaming query - it throws an `AnalysisException` if you try. So the pipeline is split into two queries connected through a small intermediate folder:

1. The first query does the windowed aggregation (`withWatermark` + tumbling window) and writes the finished window averages out to `data/_windowed_intermediate/` as CSV (in `append` mode, so only finalized windows get written, after the watermark closes them).
2. A second query reads that folder as its own input stream and runs `applyInPandasWithState` on it to do the sustained-alert check.

Both queries (plus the console/CSV sinks) start one after another in the same script, with `spark.streams.awaitAnyTermination()` at the end so the process stays alive for all of them.
