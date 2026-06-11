# ENGR 5785G - Stream processing assignment
# Scenario B - hospital patient monitoring
# tumbling window of 2 min, watermark 3 min
# alert if avg heart rate > 100 in 2 windows in a row (not just one spike)

import os
import sys

# point pyspark/hadoop at the bundled winutils.exe so local file streaming works on windows
_HADOOP_HOME = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "hadoop")
os.environ["HADOOP_HOME"] = _HADOOP_HOME
os.environ["PATH"] = os.path.join(_HADOOP_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")

# make sure spark spawns python workers using this same venv interpreter,
# not whatever "python" resolves to on PATH (otherwise PYTHON_VERSION_MISMATCH)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, TimestampType, BooleanType
)
from pyspark.sql.streaming.state import GroupStateTimeout

# schema for the csv files, has to match prepare_dataset.py output
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("patient_id", StringType(), True),
    StructField("heart_rate", IntegerType(), True),
    StructField("spo2", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("bp_systolic", IntegerType(), True),
    StructField("bp_diastolic", IntegerType(), True),
    StructField("fall_detection", StringType(), True),
    StructField("predicted_disease", StringType(), True),
    StructField("heart_rate_alert", StringType(), True),
])

# state we keep between windows for each patient
state_schema = StructType([
    StructField("prev_avg_hr", DoubleType(), True),
    StructField("prev_window_end", StringType(), True),
    StructField("alert_count", IntegerType(), True),
])

# schema for the intermediate "windowed averages" files written by query 1
# and read back by the alert query - lets us keep the window aggregation
# (one stateful operator) and the applyInPandasWithState alert logic
# (a second stateful operator) as two separate streaming queries, since
# spark doesn't allow chaining two stateful operators in a single query.
windowed_schema = StructType([
    StructField("patient_id", StringType(), True),
    StructField("window_end", TimestampType(), True),
    StructField("avg_heart_rate", DoubleType(), True),
    StructField("reading_count", LongType(), True),
    StructField("min_hr", IntegerType(), True),
    StructField("max_hr", IntegerType(), True),
])

# what comes out of the stateful function
alert_schema = StructType([
    StructField("patient_id", StringType(), False),
    StructField("window_end", StringType(), False),
    StructField("avg_heart_rate", DoubleType(), False),
    StructField("prev_avg_hr", DoubleType(), True),
    StructField("alert", BooleanType(), False),
    StructField("alert_message", StringType(), True),
])

THRESHOLD = 100.0  # bpm


# this runs once per patient per microbatch
# applyInPandasWithState calls this with: (grouping key as a tuple,
# an iterator of pandas DataFrames for this group, the GroupState)
# and expects an iterator of pandas DataFrames back.
def detect_sustained_alert(key, pdf_iter, state):
    patient_id = key[0]

    if state.exists:
        prev_avg_hr, prev_window_end, alert_count = state.get
    else:
        prev_avg_hr, prev_window_end, alert_count = None, None, 0

    out_rows = []
    dfs = list(pdf_iter)

    if dfs:
        pdf = pd.concat(dfs, ignore_index=True).sort_values("window_end")

        for _, row in pdf.iterrows():
            cur_avg = float(row["avg_heart_rate"])
            cur_win = str(row["window_end"])

            this_high = cur_avg > THRESHOLD
            prev_high = (prev_avg_hr is not None) and (prev_avg_hr > THRESHOLD)

            # the actual alert condition - both windows have to be high
            sustained = this_high and prev_high

            if sustained:
                alert_count += 1
                msg = (f"CLINICAL ALERT - patient {patient_id} avg HR "
                       f"{cur_avg:.1f} bpm (prev window was {prev_avg_hr:.1f}) "
                       f"- sustained over 2 windows, window ending {cur_win}")
            else:
                if not this_high:
                    alert_count = 0
                msg = None

            out_rows.append({
                "patient_id": patient_id,
                "window_end": cur_win,
                "avg_heart_rate": round(cur_avg, 2),
                "prev_avg_hr": round(prev_avg_hr, 2) if prev_avg_hr is not None else None,
                "alert": sustained,
                "alert_message": msg,
            })

            prev_avg_hr = cur_avg
            prev_window_end = cur_win

    if dfs or state.exists:
        state.update((prev_avg_hr, prev_window_end, alert_count))
        state.setTimeoutDuration(10 * 60 * 1000)  # 10 min, just so old patients dont sit in state forever

    yield pd.DataFrame(out_rows, columns=[
        "patient_id", "window_end", "avg_heart_rate",
        "prev_avg_hr", "alert", "alert_message",
    ])


def main():
    # default paths if you don't pass args, just run python src/patient_monitor_stream.py
    input_dir = sys.argv[1] if len(sys.argv) > 1 else "data/stream_input"
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "data/stream_output"
    checkpoint_dir = sys.argv[3] if len(sys.argv) > 3 else "data/checkpoints"

    spark = (
        SparkSession.builder
        .appName("ICU_PatientMonitor_5785G")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("starting patient monitor stream...")
    print("watching:", input_dir)

    raw = (
        spark.readStream
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load(input_dir)
    )

    # tumbling 2 min window, avg heart rate per patient
    windowed = (
        raw
        .withWatermark("timestamp", "3 minutes")
        .groupBy(
            F.window("timestamp", "2 minutes"),
            F.col("patient_id")
        )
        .agg(
            F.avg("heart_rate").alias("avg_heart_rate"),
            F.count("*").alias("reading_count"),
            F.min("heart_rate").alias("min_hr"),
            F.max("heart_rate").alias("max_hr"),
        )
        .select(
            "patient_id",
            F.col("window.end").alias("window_end"),
            "avg_heart_rate",
            "reading_count",
            "min_hr",
            "max_hr",
        )
    )

    # query 1 - just dump the windowed averages so we can see whats going on
    q1 = (
        windowed.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("numRows", "25")
        .option("checkpointLocation", checkpoint_dir + "/summary")
        .queryName("WindowedSummary")
        .start()
    )

    # query 1b - also write the finalized windows (append mode, after watermark
    # closes them) to a folder, so the alert query below can read them as a
    # fresh stream. this is what lets us have two stateful operators
    # (the window aggregation, and the applyInPandasWithState alert logic)
    # without chaining them in a single query.
    intermediate_dir = os.path.join(os.path.dirname(input_dir), "_windowed_intermediate")
    q_intermediate = (
        windowed.writeStream
        .outputMode("append")
        .format("csv")
        .option("header", "true")
        .option("path", intermediate_dir)
        .option("checkpointLocation", checkpoint_dir + "/windowed_intermediate")
        .queryName("WindowedIntermediate")
        .start()
    )

    windowed_stream = (
        spark.readStream
        .format("csv")
        .option("header", "true")
        .schema(windowed_schema)
        .load(intermediate_dir)
    )

    # check for sustained alert (2 windows in a row over threshold)
    alerts = (
        windowed_stream
        .groupBy("patient_id")
        .applyInPandasWithState(
            detect_sustained_alert,
            alert_schema,
            state_schema,
            "Update",
            GroupStateTimeout.ProcessingTimeTimeout,
        )
    )

    alerts_only = alerts.filter(F.col("alert") == True)

    # query 2 - the actual alerts, this is the one we screenshot
    q2 = (
        alerts_only.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", checkpoint_dir + "/alerts_console")
        .queryName("ClinicalAlerts_Console")
        .start()
    )

    # query 3 - also save alerts to csv just in case
    # (csv sink doesn't support "update" mode, which applyInPandasWithState
    # requires here, so write each microbatch out with foreachBatch instead)
    def write_alerts_csv(batch_df, batch_id):
        if batch_df.head(1):
            batch_df.write.mode("append").option("header", "true").csv(output_dir)

    q3 = (
        alerts_only.writeStream
        .outputMode("update")
        .foreachBatch(write_alerts_csv)
        .option("checkpointLocation", checkpoint_dir + "/alerts_file")
        .queryName("ClinicalAlerts_File")
        .start()
    )

    print("queries running:")
    for q in spark.streams.active:
        print(" -", q.name)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
