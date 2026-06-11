# quick batch test to make sure the windowing + alert logic actually
# works before running the whole streaming thing (that takes forever
# to debug since you have to wait for batches to drop)
#
# run: python src/test_pipeline_logic.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType
)
from pyspark.sql.window import Window

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


def main():
    spark = (
        SparkSession.builder
        .appName("PatientMonitor_BatchTest")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load("data/iomt_health_sample.csv")
    )

    print("rows loaded:", df.count())

    # same tumbling window logic as the streaming job
    windowed = (
        df.groupBy(
            F.window("timestamp", "2 minutes"),
            F.col("patient_id")
        )
        .agg(
            F.round(F.avg("heart_rate"), 2).alias("avg_heart_rate"),
            F.count("*").alias("reading_count"),
        )
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
        .orderBy("patient_id", "window_end")
    )

    print("num windows:", windowed.count())

    high = windowed.filter(F.col("avg_heart_rate") > 100)
    print("\nwindows with avg hr > 100:")
    high.show(10, truncate=False)

    # use lag to compare with previous window per patient
    w = Window.partitionBy("patient_id").orderBy("window_end")
    sustained = (
        windowed
        .withColumn("prev_avg", F.lag("avg_heart_rate").over(w))
        .filter((F.col("avg_heart_rate") > 100) & (F.col("prev_avg") > 100))
        .select("patient_id", "window_end", "avg_heart_rate", "prev_avg")
    )

    print("\nsustained alerts (2 windows in a row > 100bpm):")
    sustained.show(20, truncate=False)

    print("total alerts:", sustained.count())
    print("distinct patients:", sustained.select("patient_id").distinct().count())

    spark.stop()


if __name__ == "__main__":
    main()
