import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "engine-rul-topic")
OUTPUT_DIR = os.getenv("STREAM_OUTPUT_DIR", "/opt/app/runtime/stream_output/parsed")
CHECKPOINT_DIR = os.getenv("STREAM_CHECKPOINT_DIR", "/opt/app/runtime/checkpoints/spark_rul")

Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
Path(CHECKPOINT_DIR).mkdir(parents=True, exist_ok=True)

schema = StructType(
    [
        StructField("unit_number", IntegerType()),
        StructField("time_cycles", IntegerType()),
        StructField("setting_1", DoubleType()),
        StructField("setting_2", DoubleType()),
        StructField("setting_3", DoubleType()),
        StructField("T2", DoubleType()),
        StructField("T24", DoubleType()),
        StructField("T30", DoubleType()),
        StructField("T50", DoubleType()),
        StructField("P2", DoubleType()),
        StructField("P15", DoubleType()),
        StructField("P30", DoubleType()),
        StructField("Nf", DoubleType()),
        StructField("Nc", DoubleType()),
        StructField("epr", DoubleType()),
        StructField("Ps30", DoubleType()),
        StructField("phi", DoubleType()),
        StructField("NRf", DoubleType()),
        StructField("NRc", DoubleType()),
        StructField("BPR", DoubleType()),
        StructField("farB", DoubleType()),
        StructField("htBleed", DoubleType()),
        StructField("Nf_dmd", DoubleType()),
        StructField("PCNfR_dmd", DoubleType()),
        StructField("W31", DoubleType()),
        StructField("W32", DoubleType()),
        StructField("actual_rul", DoubleType()),
        StructField("event_time", StringType()),
    ]
)

spark = (
    SparkSession.builder.appName("rul-spark-stream")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
)


def write_batch(batch_df, epoch_id):
    if batch_df.count() == 0:
        return
    (
        batch_df.coalesce(1)
        .write.mode("append")
        .json(OUTPUT_DIR)
    )
    print(f"[spark] epoch={epoch_id} rows={batch_df.count()} written to {OUTPUT_DIR}")


query = (
    parsed.writeStream.foreachBatch(write_batch)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime="2 seconds")
    .start()
)

query.awaitTermination()
