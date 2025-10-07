import os, logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, MapType, LongType
from pyspark.sql.functions import (
    col, from_json, explode, to_timestamp, to_utc_timestamp,
    coalesce, lit, row_number
)
from pyspark.sql.window import Window

# Optional: load .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# Logging setup
logging.basicConfig(level="INFO")
log = logging.getLogger("consumer")

# -----------------
# Environment variables
# -----------------
bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
topic     = os.getenv("KAFKA_TOPIC", "stock_intraday_5min")

pg_host = os.getenv("POSTGRES_HOST", "postgres")
pg_port = os.getenv("POSTGRES_PORT", "5432")
pg_db   = os.getenv("POSTGRES_DB", "market_pulse")
pg_user = os.getenv("POSTGRES_USER", "admin")
pg_pwd  = os.getenv("POSTGRES_PASSWORD", "admin")
table   = os.getenv("TARGET_TABLE", "public.events_stream")

CHECKPOINT = os.getenv("STREAM_CHECKPOINT", "/opt/spark/work-dir/checkpoints/stock_stream_daily")

jdbc_opts = {
    "url": f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}",
    "dbtable": table,
    "user": pg_user,
    "password": pg_pwd,
    "driver": "org.postgresql.Driver"
}

# -----------------
# Spark session
# -----------------
spark = SparkSession.builder.appName("StockKafkaToPostgres").getOrCreate()
spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

# -----------------
# Schema for AlphaVantage Daily Prices + ingested_at
# -----------------
schema = StructType([
    StructField("Meta Data", MapType(StringType(), StringType()), True),
    StructField("Time Series (Daily)", MapType(StringType(), MapType(StringType(), StringType())), True),
    StructField("ingested_at", LongType(), True),
])

# -----------------
# Read from Kafka
# -----------------
raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap) \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

parsed = raw.selectExpr("CAST(value AS STRING) AS value_str", "timestamp AS kafka_ts") \
            .select(from_json(col("value_str"), schema).alias("obj"), col("kafka_ts")) \
            .filter(col("obj").isNotNull())

meta = col("obj").getField("Meta Data")
ts_daily = col("obj").getField("Time Series (Daily)")
ingested_at = col("obj").getField("ingested_at")

rows = parsed.select(
    meta.alias("meta"),
    ingested_at.alias("ingested_at"),
    explode(ts_daily).alias("bar_date_str", "m")
)

symbol = col("meta").getItem("2. Symbol")
tz     = coalesce(col("meta").getItem("5. Time Zone"), lit("US/Eastern"))
bar_ts = to_utc_timestamp(to_timestamp(col("bar_date_str"), "yyyy-MM-dd"), tz)

out = rows.select(
    symbol.alias("symbol"),
    bar_ts.alias("bar_time"),
    col("m").getItem("1. open").cast("double").alias("open"),
    col("m").getItem("2. high").cast("double").alias("high"),
    col("m").getItem("3. low").cast("double").alias("low"),
    col("m").getItem("4. close").cast("double").alias("close"),
    col("m").getItem("5. volume").cast("double").alias("volume"),
    to_timestamp(col("ingested_at")).alias("ingested_at"),
    lit("alpha_vantage").alias("src")
)

# -----------------
# Using raw output for streaming
# -----------------
stream_df = out

# -----------------
# Write each batch to Postgres
# -----------------
def write_batch_to_postgres(df, batch_id: int):
    try:
        if df.rdd.isEmpty():
            log.info(f"Batch {batch_id}: empty, skipping.")
            return

        # Deduplicate
        dedup = df.dropDuplicates(["symbol", "bar_time"])

        rows = dedup.count()
        log.info(f"Batch {batch_id}: writing {rows} row(s) to {table}.")
        dedup.write \
             .format("jdbc") \
             .options(**jdbc_opts) \
             .mode("append") \
             .save()
    except Exception as e:
        import traceback
        log.error(f"JDBC write failed in batch {batch_id}: {e}\n{traceback.format_exc()}")

# -----------------
# Start streaming
# -----------------
query = stream_df.writeStream \
    .trigger(processingTime="5 minute") \
    .foreachBatch(write_batch_to_postgres) \
    .option("checkpointLocation", CHECKPOINT) \
    .outputMode("append") \
    .start()

query.awaitTermination()
