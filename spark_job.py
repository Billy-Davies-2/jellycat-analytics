import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from sentiment_utils import analyze_sentiment, analyze_sentiment_with_score
from pyspark.sql.types import StringType, DoubleType

app_name = os.getenv("SPARK_APP_NAME", "JellycatSparkBatch")
spark = SparkSession.builder.appName(app_name).getOrCreate()

# NATS read
nats_url = os.getenv("NATS_URL", "nats://localhost:4222").replace("nats://", "")
nats_stream = os.getenv("NATS_STREAM", "jellycat_reviews")
nats_consumer = os.getenv("NATS_CONSUMER", "spark_consumer")
nats_subject = os.getenv("NATS_SUBJECT", "jellycat_reviews")

df = (
    spark.read.format("io.nats.jetstream.spark.NatsJetstreamSource")
    .option("nats.servers", nats_url)
    .option("nats.stream.name", nats_stream)
    .option("nats.consumer.name", nats_consumer)
    .option("nats.subject.filter", nats_subject)
    .load()
)

def _label_from_value(v: bytes) -> str:
    try:
        s = v.decode("utf-8", errors="ignore") if isinstance(v, (bytes, bytearray)) else v
    except Exception:
        s = ""
    return analyze_sentiment(s)

def _score_from_value(v: bytes) -> float:
    try:
        s = v.decode("utf-8", errors="ignore") if isinstance(v, (bytes, bytearray)) else v
    except Exception:
        s = ""
    label, score = analyze_sentiment_with_score(s)
    return float(score)

def _text_from_value(v: bytes) -> str:
    try:
        return v.decode("utf-8", errors="ignore") if isinstance(v, (bytes, bytearray)) else str(v)
    except Exception:
        return ""

label_udf = udf(_label_from_value, StringType())
score_udf = udf(_score_from_value, DoubleType())
text_udf = udf(_text_from_value, StringType())

df = (
        df.withColumn("text", text_udf(df.value))
            .withColumn("sentiment", label_udf(df.value))
            .withColumn("sentiment_score", score_udf(df.value))
            .select("text", "sentiment", "sentiment_score")
)

# ClickHouse sink
clickhouse_url = os.getenv("CLICKHOUSE_URL", "jdbc:clickhouse://localhost:8123/default")
clickhouse_user = os.getenv("CLICKHOUSE_USER", "default")
clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "")

(
    df.write.format("jdbc")
    .option("url", clickhouse_url)
    .option("dbtable", "jellycat_sentiments")
    .option("user", clickhouse_user)
    .option("password", clickhouse_password)
    .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
    .mode("append")
    .save()
)

spark.stop()

def main():
    pass  # Spark job runs on import when container executes

if __name__ == "__main__":
    main()