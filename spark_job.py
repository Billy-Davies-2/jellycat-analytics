import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from sentiment_utils import analyze_sentiment
from pyspark.sql.types import StringType

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

analyze_udf = udf(analyze_sentiment, StringType())
df = df.withColumn("sentiment", analyze_udf(df.value))

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