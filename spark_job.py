from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from sentiment_utils import analyze_sentiment
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("JellycatSparkBatch").getOrCreate()

# NATS read
df = spark.read.format("io.nats.jetstream.spark.NatsJetstreamSource") \
    .option("nats.servers", "localhost:4222") \
    .optino("nats.stream.name", "jellycat_reviews") \
    .option("nats.consumer.name", "spark_consumer") \
    .option("nats.subject.filter", "jellycat_reviews") \
    .load()

analyze_udf = udf(analyze_sentiment, StringType())
df = df.withColumn("sentiment", analyze_udf(df.value))

# ClickHouse sink
df.write \
    .format("jdbc") \
    .option("url", "jdbc:clickhouse://localhost:8123/default") \
    .option("dbtable", "jellycat_sentiments") \
    .option("user", "default") \
    .option("password", "") \
    .option("driver", "ru.yandex.clickhouse.ClickHouseDriver") \
    .mode("append") \
    .save()

spark.stop()