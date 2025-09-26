import os
from pyflink.datastream import StreamExecutionEnvironment, SourceFunction, RuntimeContext
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.descriptors import Schema, Jdbc, JdbcCatalog
from pyflink.table.udf import udf
import asyncio
from nats.aio.client import Client as NATS
from sentiment_utils import analyze_sentiment

class NatsSourceFunction(SourceFunction):
    def run(self, ctx: RuntimeContext):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
    subject = os.getenv("NATS_SUBJECT", "jellycat_reviews")
    nc = loop.run_until_complete(NATS.connect(nats_url))
    sub = loop.run_until_complete(nc.subscribe(subject))
    while True:
        msg = loop.run_until_complete(nc.next_msg())
        ctx.collect((msg.subject, msg.data.decode()))

    def cancel(self):
        pass # add cleanup

@udf(result_type=DataTypes.STRING())
def flink_analyze_label(text):
    return analyze_sentiment(text)

@udf(result_type=DataTypes.DOUBLE())
def flink_analyze_score(text):
    from sentiment_utils import analyze_sentiment_with_score
    label, score = analyze_sentiment_with_score(text)
    return float(score)

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

# Source
stream = env.add_source(NatsSourceFunction())

"""Note: In a real app, define proper schema descriptors; simplified here."""
from pyflink.table import Table
table = t_env.from_data_stream(stream)
t_env.create_temporary_view("nats_source", table)

# Process
result_table = t_env.from_path("nats_source").select("f0 as text, flink_analyze_label(f0) as sentiment, flink_analyze_score(f0) as sentiment_score")

# ClickHouse sink
clickhouse_url = os.getenv("CLICKHOUSE_URL", "jdbc:clickhouse://localhost:8123/default")
clickhouse_user = os.getenv("CLICKHOUSE_USER", "default")
clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "")
catalog = JdbcCatalog("clickhouse_catalog", "default", f"{clickhouse_url}?user={clickhouse_user}&password={clickhouse_password}", "ru.yandex.clickhouse.ClickHouseDriver")
t_env.register_catalog("clickhouse_catalog", catalog)
t_env.execute_sql("""
    CREATE TABLE sentiments_sink (
                  text STRING,
                  sentiment STRING,
                  sentiment_score DOUBLE
              ) WITH (
                  'connector' = 'jdbc',
                  'url' = '{clickhouse_url}',
                  'table-name' = 'jellycat_sentiments',
                  'username' = '{clickhouse_user}',
                  'password' = '{clickhouse_password}',
                  'driver' = 'ru.yandex.clickhouse.ClickHouseDriver'
              )
                  """)
result_table.insert_into("sentiments_sink")

t_env.execute("JellycatFlinkRealTime")

def main():
    pass  # Flink job runs when module executes

if __name__ == "__main__":
    main()