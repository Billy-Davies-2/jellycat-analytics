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
        nc = loop.run_until_complete(NATS.connect("nats://localhost:4222"))
        sub = loop.run_until_complete(nc.subscribe("jellycat_reviews"))
        while True:
            msg = loop.run_until_complete(nc.next_msg())
            ctx.collect((msg.subject, msg.data.decode()))

    def cancel(self):
        pass # add cleanup

@udf(result_type=DataTypes.STRING())
def flink_analyze(text):
    return analyze_sentiment(text)

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

# Source
stream = env.add_source(NatsSourceFunction())

# To table
t_env.create_temporary_view("nats_source", t_env.from_data_stream(Schema().field("id", DataTypes.STRING())))

# Process
result_table = t_env.from_path("nats_source").select("id, flink_analyze(id) as sentiment")

# ClickHouse sink
catalog = JdbcCatalog("clickhouse_catalog", "default", "jdbc:clickhouse://localhost:8123/default?user=default&password=", "ru.yandex.clickhouse.ClickHouseDriver")
t_env.register_catalog("clickhouse_catalog", catalog)
t_env.execute_sql("""
    CREATE TABLE sentiments_sink (
                  id STRING,
                  sentiment STRING
              ) WITH (
                  'connector' = 'jdbc',
                  'url' = 'jdbc:clickhouse://localhost:8123/default',
                  'table-name' = 'jellycat_sentiments',
                  'username' = 'default',
                  'password' = '',
                  'driver' = 'ru.yandex.clickhouse.ClickHouseDriver'
              )
                  """)
result_table.insert_into("sentiments_sink")

t_env.execute("JellycatFlinkRealTime")

def main():
    pass  # Flink job runs when module executes

if __name__ == "__main__":
    main()