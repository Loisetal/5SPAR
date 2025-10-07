from pyspark.sql import SparkSession, DataFrame
from common.config import config

def get_spark_session(app_name="MastodonApp", streaming=False):
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.driver.extraJavaOptions", "--add-opens=java.base/javax.security.auth=ALL-UNNAMED")
    
    if streaming:
        builder = builder.config("spark.sql.streaming.checkpointLocation", config.CHECKPOINT_LOCATION)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def write_to_postgres(df: DataFrame, table_name: str, mode: str = "append"):
    df.write \
        .format("jdbc") \
        .option("url", config.POSTGRES_URL) \
        .option("dbtable", table_name) \
        .option("user", config.POSTGRES_USER) \
        .option("password", config.POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode(mode) \
        .save()

def read_from_postgres(spark, table_name: str) -> DataFrame:
    return spark.read \
        .format("jdbc") \
        .option("url", config.POSTGRES_URL) \
        .option("dbtable", table_name) \
        .option("user", config.POSTGRES_USER) \
        .option("password", config.POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .load()

def read_kafka_stream(spark):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", config.KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()