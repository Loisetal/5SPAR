from pyspark.sql import SparkSession

def get_spark_session(app_name="MastodonApp"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.extraJavaOptions", "--add-opens=java.base/javax.security.auth=ALL-UNNAMED") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_to_postgres(df, table_name, mode="append"):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://mastodon_db:5432/mastodon") \
        .option("dbtable", table_name) \
        .option("user", "mastodon_user") \
        .option("password", "mastodon") \
        .option("driver", "org.postgresql.Driver") \
        .mode(mode) \
        .save()
