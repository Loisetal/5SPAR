from pyspark.sql.functions import col, length, to_date, explode, count, avg, desc, row_number
from pyspark.sql.window import Window
from common.spark_utils import get_spark_session, write_to_postgres

# Créer une session Spark
spark = get_spark_session("MastodonBatchProcessor")

# Lire les données historiques depuis PostgreSQL
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://mastodon_db:5432/mastodon") \
    .option("dbtable", "public.toot_counts") \
    .option("user", "mastodon_user") \
    .option("password", "mastodon") \
    .option("driver", "org.postgresql.Driver") \
    .load()
