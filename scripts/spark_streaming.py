from pyspark.sql.functions import from_json, col, window, length
from common.spark_utils import get_spark_session, write_to_postgres
from common.schema import toot_schema

# Création de la session Spark avec option spéciale Windows (--add-opens)
spark = get_spark_session("MastodonStreamProcessor")

# Lecture du stream depuis Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "mastodon_stream") \
    .option("startingOffsets", "latest") \
    .load()

# Conversion du JSON en colonnes structurées
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), toot_schema).alias("data")) \
    .select("data.*")

# Transformation 1 : filtrer par langue (ex: anglais uniquement)
df_filtered = df_parsed.filter(col("language") == "en")

# Transformation 2 : ajouter une fenêtre temporelle (par minute)
df_with_time = df_filtered \
    .withColumn("timestamp", col("created_at").cast("timestamp"))

# Action 1 : compter le nombre de toots par minute
toot_counts = df_with_time.groupBy(window(col("timestamp"), "1 minute")).count() \
    .withColumn("window_start", col("window").start) \
    .withColumn("window_end", col("window").end) \
    .drop("window")

# Action 2 : calculer la longueur moyenne des toots
avg_length = df_with_time.withColumn("toot_length", length(col("text"))) \
    .groupBy(window(col("timestamp"), "1 minute")).avg("toot_length") \
    .withColumn("window_start", col("window").start) \
    .withColumn("window_end", col("window").end) \
    .drop("window")

# Sauvegarde des résultats dans PostgreSQL
def write_to_postgres(df, table_name):
    df.write \
      .format("jdbc") \
      .option("url", "jdbc:postgresql://mastodon_db:5432/mastodon") \
      .option("dbtable", table_name) \
      .option("user", "mastodon_user") \
      .option("password", "mastodon") \
      .option("driver", "org.postgresql.Driver") \
      .mode("append") \
      .save()

# Démarrage des streams
query_counts = toot_counts.writeStream.foreachBatch(
    lambda df, epoch_id: write_to_postgres(df, "public.toot_counts")
).outputMode("update").start()

query_avg = avg_length.writeStream.foreachBatch(
    lambda df, epoch_id: write_to_postgres(df, "public.avg_toot_length")
).outputMode("update").start()

query_counts.awaitTermination()
query_avg.awaitTermination()


"""
💡 Pour lancer ce script sous Windows avec spark-submit (et éviter l'erreur getSubject) :

spark-submit ^
  --conf "spark.driver.extraJavaOptions=--add-opens=java.base/javax.security.auth=ALL-UNNAMED" ^
  --packages org.postgresql:postgresql:42.6.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 ^
  spark_streaming.py

⚠️ Adapte la version de Spark/Kafka connector au tien :
  - spark-sql-kafka-0-10_2.12:3.5.1 pour Spark 3.5.1
  - postgres driver 42.6.0 (stable)

"""
