from pyspark.sql.functions import from_json, col, window, length, explode
from common.spark_utils import get_spark_session, write_to_postgres
from common.schema import toot_schema
from functools import reduce

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

# Transformation 1 : filtrer par langue et mots-clés (ex: anglais uniquement)
keywords = ["Spark", "Mastodon", "Data"]
df_filtered = df_parsed.filter(
    (col("language") == "en") &
    reduce(lambda a, b: a | b, [col("text").contains(k) for k in keywords])
)

# Transformation 2 : ajouter une fenêtre temporelle (par minute)
df_with_time = df_filtered \
    .withColumn("timestamp", col("created_at").cast("timestamp"))

# Action 1 : compter le nombre de toots par minute
toot_counts = df_with_time.groupBy(window(col("timestamp"), "1 minute")).count() \
    .withColumn("window_start", col("window").start) \
    .withColumn("window_end", col("window").end) \
    .drop("window")

# Action 2 : longueur moyenne des toots par utilisateur
avg_length_user = df_with_time.withColumn("toot_length", length(col("text"))) \
    .groupBy("user_id", window(col("timestamp"), "1 minute")) \
    .avg("toot_length") \
    .withColumn("window_start", col("window").start) \
    .withColumn("window_end", col("window").end) \
    .drop("window")

# Action 3 : hashtags les plus populaires par fenêtre
df_hashtags = df_with_time.withColumn("hashtag", explode(col("hashtags")))
top_hashtags = df_hashtags.groupBy("hashtag", window(col("timestamp"), "1 minute")) \
    .count() \
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
query_raw = df_with_time.writeStream.foreachBatch(
    lambda df, epoch_id: write_to_postgres(df, "public.toots")
).outputMode("append").start()

query_counts = toot_counts.writeStream.foreachBatch(
    lambda df, epoch_id: write_to_postgres(df, "public.toot_counts")
).outputMode("update").start()

query_avg = avg_length_user.writeStream.foreachBatch(
    lambda df, epoch_id: write_to_postgres(df, "public.avg_toot_length_user")
).outputMode("update").start()

query_tags = top_hashtags.writeStream.foreachBatch(
    lambda df, epoch_id: write_to_postgres(df, "public.top_hashtags")
).outputMode("update").start()

for query in [query_raw, query_counts, query_avg, query_tags]:
    query.awaitTermination()

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
