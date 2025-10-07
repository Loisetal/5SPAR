from pyspark.sql.functions import from_json, col, window, length, explode
from common.spark_utils import get_spark_session, write_to_postgres, read_kafka_stream
from common.schema import toot_schema
from common.config import config
from functools import reduce

# Création de la session Spark avec option spéciale Windows (--add-opens)
spark = get_spark_session("MastodonStreamProcessor", streaming=True)

# Lecture du stream depuis Kafka
df_raw = read_kafka_stream(spark)

# Conversion du JSON en colonnes structurées
df_parsed = (
    df_raw
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), toot_schema).alias("data"))
    .select("data.*")
)

# Transformation 1 : filtrer par langue et mots-clés (ex: anglais uniquement)
keywords = config.KEYWORDS
df_filtered = df_parsed.filter(
    (col("language") == "en") &
    reduce(lambda a, b: a | b, [col("text").contains(k) for k in keywords])
)

# Transformation 2 : ajouter une fenêtre temporelle (par minute)
df_with_time = df_filtered.withColumn("timestamp", col("created_at").cast("timestamp"))

# Action 1 : compter le nombre de toots par minute
toot_counts = df_with_time.groupBy(window(col("timestamp"), config.WINDOW_DURATION)).count() \
    .withColumn("window_start", col("window").start) \
    .withColumn("window_end", col("window").end) \
    .drop("window")

# Action 2 : longueur moyenne des toots par utilisateur
avg_length_user = df_with_time.withColumn("toot_length", length(col("text"))) \
    .groupBy("user_id", window(col("timestamp"), config.WINDOW_DURATION)) \
    .avg("toot_length") \
    .withColumn("window_start", col("window").start) \
    .withColumn("window_end", col("window").end) \
    .drop("window")

# Action 3 : hashtags les plus populaires par fenêtre
df_hashtags = df_with_time.withColumn("hashtag", explode(col("hashtags")))
top_hashtags = df_hashtags.groupBy("hashtag", window(col("timestamp"), config.WINDOW_DURATION)) \
    .count() \
    .withColumn("window_start", col("window").start) \
    .withColumn("window_end", col("window").end) \
    .drop("window")

# Démarrage des streams
query_raw = df_with_time.writeStream.foreachBatch(
    lambda df, epoch_id: write_to_postgres(df, "public.toots", mode="append")
).outputMode("append").start()

query_counts = toot_counts.writeStream.foreachBatch(
    lambda df, epoch_id: write_to_postgres(df, "public.toot_counts", mode="append")
).outputMode("update").start()

query_avg = avg_length_user.writeStream.foreachBatch(
    lambda df, epoch_id: write_to_postgres(df, "public.avg_toot_length_user", mode="append")
).outputMode("update").start()

query_tags = top_hashtags.writeStream.foreachBatch(
    lambda df, epoch_id: write_to_postgres(df, "public.top_hashtags", mode="append")
).outputMode("update").start()

# Attente de la fin des streams
for query in [query_raw, query_counts, query_avg, query_tags]:
    query.awaitTermination()