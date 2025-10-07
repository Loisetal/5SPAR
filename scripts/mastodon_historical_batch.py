from pyspark.sql import Row
from pyspark.sql.functions import col, length, to_date, explode, count, avg, desc, row_number, hour
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from common.spark_utils import get_spark_session, write_to_postgres, read_from_postgres
from common.schema import toot_schema
from common.config import config
import random

# Créer la session Spark
spark = get_spark_session("MastodonBatchProcessor")

# Lire les tables issues du streaming
df_toots = read_from_postgres(spark, "public.toots")

# Vérifier si la table est vide et peupler avec des données de test
if df_toots.rdd.isEmpty():
    print("Table `public.toots` vide, génération de données de test...")
    users = ["user1", "user2", "user3"]
    hashtags_list = [["spark", "data"], ["mastodon"], ["test", "spark"]]
    base_time = datetime.now() - timedelta(days=5)
    rows = []
    for i in range(config.BATCH_SIZE):
        user = random.choice(users)
        hashtags = random.choice(hashtags_list)
        text = f"Test toot {i} from {user}"
        created_at = (base_time + timedelta(hours=i)).isoformat()
        rows.append(Row(
            toot_id=f"t{i}",
            user_id=user,
            username=user,
            created_at=created_at,
            text=text,
            hashtags=hashtags,
            language="en",
            favourites_count=random.randint(0,5),
            reblogs_count=random.randint(0,2),
            reply_to_id=None,
            url=f"http://example.com/t{i}"
        ))
    df_test = spark.createDataFrame(rows, schema=toot_schema)
    write_to_postgres(df_test, "public.toots", mode="overwrite")
    df_toots = df_test
else:
    print("Table `public.toots` contient déjà des données.")

# Conversion en timestamp et date
df_toots = df_toots.withColumn("timestamp", col("created_at").cast("timestamp"))
df_toots = df_toots.withColumn("date", to_date(col("timestamp")))

# Cache pour optimiser les calculs multiples
df_toots.cache()

# Filtrer utilisateurs actifs (plus de X toots)
X = config.ACTIVE_USER_THRESHOLD
active_users = df_toots.groupBy("user_id").agg(count("*").alias("toot_count")) \
    .filter(col("toot_count") > X) \
    .select("user_id")
df_active = df_toots.join(active_users, on="user_id", how="inner")

# Ajouter longueur des toots
df_active = df_active.withColumn("toot_length", length(col("text")))

# --Calculs principaux--
# Total toots par jour
toots_per_day = df_active.groupBy("date").agg(count("*").alias("total_toots"))

# Moyenne longueur des toots par jour
avg_toot_length = df_active.groupBy("date").agg(avg("toot_length").alias("avg_toot_length"))

# Moyenne longueur des toots par utilisateur (supplémentaire)
avg_length_user = df_active.groupBy("user_id").agg(avg("toot_length").alias("avg_toot_length_user"))

# Hashtags : top par jour
df_hashtags = df_active.withColumn("hashtag", explode(col("hashtags")))
window_spec = Window.partitionBy("date").orderBy(desc("count"))
top_hashtags = df_hashtags.groupBy("date","hashtag").agg(count("*").alias("count")) \
    .withColumn("rank", row_number().over(Window.partitionBy("date").orderBy(desc("count")))) \
    .filter(col("rank")==1) \
    .select("date","hashtag","count")

# Total toots par hashtag
total_toots_hashtag = df_hashtags.groupBy("hashtag").agg(count("*").alias("total_count"))

# Toots par heure
toots_per_hour = df_active.withColumn("hour", hour(col("timestamp"))) \
    .groupBy("date","hour").agg(count("*").alias("toots_per_hour"))

# Optimisations Spark
df_active.cache()
toots_per_day = toots_per_day.repartition(4)
avg_toot_length = avg_toot_length.coalesce(2)
top_hashtags = top_hashtags.coalesce(2)
avg_length_user = avg_length_user.coalesce(2)
total_toots_hashtag = total_toots_hashtag.coalesce(2)
toots_per_hour = toots_per_hour.coalesce(2)

# Affichage pour vérification
print("=== Toots per day ===")
toots_per_day.show(5)

print("=== Avg toot length per day ===")
avg_toot_length.show(5)

print("=== Avg toot length per user ===")
avg_length_user.show(5)

print("=== Top hashtags per day ===")
top_hashtags.show(5)

print("=== Total toots per hashtag ===")
total_toots_hashtag.show(5)

print("=== Toots per hour ===")
toots_per_hour.show(5)

# Sauvegarde dans PostgreSQL
write_to_postgres(toots_per_day, "public.toots_per_day", mode="overwrite")
write_to_postgres(avg_toot_length, "public.avg_toot_length_per_day", mode="overwrite")
write_to_postgres(avg_length_user, "public.avg_toot_length_per_user", mode="overwrite")
write_to_postgres(top_hashtags, "public.top_hashtags_per_day", mode="overwrite")
write_to_postgres(total_toots_hashtag, "public.total_toots_per_hashtag", mode="overwrite")
write_to_postgres(toots_per_hour, "public.toots_per_hour", mode="overwrite")

print("Batch processing terminé !")