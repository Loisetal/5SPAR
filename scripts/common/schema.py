from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

# Schéma commun pour les toots Mastodon
toot_schema = StructType([
    StructField("toot_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("username", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("text", StringType(), True),
    StructField("hashtags", ArrayType(StringType()), True),
    StructField("language", StringType(), True),
    StructField("favourites_count", IntegerType(), True),
    StructField("reblogs_count", IntegerType(), True),
    StructField("reply_to_id", StringType(), True),
    StructField("url", StringType(), True),
])
