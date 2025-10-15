# batch_sentiment.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, regexp_replace, udf
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, IDF, StringIndexer, IndexToString
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.types import DoubleType

# --------- CONFIG ---------
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB   = os.getenv("POSTGRES_DB", "mastodon")
PG_USER = os.getenv("POSTGRES_USER", "mastodon_user")
PG_PWD  = os.getenv("POSTGRES_PASSWORD", "mastodon_pw")
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {"user": PG_USER, "password": PG_PWD, "driver": "org.postgresql.Driver"}

TABLE_TOOTS    = os.getenv("TABLE_TOOTS", "toots")             # doit contenir toot_id, text
TABLE_LABELED  = os.getenv("TABLE_LABELED", "toots_labeled")   # sera créé si absent
TABLE_OUT      = os.getenv("TABLE_OUT", "toots_with_sentiment")

SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "MastoBatchSentiment")
USE_NB         = os.getenv("USE_NAIVE_BAYES", "false").lower() == "true"  # sinon LogisticRegression binaire
NEUTRAL_LOW    = float(os.getenv("NEUTRAL_LOW", "0.45"))
NEUTRAL_HIGH   = float(os.getenv("NEUTRAL_HIGH", "0.55"))

# Règles bootstrap (override possibles via env)
POS_REGEX = os.getenv(
    "BOOTSTRAP_POS_REGEX",
    r"(?:\b(love|great|awesome|merci|super|g[ée]nial|bravo|magnifique|excellent)\b|😍|❤️|👍|🎉)"
)
NEG_REGEX = os.getenv(
    "BOOTSTRAP_NEG_REGEX",
    r"(?:\b(hate|awful|terrible|merde|nul|pourri|honte|d[ée]teste)\b|😡|💩|👎)"
)

# --------- SPARK ---------
spark = (SparkSession.builder
         .appName(SPARK_APP_NAME)
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())

def table_exists(table_name: str) -> bool:
    try:
        spark.read.jdbc(JDBC_URL, table_name, properties=JDBC_PROPS).limit(1).collect()
        return True
    except Exception:
        return False

def ensure_labeled_table():
    """
    Si TABLE_LABELED n'existe pas :
      - lit TABLE_TOOTS (toot_id, text),
      - nettoie HTML,
      - applique regex POS/NEG,
      - filtre les NULL,
      - écrit TABLE_LABELED (append) -> Postgres crée la table si besoin.
    """
    if table_exists(TABLE_LABELED):
        print(f"[BOOTSTRAP] Table '{TABLE_LABELED}' trouvée ✔")
        return

    if not table_exists(TABLE_TOOTS):
        raise RuntimeError(f"La table source '{TABLE_TOOTS}' est introuvable — impossible de booter toots_labeled.")

    print(f"[BOOTSTRAP] Création rapide de '{TABLE_LABELED}' depuis '{TABLE_TOOTS}' (règles faibles POS/NEG).")
    toots_df = spark.read.jdbc(JDBC_URL, TABLE_TOOTS, properties=JDBC_PROPS).select("text").na.drop(subset=["text"])
    toots_df = toots_df.withColumn("text", regexp_replace(col("text"), "<.*?>", " "))

    labeled = (toots_df
        .withColumn("label",
                    when(col("text").rlike(POS_REGEX), lit("positive"))
                    .when(col("text").rlike(NEG_REGEX), lit("negative"))
                    .otherwise(lit(None)))
        .na.drop(subset=["label"])
        .select("text", "label")
    )

    cnt = labeled.count()
    if cnt == 0:
        raise RuntimeError(
            "Bootstrap impossible : aucune ligne ne matche les regex POS/NEG. "
            "Ajuste BOOTSTRAP_POS_REGEX/BOOTSTRAP_NEG_REGEX ou insère manuellement quelques exemples dans toots_labeled."
        )

    labeled.write.jdbc(JDBC_URL, TABLE_LABELED, mode="append", properties=JDBC_PROPS)
    print(f"[BOOTSTRAP] {cnt} lignes écrites dans '{TABLE_LABELED}'. Tu pourras corriger/compléter ensuite pour améliorer la qualité.")

# ---- Assure la présence de toots_labeled ----
ensure_labeled_table()

# --------- LOAD TRAINING (Postgres uniquement) ---------
training_df = spark.read.jdbc(JDBC_URL, TABLE_LABELED, properties=JDBC_PROPS).select("text","label")
training_df = training_df.withColumn("text", regexp_replace(col("text"), "<.*?>", " ")).na.drop(subset=["text","label"])

# --------- PIPELINE FEATURES ---------
tokenizer  = RegexTokenizer(inputCol="text", outputCol="tokens", pattern=r"\W+")
remover    = StopWordsRemover(inputCol="tokens", outputCol="words")
cv         = CountVectorizer(inputCol="words", outputCol="tf")
idf        = IDF(inputCol="tf", outputCol="features")
indexer    = StringIndexer(inputCol="label", outputCol="label_idx").fit(training_df)

# --------- CLASSIFIER ---------
if USE_NB:
    clf = NaiveBayes(featuresCol="features", labelCol="label_idx", modelType="multinomial")
else:
    clf = LogisticRegression(featuresCol="features", labelCol="label_idx", maxIter=50)

decoder = IndexToString(inputCol="prediction", outputCol="pred_label", labels=indexer.labels)
pipeline = Pipeline(stages=[tokenizer, remover, cv, idf, indexer, clf, decoder])

# --------- TRAIN / TEST ---------
train_df, test_df = training_df.randomSplit([0.8, 0.2], seed=42)
model = pipeline.fit(train_df)
pred_test = model.transform(test_df)

e_acc = MulticlassClassificationEvaluator(labelCol="label_idx", predictionCol="prediction", metricName="accuracy").evaluate(pred_test)
e_f1  = MulticlassClassificationEvaluator(labelCol="label_idx", predictionCol="prediction", metricName="f1").evaluate(pred_test)
print(f"[EVAL] accuracy={e_acc:.4f}  f1={e_f1:.4f}")

# --------- APPLY ON HISTORICAL (table 'toots', champ 'text') ---------
toots_df = spark.read.jdbc(JDBC_URL, TABLE_TOOTS, properties=JDBC_PROPS).select("toot_id","text").na.drop(subset=["text"])
toots_df = toots_df.withColumn("text", regexp_replace(col("text"), "<.*?>", " "))

pred_hist = model.transform(toots_df)

if not USE_NB:
    # proba positive pour LR binaire
    labels = indexer.labels
    try:
        pos_i = labels.index("positive"); neg_i = labels.index("negative")
    except ValueError:
        pos_i, neg_i = 1, 0

    def prob_pos(v):
        try: return float(v[pos_i])
        except Exception: return None

    prob_udf = udf(prob_pos, DoubleType())

    pred_hist = (pred_hist
                 .withColumn("prob_positive", prob_udf(col("probability")))
                 .withColumn("prob_negative", 1 - col("prob_positive"))
                 .withColumn(
                     "sentiments",
                     when(col("prob_positive").isNull(), lit("neutral"))
                     .when((col("prob_positive") >= lit(NEUTRAL_LOW)) & (col("prob_positive") <= lit(NEUTRAL_HIGH)), lit("neutral"))
                     .when(col("prob_positive") > lit(0.5), lit("positive"))
                     .otherwise(lit("negative"))
                 ))
else:
    pred_hist = (pred_hist
                 .withColumn("sentiments", col("pred_label"))
                 .withColumn("prob_positive", lit(None).cast("double"))
                 .withColumn("prob_negative", lit(None).cast("double")))

out = pred_hist.select("toot_id","text","sentiments","prob_positive","prob_negative")
out.write.jdbc(JDBC_URL, TABLE_OUT, mode="append", properties=JDBC_PROPS)

print(f"{out.count()} lignes écrites dans {TABLE_OUT}")
spark.stop()
