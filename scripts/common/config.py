import os

class Config:
    # PostgreSQL
    INSTANCE_URL = os.getenv("INSTANCE_URL", "https://mastodon.social")
    ACCESS_TOKEN = os.getenv("ACCESS_TOKEN", "SkRBTtrjicN1-imUK-dTgLKm5b7j5N1RXaHugvvc5m4")

    # PostgreSQL
    POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://mastodon_db:5432/mastodon")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "mastodon_user")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "mastodon")
    
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "mastodon_stream")
    
    # Spark
    CHECKPOINT_LOCATION = os.getenv("SPARK_CHECKPOINT_LOCATION", "/tmp/checkpoints")
    
    # Processing
    KEYWORDS = os.getenv("PROCESSING_KEYWORDS", "Spark,Mastodon,Data").split(",")
    WINDOW_DURATION = os.getenv("WINDOW_DURATION", "1 minute")
    ACTIVE_USER_THRESHOLD = int(os.getenv("ACTIVE_USER_THRESHOLD", "1"))
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "30"))

config = Config()