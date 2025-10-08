import os
import json
from common.config import config
from mastodon import Mastodon, StreamListener
from kafka import KafkaProducer
from dotenv import load_dotenv

# Charger les variables d'environnement depuis .env
load_dotenv()

# Initialisation Kafka
producer = KafkaProducer(
    bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Listener Mastodon
class TootListener(StreamListener):
    def on_update(self, status):
        toot = {
            "toot_id": str(status["id"]),
            "user_id": str(status["account"]["id"]),
            "username": status["account"]["username"],
            "created_at": status["created_at"].isoformat(),
            "text": status["content"],
            "hashtags": [tag["name"] for tag in status["tags"]],
            "language": status.get("language"),
            "favourites_count": status["favourites_count"],
            "reblogs_count": status["reblogs_count"],
            "reply_to_id": str(status["in_reply_to_id"]) if status["in_reply_to_id"] else None,
            "url": status["url"]
        }
        producer.send(config.KAFKA_TOPIC, toot)
        producer.flush()
        print(f"Toot envoyé: {toot['text']}")

# Connexion à Mastodon
mastodon = Mastodon(
    access_token=config.ACCESS_TOKEN,
    api_base_url=config.INSTANCE_URL
)

# Démarrage du stream public
listener = TootListener()
print("Streaming Mastodon -> Kafka démarré...")
mastodon.stream_public(listener)
