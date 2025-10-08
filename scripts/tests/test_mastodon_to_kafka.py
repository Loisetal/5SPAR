import json
import re
import time
import os
from mastodon import Mastodon, StreamListener
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

# Config Mastodon
INSTANCE_URL = os.getenv("INSTANCE_URL")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

# Config Kafka
KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "mastodon_stream"

# Hashtags à filtrer
#FILTER_HASHTAGS = {"DataScience", "AI"}
FILTER_HASHTAGS = set()

# Kafka Producer
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

# Fonction pour nettoyer le HTML
def clean_html(raw_html):
    clean_text = re.sub('<[^<]+?>', '', raw_html)
    return clean_text

# Listener Mastodon
class MyListener(StreamListener):
    def on_update(self, status):
        try:
            text_html = status.get('content', '')
            text = clean_html(text_html)
            hashtags = [h.strip('#') for h in re.findall(r"#\w+", text)]
            hashtags_lower = {h.lower() for h in hashtags}

            # Filtrer par hashtags
            if not FILTER_HASHTAGS or hashtags_lower & {h.lower() for h in FILTER_HASHTAGS}:
                msg = {
                    "toot_id": str(status.get('id')),
                    "user_id": str(status.get('account', {}).get('id')),
                    "username": status.get('account', {}).get('acct'),
                    "created_at": status.get('created_at').isoformat() if status.get('created_at') else None,
                    "text": text,
                    "hashtags": hashtags,
                    "language": status.get('language'),
                    "favourites_count": status.get('favourites_count', 0),
                    "reblogs_count": status.get('reblogs_count', 0),
                    "reply_to_id": status.get('in_reply_to_id'),
                    "url": status.get('url')
                }
                producer.produce(KAFKA_TOPIC, key=msg["user_id"], value=json.dumps(msg).encode('utf-8'))
                producer.poll(0)
        except Exception as e:
            print("Erreur on_update:", e)

    def on_error(self, status_code):
        print("Stream error:", status_code)
        return True

if __name__ == "__main__":
    mastodon = Mastodon(access_token=ACCESS_TOKEN, api_base_url=INSTANCE_URL)
    listener = MyListener()
    print("Streaming toots...")
    while True:
        try:
            mastodon.stream_public(listener)
        except Exception as e:
            print("Erreur stream:", e)
            time.sleep(5)
