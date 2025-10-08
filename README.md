
# 5SPAR - Projet
Loise Talluau & Mathieu

## Set up
Création d'un environnement virtuel, installation des librairies et déploiement du docker.

### Lancer docker
```bash
docker compose up -d
```

## Tests connexions
### Test connexion mastodon
```bash
python scripts/tests/test_mastodon.py
```

- **Objectif** : Vérification Token + instance .
- **Résultat attendu** : Affichage de toots.

### Test kafka
```bash
docker exec -it kafka kafka-topics.sh --list --bootstrap-server kafka:9092
```

- **Objectif** : vérifier que ton topic mastodon_stream existe bien.
- **Résultat attendu** :
```bash
mastodon_stream
```
 
### Tester la connexion Mastodon → Kafka
```bash
docker exec -it spark bash
pip install Mastodon.py confluent-kafka python-dotenv
python scripts/tests/test_mastodon_to_kafka.py   
```
- **Objectif** : lancer le script qui récupère les toots et les publie dans Kafka.
- **Résultat attendu** : 
```bash
Streaming toots...
```

### Vérifier que Kafka reçoit bien les toots
En parallère de la commande précédente, ouvrir un autre terminal pour voir apparaitres les toots
```bash
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic mastodon_stream --from-beginning --max-messages 5
```

- **Objectif** : voir les messages envoyés par ton script Python.
- **Résultat attendu** : plusieurs messages JSON correspondant aux toots

```bash
wsl -d Ubuntu
sudo apt install python3-venv -y
python3 -m venv venv_linux
source venv_linux/bin/activate
pip install -r requirements.txt 

docker exec -it spark bash
pip install pyspark

spark-submit   --conf "spark.driver.extraJavaOptions=--add-opens=java.base/javax.security.auth=ALL-UNNAMED"   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0   /home/jovyan/scripts/spark_streaming.py

spark-submit   --conf "spark.driver.extraJavaOptions=--add-opens=java.base/javax.security.auth=ALL-UNNAMED"   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0   /home/jovyan/scripts/mastodon_historical_batch.py
```


docker exec -it spark bash

pip install Mastodon.py confluent-kafka python-dotenv




