
# 5SPAR - Projet
Loise Talluau & Mathieu

## Set up
Création d'un environnement virtuel, installation des librairies et déploiement du docker.

### Crée et active un environnement virtuel Python, puis installe les dépendances 
```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### Créer un fichier .env et le remplir à partir du .env.example

### Lancer les conteneurs docker
```bash
docker compose up -d
```

S'assurer de la bonne execution des containers dans Docker Desktop, au besoin relancer manuellement (Kafka)

## Exécuter le producteur Mastodon → Kafka
### Ouvrir deux terminaux en parallèle
Ouvrir un terminal et exécuter le script mastodon_producer.py pour collecter les toots Mastodon et les envoyer à Kafka
```bash
docker exec -it spark bash

pip install pyspark
pip install Mastodon.py confluent-kafka python-dotenv
pip install -U kafka-python==2.0.2

python mastodon_producer.py
```

## Exécuter le streaming Kafka → Spark → PostgreSQL
Ouvre un nouveau terminal et exécute le script spark_streaming.py pour lire en continu les messages du topic Kafka mastodon_stream, appliquer des transformations et enregistrer les résultats dans PostgreSQL.
```bash
docker exec -it spark bash

spark-submit   --conf "spark.driver.extraJavaOptions=--add-opens=java.base/javax.security.auth=ALL-UNNAMED"   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0   /home/jovyan/scripts/spark_streaming.py
```
## Exécuter le batch historique Mastodon
Ouvre un terminal et exécute le script mastodon_historical_batchg.py
```bash
docker exec -it spark bash

spark-submit   --conf "spark.driver.extraJavaOptions=--add-opens=java.base/javax.security.auth=ALL-UNNAMED"   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0   /home/jovyan/scripts/mastodon_historical_batch.py
```

## Base de données
Avec un outil comme DBeaver, consultez les données stockées

## Analyse des sentiments et Data Viz
Se rendre sur le notebook et executer les cellules









## Tests (tests ayant été réalisés avant la partie expliquée au-dessus. Plus vraiment d'intéret aujourd'hui)
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
