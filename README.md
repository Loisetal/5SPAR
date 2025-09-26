# Tester connexion mastodon
```bash
python test_mastodon.py
```

- **Objectif** : vérifier que ton token et ton instance fonctionnent.
- **Résultat attendu** : tu devrais voir quelques toots récents s’afficher dans le terminal.

# Tester kafka
```bash
docker exec -it kafka kafka-topics.sh --list --bootstrap-server kafka:9092
```

- **Objectif** : vérifier que ton topic mastodon_stream existe bien.
- **Résultat attendu** :
```bash
mastodon_stream
```
 
# Tester la connexion Mastodon → Kafka
```bash
python mastodon_to_kafka.py   
```
- **Objectif** : lancer le script qui récupère les toots et les publie dans Kafka.
- **Résultat attendu** : 
```bash
Streaming toots...
```

# Vérifier que Kafka reçoit bien les toots
```bash
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic mastodon_stream --from-beginning --max-messages 5
```

- **Objectif** : voir les messages envoyés par ton script Python.
- **Résultat attendu** : plusieurs messages JSON correspondant aux toots