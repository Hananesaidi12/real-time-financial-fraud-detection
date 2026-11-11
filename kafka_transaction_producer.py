import time
import json
import random
from datetime import datetime
import uuid
from kafka import KafkaProducer

# -------------------------------
# Connexion à Kafka
# -------------------------------
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# -------------------------------
# Configuration
# -------------------------------
USERS = [f"U{str(i).zfill(3)}" for i in range(1, 101)]
COUNTRIES_NORMAL = ["MA", "FR", "ES", "DE"]
COUNTRIES_RISK = ["RU", "NG", "IR", "KP"]
TRANSACTION_TYPES = ["online", "pos", "atm"]

NORMAL_AMOUNT_RANGE = (5, 500)
FRAUD_AMOUNT_RANGE = (1000, 10000)

FRAUD_PROBABILITY = 0.08
SEND_INTERVAL = 1

print("💳 Kafka Producer démarré...\n")

# -------------------------------
# Envoi continu
# -------------------------------
while True:
    is_fraud = random.random() < FRAUD_PROBABILITY

    transaction = {
        "transaction_id": f"TX-{uuid.uuid4().hex[:8]}",
        "user_id": random.choice(USERS),
        "amount": round(
            random.uniform(*FRAUD_AMOUNT_RANGE if is_fraud else NORMAL_AMOUNT_RANGE), 2
        ),
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "country": random.choice(COUNTRIES_RISK if is_fraud else COUNTRIES_NORMAL),
        "is_fraud_simulated": is_fraud,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    producer.send("transactions", transaction)
    print("Envoyé :", transaction)

    time.sleep(SEND_INTERVAL)
