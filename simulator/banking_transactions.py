from kafka import KafkaProducer
import json
import random
import uuid
import time
from datetime import datetime, timedelta

# ===============================
# EVENT HUB CONFIGURATION
# ===============================
EVENT_HUB_NAME = "<<>EVENT_HUB_NAME>"
EVENTHUB_NAMESPACE = "<<NAMESPACE_HOSTNAME>>"
CONNECTION_STRING = "<<NAMESPACE_CONNECTION_STRING>>"

producer = KafkaProducer(
    bootstrap_servers=f"{EVENTHUB_NAMESPACE}:9093",
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username="$ConnectionString",
    sasl_plain_password=CONNECTION_STRING,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ===============================
# REFERENCE DATA
# ===============================
branches = ["Dallas", "Austin", "Phoenix", "Seattle", "New York"]
account_types = ["SAVINGS", "CHECKING", "CREDIT"]
channels = ["ATM", "POS", "ONLINE"]
genders = ["M", "F"]

# ===============================
# FRAUD LOGIC
# ===============================
def fraud_flag(amount):
    return "Y" if amount > 3000 else "N"

# ===============================
# EVENT GENERATOR
# ===============================
def generate_transaction_event():
    amount = round(random.uniform(-500, 5000), 2)
    txn_time = datetime.utcnow() - timedelta(minutes=random.randint(1, 500000))

    event = {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": random.randint(1, 2000),
        "account_id": random.randint(1, 3000),
        "branch": random.choice(branches),
        "account_type": random.choice(account_types),
        "transaction_type": "CREDIT" if amount > 0 else "DEBIT",
        "transaction_amount": amount,
        "channel": random.choice(channels),
        "transaction_timestamp": txn_time.isoformat(),
        "fraud_flag": fraud_flag(amount)
    }
    return event

# ===============================
# MAIN LOOP
# ===============================
if __name__ == "__main__":
    print("Starting Banking Event Hub Simulator...")
    while True:
        event = generate_transaction_event()
        producer.send(EVENT_HUB_NAME, event)
        print(f"Sent Event: {event}")
        time.sleep(1)