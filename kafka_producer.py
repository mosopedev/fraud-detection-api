from kafka import KafkaProducer
import pandas as pd
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

transactions = pd.read_csv("sampled_data.csv")
transactions =  transactions.drop(columns=["is_fraud"])
transactions = transactions.to_dict(orient="records")

for txn in transactions:
    producer.send("transactions", txn)
    print(f"Sent: {txn}")
    time.sleep(1)  # Simulate delay
