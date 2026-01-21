import json
from confluent_kafka import Consumer

c = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "dlq-audit",
    "auto.offset.reset": "latest"
})
c.subscribe(["shop.dlq"])

print("DLQ audit started (listening to shop.dlq)")

while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Kafka error:", msg.error())
        continue

    ev = json.loads(msg.value().decode("utf-8"))
    print("[DLQ]", ev["reason"], "raw_value=", ev["raw_value"])
