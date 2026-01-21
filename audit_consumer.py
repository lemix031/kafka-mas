import json
from confluent_kafka import Consumer
#Ovde zapravo imamo - jedan dogadjaj, vise nezavnisnih reakcija!
consumer = Consumer({
    "bootstrap.servers":"localhost:9092",
    "group.id":"audit",
    "auto.offset.reset":"latest"
})

consumer.subscribe(["shop.inventory"])

print("Audit service started (listening to shop inventory)")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Kafka error", msg.error())
        continue
    
    event = json.loads(msg.value().decode("utf-8"))

    order_id = event["order_id"]
    event_type = event["event_type"]
    product_name = event["product_name"]
    quantity = event["quantity"]

    if event_type == "INVENTORY_RESERVED":
        print(f"[AUDIT] Order {order_id}: RESERVERED {quantity}x{product_name}")
    else:
        print(f"[AUDIT] Order {order_id}: REJECTED ({event['reason']})")
