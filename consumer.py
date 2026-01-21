import json
from confluent_kafka import Consumer
from confluent_kafka import Producer
import time

BOOTSTRAP = "localhost:9092"
TOPIC_ORDERS = "shop.orders"
TOPIC_INVENTORY = "shop.inventory"
#mogucnost citanja nekih starijih poruka - najbolje prebaciti u latest
consumer = Consumer({
    "bootstrap.servers":BOOTSTRAP,
    "group.id":"inventory",
    "auto.offset.reset":"latest"
})

consumer.subscribe([TOPIC_ORDERS])

#MVP2 Pravimo Producer-a u Consumer-u koji ce da emituje ishod u shop.inventory
producer = Producer({"bootstrap.servers":BOOTSTRAP})

stock = {
    "majica":200,
    "duks":200,
    "patike":200
}

#stampa stock-a
print("Inventory service started. Stock: ",stock)

#poll kaze uzmi prvu dostupnu poruku iz kafka brokera, makar iz topic-a na koji sam pretplacen
#cekam najvise 1.0 sekund (argument)
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    raw = msg.value()
    if not raw:
        continue

    try:
        order = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError:
        print("Skipping bad message:", raw)
        continue

    #order = json.loads(msg.value().decode("utf-8"))
    product_name = order["product_name"]
    quantity = order["quantity"]
    order_id = order["order_id"]
    if stock.get(product_name, 0) >= quantity:
        stock[product_name] -= quantity
        outcome_type = "INVENTORY_RESERVED"
        reason=None
        print(f"ORDER OK {order['order_id']} -> {product_name} - {quantity}")

    else:
        outcome_type = "INVENTORY_REJECTED"
        reason = f"OUT_OF_STOCK have={stock.get(product_name,0)} need={quantity}"
        print(f"ORDER FAIL {order_id} -> {product_name} x {quantity} ({reason})")

    outcome_event = {
    "event_type": outcome_type,
    "order_id": order_id,
    "product_name": product_name,
    "quantity": quantity,
    "reason": reason,
    "stock_after": stock.get(product_name, 0)
}

    producer.produce(
        TOPIC_INVENTORY,
        key=order_id,
        value=json.dumps(outcome_event)
    )
    producer.flush()  # MVP jednostavno; kasnije uklanjamo flush po poruci

    print("INVENTORY EVENT:", outcome_event)
    print("CURRENT STOCK:", stock)
    time.sleep(0.1)