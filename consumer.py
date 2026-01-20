import json
from confluent_kafka import Consumer


consumer = Consumer({
    "bootstrap.servers":"localhost:9092",
    "group.id":"inventory",
    "auto.offset.reset":"earliest"
})

consumer.subscribe(["shop.orders"])

stock = {
    "majica":200,
    "duks":200,
    "patike":200
}

#poll kaze uzmi prvu dostupnu poruku iz kafka brokera, makar iz topic-a na koji sam pretplacen
#cekam najvise 1.0 sekund (argument)
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    order = json.loads(msg.value())
    product_name = order["product_name"]
    quantity = order["quantity"]

    if stock.get(product_name, 0) >= quantity:
        stock[product_name] -= quantity
        print(f"ORDER OK {order['order_id']} -> {product_name} - {quantity}")

    else:
        print(f"OUT OF STOCK {product_name} for order {order['order_id']}")