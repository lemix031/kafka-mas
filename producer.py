import json
import time
import uuid
import random
from confluent_kafka import Producer

#MVP stigla je porudzbina, porudzbina ima svoj proizvod i kolicinu
#kafka je kanal kojim ce porudzbina od producera da stize do consumer-a

#gde se odvija kafka
producer = Producer({"bootstrap.servers":"localhost:9092"})

topic = "shop.orders"

products = ["majica","patike","duks"]

while True:
    event = {
        "order_id":str(uuid.uuid4()),
        "product_name": random.choice(products),
        "quantity":random.randint(1,3)
    }

    producer.produce(
        topic,
        key = event["order_id"],
        value = json.dumps(event)
    )
    producer.flush()

    print("ORDER CREATED", event)
    time.sleep(2)


