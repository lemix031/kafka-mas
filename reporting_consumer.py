import json
import os
from confluent_kafka import Consumer
import psycopg2

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS","localhost:9092")
TOPIC_INVENTORY = os.getenv("KAFKA_TOPIC_INVENTORY","shop.inventory")

PG_DSN = os.getenv(
    "PG_DSN",
    "dbname=postgres user=postgres password=postgres host=localhost port=5432"
)

consumer = Consumer({
    "bootstrap.servers":BOOTSTRAP,
    "group.id":"reporting",
    "auto.offset.reset":"latest"
})

consumer.subscribe([TOPIC_INVENTORY])

conn = psycopg2.connect(PG_DSN)
conn.autocommit = True

INSERT_EVENT_SQL = """
INSERT INTO inventory_events (order_id, event_type, product_name, quantity, reason, raw_event)
VALUES (%s,%s,%s,%s,%s,%s::jsonb)
"""

UPSERT_STATUS_SQL = """
INSERT INTO order_status (order_id, status, product_name, quantity, reason)
"""

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Kafka error:", msg.error())
        continue

    try:
        ev = json.loads(msg.value().decode("utf-8"))
        order_id = ev["order_id"]
        event_type = ev["event_type"]
        sku = ev["product_name"]
        qty = int(ev["quantity"])
        reason = ev.get("reason")

        status = "RESERVED" if event_type == "INVENTORY_RESERVED" else "REJECTED"
        raw_json = json.dumps(ev)

        with conn.cursor() as cur:
            cur.execute(INSERT_EVENT_SQL, (order_id, event_type, sku, qty, reason, raw_json))
            cur.execute(UPSERT_STATUS_SQL, (order_id, status, sku, qty, reason))

        print(f"[REPORTING] stored order={order_id} status={status}")

    except Exception as e:
        # U ovom servisu je OK da samo preskoči i nastavi (možeš kasnije DLQ i ovde)
        print("[REPORTING] error:", e)