# FILE: gateway/app_asyncoutbox.py
from __future__ import annotations
import os, asyncio, blake3, orjson as json
from aiokafka import AIOKafkaProducer
from .outbox import Outbox

TOPIC = os.getenv("KAFKA_TOPIC","tcd.receipts")
BOOT  = os.getenv("KAFKA_BOOTSTRAP","redpanda:9092")
OUTBOX_PATH = os.getenv("OUTBOX_PATH","/data/outbox.db")

producer: AIOKafkaProducer | None = None
outbox = Outbox(OUTBOX_PATH)

async def start():
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=BOOT,
        linger_ms=5, compression_type="lz4",
        value_serializer=lambda v: json.dumps(v))
    await producer.start()
    asyncio.create_task(_pump())

async def stop():
    global producer
    if producer:
        await producer.stop()
        producer = None

def head_hex(tenant: str, body_json: str) -> str:
    h = blake3.blake3(); h.update(b"tcd:receipt"); h.update(tenant.encode()); h.update(body_json.encode()); return "0x"+h.hexdigest()

def enqueue(event: dict):
    k = event["head_hex"]
    outbox.add(k, event)

async def _pump():
    while True:
        try:
            batch = list(outbox.batch(200))
            if not batch:
                await asyncio.sleep(0.1); continue
            for rid, key, payload in batch:
                tenant = payload["tenant"]
                await producer.send_and_wait(TOPIC, value=payload, key=tenant.encode())
                outbox.mark_sent(rid)
        except Exception:
            await asyncio.sleep(0.5)