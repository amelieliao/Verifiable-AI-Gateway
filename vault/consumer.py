# FILE: vault/consumer.py
from __future__ import annotations
import os, asyncio, blake3, orjson as json, psycopg_pool
from aiokafka import AIOKafkaConsumer

BOOT = os.getenv("KAFKA_BOOTSTRAP","redpanda:9092")
TOPIC = os.getenv("TOPIC","tcd.receipts")
GROUP = os.getenv("GROUP_ID","vault-consumers")
DBURL = os.getenv("DATABASE_URL")

pool: psycopg_pool.AsyncConnectionPool

def zeros(): return "0x" + "00"*32
def canonical(o): return json.dumps(o, option=json.OPT_SORT_KEYS).decode()
def head_from(tenant:str, body:dict)->str:
    h = blake3.blake3(); h.update(b"tcd:receipt"); h.update(tenant.encode()); h.update(canonical(body).encode()); return "0x"+h.hexdigest()

async def init_pool(): 
    global pool; pool = psycopg_pool.AsyncConnectionPool(DBURL, min_size=1, max_size=10)

async def handle(e: dict):
    tenant = e["tenant"]
    body = {k:e[k] for k in ("ts_ns","tenant","prev","ctx","body")}
    head = e.get("head_hex") or head_from(tenant, body)
    async with pool.connection() as conn:
        async with conn.transaction():
            row = await conn.execute("SELECT last_head FROM tenant_state WHERE tenant=%s FOR UPDATE",(tenant,))
            r = await row.fetchone(); last = r[0] if r else zeros()
            if body["prev"] != last:
                # 简化：冲突丢弃（生产入 DLQ）
                return
            await conn.execute(
                "INSERT INTO receipts (tenant, head_hex, prev, ts_ns, ctx, body) VALUES (%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (head_hex) DO NOTHING",
                (tenant, head, body["prev"], body["ts_ns"], body["ctx"], body["body"])
            )
            await conn.execute(
                "INSERT INTO tenant_state (tenant,last_head) VALUES (%s,%s) "
                "ON CONFLICT (tenant) DO UPDATE SET last_head=EXCLUDED.last_head",
                (tenant, head)
            )

async def main():
    await init_pool()
    consumer = AIOKafkaConsumer(TOPIC, bootstrap_servers=BOOT, group_id=GROUP,
                                enable_auto_commit=True, value_deserializer=lambda b: json.loads(b))
    await consumer.start()
    try:
        async for msg in consumer:
            try:
                await handle(msg.value)
            except Exception:
                pass
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())
