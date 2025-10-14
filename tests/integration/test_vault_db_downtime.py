# FILE: tests/integration/test_vault_db_downtime.py
# Simulate DB outage by stopping the Postgres container between requests.
import time

def test_db_goes_down_then_put_500(vault_client, pg):
    r0 = vault_client.get("/last_head", params={"tenant":"down"})
    prev = r0.json()["last_head_hex"]
    b = {"ts_ns":int(time.time_ns()), "tenant":"down", "prev":prev, "ctx":{}, "body":{}}
    ok = vault_client.post("/receipts", json={"body": b})
    assert ok.status_code == 200

    url, container = pg
    container.stop()  # simulate DB outage
    time.sleep(1.0)

    b2 = {"ts_ns":int(time.time_ns()), "tenant":"down", "prev":ok.json()["head_hex"], "ctx":{}, "body":{}}
    r = vault_client.post("/receipts", json={"body": b2})
    assert r.status_code >= 500