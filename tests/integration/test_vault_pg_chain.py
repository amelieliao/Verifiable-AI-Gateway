# FILE: tests/integration/test_vault_pg_chain.py
# Integration tests for Vault POST /receipts and GET /tail with Postgres backend.
import time

def zeros():
    return "0x" + "00"*32

def test_chain_ok_and_tail(vault_client):
    r0 = vault_client.get("/last_head", params={"tenant":"acme"})
    assert r0.status_code == 200
    prev = r0.json()["last_head_hex"]

    body = {"ts_ns":int(time.time_ns()), "tenant":"acme", "prev": prev, "ctx":{"k":"v"}, "body":{"x":1}}
    r1 = vault_client.post("/receipts", json={"body": body})
    assert r1.status_code == 200
    h1 = r1.json()["head_hex"]

    r2 = vault_client.get("/last_head", params={"tenant":"acme"})
    body2 = {"ts_ns":int(time.time_ns()), "tenant":"acme", "prev": r2.json()["last_head_hex"], "ctx":{}, "body":{"y":2}}
    r2w = vault_client.post("/receipts", json={"body": body2})
    assert r2w.status_code == 200

    t = vault_client.get("/tail", params={"tenant":"acme","n":10})
    assert t.status_code == 200
    items = t.json()["items"]
    heads = [i[0] for i in items]
    assert any(h1 == x for x in heads)

def test_conflict_409(vault_client):
    r = vault_client.get("/last_head", params={"tenant":"z"})
    prev = r.json()["last_head_hex"]
    body = {"ts_ns":int(time.time_ns()), "tenant":"z", "prev":prev, "ctx":{}, "body":{}}
    ok = vault_client.post("/receipts", json={"body": body})
    assert ok.status_code == 200

    again = vault_client.post("/receipts", json={"body": body})
    assert again.status_code == 409