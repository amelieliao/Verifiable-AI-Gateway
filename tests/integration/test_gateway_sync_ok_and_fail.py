# FILE: tests/integration/test_gateway_sync_ok_and_fail.py
# Integration tests for Gateway sync path with respx mocks.
import respx
import httpx
from fastapi.testclient import TestClient

def _zeros():
    return "0x" + "00"*32

@respx.mock
def test_gateway_sync_happy_path(gateway_client_sync: TestClient):
    respx.post("http://side.local/diagnose").mock(
        return_value=httpx.Response(200, json={"decision":"allow","degrade":{"temperature":0.7}})
    )
    respx.post("http://prov.local/infer").mock(
        return_value=httpx.Response(200, json={"answer":"Echo: hello"})
    )
    respx.get("http://vault.local/last_head").mock(
        return_value=httpx.Response(200, json={"tenant":"acme","last_head_hex":_zeros()})
    )
    respx.post("http://vault.local/receipts").mock(
        return_value=httpx.Response(200, json={"ok":True,"head_hex":"0xabc","prev":_zeros(),"count":1})
    )

    r = gateway_client_sync.post("/infer", json={
        "tenant":"acme","user":"u1","session":"s1","prompt":"hello"
    })
    assert r.status_code == 200
    j = r.json()
    assert j["queued"] is False and j["proposed_head_hex"] == "0xabc"
    assert j["answer"].startswith("Echo")

@respx.mock
def test_gateway_provider_500_bubbles_up(gateway_client_sync: TestClient):
    respx.post("http://side.local/diagnose").mock(
        return_value=httpx.Response(200, json={"decision":"allow","degrade":{}})
    )
    respx.post("http://prov.local/infer").mock(
        return_value=httpx.Response(500, json={"error":"boom"})
    )
    r = gateway_client_sync.post("/infer", json={
        "tenant":"acme","user":"u1","session":"s1","prompt":"hello"
    })
    assert r.status_code == 500

@respx.mock
def test_gateway_vault_timeout_returns_502(gateway_client_sync: TestClient):
    respx.post("http://side.local/diagnose").mock(
        return_value=httpx.Response(200, json={"decision":"allow","degrade":{}})
    )
    respx.post("http://prov.local/infer").mock(
        return_value=httpx.Response(200, json={"answer":"OK"})
    )
    respx.get("http://vault.local/last_head").mock(side_effect=httpx.ReadTimeout("rt"))
    r = gateway_client_sync.post("/infer", json={
        "tenant":"acme","user":"u1","session":"s1","prompt":"hi"
    })
    assert r.status_code == 502

@respx.mock
def test_gateway_vault_conflict_returns_502(gateway_client_sync: TestClient):
    respx.post("http://side.local/diagnose").mock(
        return_value=httpx.Response(200, json={"decision":"allow","degrade":{}})
    )
    respx.post("http://prov.local/infer").mock(
        return_value=httpx.Response(200, json={"answer":"OK"})
    )
    respx.get("http://vault.local/last_head").mock(
        return_value=httpx.Response(200, json={"tenant":"acme","last_head_hex":_zeros()})
    )
    respx.post("http://vault.local/receipts").mock(
        return_value=httpx.Response(409, json={"detail":{"expected_prev":_zeros(),"got_prev":"0xdead"}})
    )
    r = gateway_client_sync.post("/infer", json={
        "tenant":"acme","user":"u1","session":"s1","prompt":"hi"
    })
    assert r.status_code == 502