# FILE: tests/concurrency/test_cas_conflict.py
import time, concurrent.futures, requests, os
BASE=os.getenv("VAULT_BASE","http://localhost:8083")
TENANT="t"
def _put(prev):
    b={"ts_ns":int(time.time_ns()),"tenant":TENANT,"prev":prev,"ctx":{},"body":{}}
    r=requests.post(f"{BASE}/receipts", json={"body":b})
    return r.status_code
def test_conflict_burst():
    prev = requests.get(f"{BASE}/last_head", params={"tenant":TENANT}).json()["last_head_hex"]
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as ex:
        codes = list(ex.map(_put, [prev]*30))
    assert codes.count(200) == 1
    assert codes.count(409) >= 29
