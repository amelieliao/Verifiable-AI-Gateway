# FILE: tests/unit/test_gateway_outbox.py
# Unit tests for a simple Outbox (if present).
import time
import tempfile
import os
from pathlib import Path
from ._mod import load_module
import pytest

@pytest.mark.skipif(not Path("gateway/outbox.py").exists(), reason="outbox module not present")
def test_outbox_add_batch_mark():
    tmp = tempfile.TemporaryDirectory()
    path = os.path.join(tmp.name, "outbox.db")
    mod = load_module(str(Path("gateway/outbox.py")), "gw_outbox_mod")
    ob = mod.Outbox(path)

    key = "k1"
    payload = {"foo":"bar","ts":int(time.time_ns())}
    ob.add(key, payload)

    batch = list(ob.batch(10))
    assert len(batch) == 1
    rid, k, p = batch[0]
    assert k == key and p == payload

    ob.mark_sent(rid)
    batch2 = list(ob.batch(10))
    assert len(batch2) == 0