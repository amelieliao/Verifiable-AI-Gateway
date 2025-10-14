# FILE: tests/unit/test_vault_utils.py
# Unit tests for canonical JSON and head hashing (domain separation).
import time
from pathlib import Path
from ._mod import load_module

def _canonical_json(mod, obj):
    # Support both canonical_json and _canonical_json function names.
    fn = getattr(mod, "canonical_json", None) or getattr(mod, "_canonical_json", None)
    assert fn, "canonical_json function not found"
    return fn(obj)

def _head_from_body(mod, j: str, tenant: str):
    # Support both head_from_body and _head_from_body function names.
    fn = getattr(mod, "head_from_body", None) or getattr(mod, "_head_from_body", None)
    assert fn, "head_from_body function not found"
    return fn(j, tenant)

def test_head_deterministic_and_domain_separation(monkeypatch):
    mod = load_module(str(Path("vault/app.py")), "vault_utils_mod")
    body = {"ts_ns": int(time.time_ns()), "tenant":"a", "prev":"0x"+"00"*32, "ctx":{}, "body":{"x":1}}
    j = _canonical_json(mod, body)
    h1 = _head_from_body(mod, j, "a")
    h2 = _head_from_body(mod, j, "a")
    assert h1 == h2
    h3 = _head_from_body(mod, j, "b")
    assert h1 != h3

def test_canonical_json_sorted_keys():
    mod = load_module(str(Path("vault/app.py")), "vault_utils_mod2")
    a = {"b":1, "a":2}
    s = _canonical_json(mod, a)
    assert s == '{"a":2,"b":1}'