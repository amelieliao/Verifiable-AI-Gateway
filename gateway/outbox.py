# FILE: gateway/outbox.py
from __future__ import annotations
import sqlite3, json, time, os, threading
_SCHEMA = """
CREATE TABLE IF NOT EXISTS outbox(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  k  TEXT UNIQUE NOT NULL,
  payload TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  ts_ns INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status, ts_ns);
"""
class Outbox:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(_SCHEMA)
        self._lk = threading.RLock()
    def add(self, key: str, payload: dict):
        with self._lk, self._db:
            self._db.execute("INSERT OR IGNORE INTO outbox(k,payload,ts_ns) VALUES(?,?,?)",
                             (key, json.dumps(payload, separators=(',',':')), time.time_ns()))
    def batch(self, n: int = 200):
        with self._lk:
            cur = self._db.execute("SELECT id,k,payload FROM outbox WHERE status='pending' ORDER BY id LIMIT ?", (n,))
            for rid, k, p in cur.fetchall():
                yield rid, k, json.loads(p)
    def mark_sent(self, rid: int):
        with self._lk, self._db:
            self._db.execute("UPDATE outbox SET status='sent' WHERE id=?", (rid,))
