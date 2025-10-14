# FILE: tests/conftest.py
# Common fixtures: ephemeral Postgres via testcontainers, schema init, app TestClients.
import os
import psycopg
import pytest
from pathlib import Path
from fastapi.testclient import TestClient
from testcontainers.postgres import PostgresContainer
from ._mod import load_module

ROOT = Path(__file__).resolve().parents[1]

def _normalize_pg_url(url: str) -> str:
    # Normalize DSN if provider returns a driver suffix.
    return url.replace("+psycopg2", "")

@pytest.fixture(scope="session")
def pg():
    """Start ephemeral Postgres and yield (dsn, container)."""
    with PostgresContainer("postgres:16").with_env("POSTGRES_DB","tcd") as pgc:
        url = _normalize_pg_url(pgc.get_connection_url())
        yield url, pgc

@pytest.fixture(scope="session")
def init_db(pg):
    """Apply SQL schema from vault/sql/001_init.sql."""
    url, _ = pg
    sql_file = ROOT / "vault" / "sql" / "001_init.sql"
    with psycopg.connect(url) as conn, conn.cursor() as cur:
        cur.execute(sql_file.read_text())
        conn.commit()

@pytest.fixture()
def vault_client(pg, init_db, monkeypatch):
    """FastAPI TestClient for Vault service (backed by the ephemeral Postgres)."""
    url, _ = pg
    monkeypatch.setenv("DATABASE_URL", url)
    monkeypatch.setenv("DEMO_ALLOW_UNSIGNED", "1")
    mod = load_module(str(ROOT / "vault" / "app.py"), "vault_app")
    with TestClient(mod.app) as c:
        yield c

@pytest.fixture()
def gateway_client_sync(monkeypatch):
    """FastAPI TestClient for Gateway (sync path; external calls mocked with respx)."""
    monkeypatch.setenv("ASYNC_MODE", "0")
    monkeypatch.setenv("SIDE_URL", "http://side.local")
    monkeypatch.setenv("PROVIDER_URL", "http://prov.local")
    monkeypatch.setenv("VAULT_URL", "http://vault.local")
    mod = load_module(str(ROOT / "gateway" / "app.py"), "gateway_app_sync")
    with TestClient(mod.app) as c:
        yield c

@pytest.fixture()
def vault_module(pg, init_db, monkeypatch):
    """Return the loaded Vault module for direct function access."""
    url, _ = pg
    monkeypatch.setenv("DATABASE_URL", url)
    monkeypatch.setenv("DEMO_ALLOW_UNSIGNED", "1")
    return load_module(str(ROOT / "vault" / "app.py"), "vault_app_mod")