# FILE: vault/app.py
from __future__ import annotations
import os, time, logging
from typing import Optional, Dict, Any, Tuple, List
from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from pythonjsonlogger import jsonlogger
from contextlib import asynccontextmanager
import blake3, orjson as _json
import psycopg_pool, psycopg

# -------- Settings --------
class Settings(BaseSettings):
    VAULT_PORT: int = 8083
    DATABASE_URL: str
    VAULT_SHARED_SECRET: Optional[str] = None
    DEMO_ALLOW_UNSIGNED: bool = True
    LOG_LEVEL: str = "INFO"
    OTEL_EXPORTER_OTLP_ENDPOINT: Optional[str] = None
    class Config: env_file=".env"; extra="ignore"
s = Settings()

# -------- Logging --------
def setup_logging():
    h = logging.StreamHandler()
    h.setFormatter(jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    r = logging.getLogger(); r.handlers.clear(); r.addHandler(h); r.setLevel(s.LOG_LEVEL)
setup_logging(); logger = logging.getLogger("vault")

# -------- OTel --------
def setup_tracing(app:FastAPI):
    try:
        if not s.OTEL_EXPORTER_OTLP_ENDPOINT: return
        from opentelemetry import trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        tp = TracerProvider(resource=Resource(attributes={"service.name":"tcd-vault"}))
        tp.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=s.OTEL_EXPORTER_OTLP_ENDPOINT)))
        trace.set_tracer_provider(tp); FastAPIInstrumentor.instrument_app(app)
    except Exception as e:
        logger.warning("otel init failed: %s", e)

# -------- Pool --------
pool: psycopg_pool.ConnectionPool | None = None
def init_pool():
    global pool; pool = psycopg_pool.ConnectionPool(s.DATABASE_URL, min_size=1, max_size=10)
def close_pool():
    global pool
    if pool: pool.close(); pool=None

# -------- Metrics --------
PUT_OK = Counter("tcd_vault_put_ok_total","",{ "tenant":""})
PUT_CONFLICT = Counter("tcd_vault_put_conflict_total","",{ "tenant":""})
PUT_UNAUTH = Counter("tcd_vault_put_unauth_total","",{ "tenant":""})
STORE_COUNT = Gauge("tcd_vault_store_count","total receipts")
LAT = Histogram("tcd_vault_latency_seconds","",{ "path":""}, buckets=(0.001,0.005,0.01,0.02,0.05,0.1,0.25))
INFO = Gauge("tcd_vault_info","",{ "version":""}); INFO.labels(version="0.2.0").set(1.0)

# -------- Models --------
class ReceiptBody(BaseModel):
    ts_ns: int
    tenant: str
    prev: str
    ctx: Dict[str, Any] = Field(default_factory=dict)
    body: Dict[str, Any] = Field(default_factory=dict)

class ReceiptPutIn(BaseModel):
    body: ReceiptBody

class ReceiptPutOut(BaseModel):
    ok: bool
    head_hex: str
    prev: str
    count: int

# -------- Helpers --------
def zeros() -> str: return "0x"+"00"*32
def canonical_json(obj: Any) -> str: return _json.dumps(obj, option=_json.OPT_SORT_KEYS).decode()
def head_from_body(body_json: str, tenant: str) -> str:
    h=blake3.blake3(); h.update(b"tcd:receipt"); h.update(tenant.encode()); h.update(body_json.encode()); return "0x"+h.hexdigest()

def verify_sig(tenant: str, body_json: str, x_sig: Optional[str]) -> bool:
    if s.DEMO_ALLOW_UNSIGNED: return True
    if not s.VAULT_SHARED_SECRET or not x_sig: return False
    d=blake3.blake3(); d.update(s.VAULT_SHARED_SECRET.encode()); d.update(body_json.encode())
    return (d.hexdigest()==x_sig)

# -------- Lifespan --------
from fastapi import Request
@asynccontextmanager
async def lifespan(app:FastAPI):
    init_pool(); setup_tracing(app); yield; close_pool()
app = FastAPI(title="tcd-vault", version="0.2.0", lifespan=lifespan)

@app.middleware("http")
async def mw(request:Request, call_next):
    t0=time.perf_counter(); resp=None
    try:
        resp=await call_next(request); return resp
    finally:
        LAT.labels(path=request.url.path).observe(time.perf_counter()-t0)

@app.get("/healthz")
def healthz(): return {"ok":True,"service":"vault"}

@app.get("/metrics")
def metrics(): return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/last_head")
def last_head(tenant: str = Query(..., min_length=1)):
    with pool.connection() as conn:
        row = conn.execute("SELECT last_head FROM tenant_state WHERE tenant=%s", (tenant,)).fetchone()
        last = row[0] if row else zeros()
        return {"tenant": tenant, "last_head_hex": last}

@app.get("/tail")
def tail(tenant: str = Query(..., min_length=1), n: int = Query(100, ge=1, le=1000)):
    with pool.connection() as conn:
        rows = conn.execute("SELECT head_hex, body FROM receipts WHERE tenant=%s ORDER BY ts_ns DESC LIMIT %s",
                            (tenant, n)).fetchall()
        items = []
        for head, body in rows:
            items.append([head, canonical_json(body)])
        cnt = conn.execute("SELECT count(*) FROM receipts WHERE tenant=%s", (tenant,)).fetchone()[0]
        STORE_COUNT.set(cnt)
        return {"tenant": tenant, "items": items, "count": cnt}

@app.post("/receipts", response_model=ReceiptPutOut)
def put_receipt(req: ReceiptPutIn, x_sig: Optional[str] = Header(default=None, alias="X-Sig")):
    body = req.body.model_dump()
    body_json = canonical_json(body)
    tenant = body["tenant"]
    if not verify_sig(tenant, body_json, x_sig):
        PUT_UNAUTH.labels(tenant=tenant).inc()
        raise HTTPException(401, "unauthorized")
    head = head_from_body(body_json, tenant)

    with pool.connection() as conn:
        with conn.transaction():
            row = conn.execute("SELECT last_head FROM tenant_state WHERE tenant=%s FOR UPDATE", (tenant,)).fetchone()
            last = row[0] if row else zeros()
            if body["prev"] != last:
                PUT_CONFLICT.labels(tenant=tenant).inc()
                raise HTTPException(status_code=409, detail={"expected_prev":last, "got_prev":body["prev"]})
            conn.execute(
                "INSERT INTO receipts (tenant, head_hex, prev, ts_ns, ctx, body) VALUES (%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (head_hex) DO NOTHING",
                (tenant, head, body["prev"], body["ts_ns"], body["ctx"], body["body"])
            )
            conn.execute(
                "INSERT INTO tenant_state (tenant, last_head) VALUES (%s,%s) "
                "ON CONFLICT (tenant) DO UPDATE SET last_head=EXCLUDED.last_head",
                (tenant, head)
            )
        cnt = conn.execute("SELECT count(*) FROM receipts WHERE tenant=%s",(tenant,)).fetchone()[0]
        STORE_COUNT.set(cnt)
    PUT_OK.labels(tenant=tenant).inc()
    return ReceiptPutOut(ok=True, head_hex=head, prev=body["prev"], count=cnt)
