# FILE: gateway/app.py
from __future__ import annotations
import os, time, logging, hmac
from typing import Optional, Dict, Any
import httpx
from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from pythonjsonlogger import jsonlogger
from contextlib import asynccontextmanager
import orjson as _json
import blake3

# -------- Settings --------
class Settings(BaseSettings):
    SIDE_URL: str = "http://sidecar:8081"
    PROVIDER_URL: str = "http://provider:8082"
    VAULT_URL: str = "http://vault:8083"
    GATEWAY_PORT: int = 8080
    GATEWAY_SHARED_SECRET: Optional[str] = None
    AUTH_JWT_SECRET: Optional[str] = None  # HS256 demo
    RATE_LIMIT: str = "50/second"
    LOG_LEVEL: str = "INFO"
    OTEL_EXPORTER_OTLP_ENDPOINT: Optional[str] = None
    ASYNC_MODE: bool = False
    KAFKA_BOOTSTRAP: Optional[str] = None
    OUTBOX_PATH: str = "/data/outbox.db"

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()
SIDE_URL, PROVIDER_URL, VAULT_URL = settings.SIDE_URL, settings.PROVIDER_URL, settings.VAULT_URL

# -------- Logging --------
def setup_logging():
    h = logging.StreamHandler()
    h.setFormatter(jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(h)
    root.setLevel(settings.LOG_LEVEL)
setup_logging()
logger = logging.getLogger("gateway")

# -------- OTel --------
def setup_tracing(app: FastAPI):
    try:
        if not settings.OTEL_EXPORTER_OTLP_ENDPOINT: return
        from opentelemetry import trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
        provider = TracerProvider(resource=Resource(attributes={"service.name":"tcd-gateway"}))
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.OTEL_EXPORTER_OTLP_ENDPOINT)))
        trace.set_tracer_provider(provider)
        FastAPIInstrumentor.instrument_app(app)
        HTTPXClientInstrumentor().instrument()
    except Exception as e:
        logger.warning("otel init failed: %s", e)

# -------- Metrics --------
REQ_TOTAL = Counter("tcd_gateway_http_requests_total","requests",{ "path":"", "code":""})
LATENCY  = Histogram("tcd_gateway_http_latency_seconds","latency",{ "path":""}, buckets=(0.005,0.01,0.025,0.05,0.1,0.25,0.5,1.0))
ERRORS   = Counter("tcd_gateway_errors_total","errors",{ "where":""})
INFO     = Gauge("tcd_gateway_info","build info",{ "version":""})
INFO.labels(version="0.2.0").set(1.0)

# -------- Models --------
class InferIn(BaseModel):
    tenant: str
    user: str
    session: str
    prompt: str
    model_id: str = "demo"

class InferOut(BaseModel):
    tenant: str
    user: str
    session: str
    answer: str
    queued: bool = False
    proposed_head_hex: Optional[str] = None

# -------- Helpers --------
def canonical_json(obj: Any) -> str:
    return _json.dumps(obj, option=_json.OPT_SORT_KEYS).decode()

def sign_body(tenant: str, body_json: str) -> Optional[str]:
    if not settings.GATEWAY_SHARED_SECRET:
        return None
    # demo：BLAKE3(secret || body)
    d = blake3.blake3()
    d.update(settings.GATEWAY_SHARED_SECRET.encode())
    d.update(body_json.encode())
    return d.hexdigest()

async def vault_last_head(c: httpx.AsyncClient, tenant: str) -> str:
    r = await c.get(f"{VAULT_URL}/last_head", params={"tenant":tenant})
    if r.status_code != 200:
        raise HTTPException(502, f"vault last_head failed {r.status_code}")
    return r.json()["last_head_hex"]

async def call_sidecar(c: httpx.AsyncClient, req: InferIn) -> Dict[str, Any]:
    r = await c.post(f"{SIDE_URL}/diagnose", json=req.model_dump())
    r.raise_for_status()
    return r.json()

async def call_provider(c: httpx.AsyncClient, prompt: str) -> Dict[str, Any]:
    r = await c.post(f"{PROVIDER_URL}/infer", json={"prompt":prompt})
    r.raise_for_status()
    return r.json()

# -------- JWT (HS256 demo) --------
from fastapi.security import OAuth2PasswordBearer
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)

def require_jwt(token: Optional[str] = Depends(oauth2_scheme)) -> dict:
    if not settings.AUTH_JWT_SECRET:  # 关闭认证（演示）
        return {}
    import jwt as pyjwt
    if not token: raise HTTPException(401,"missing bearer token")
    try:
        return pyjwt.decode(token, settings.AUTH_JWT_SECRET, algorithms=["HS256"])
    except Exception as e:
        raise HTTPException(401, f"invalid token: {e}")

# -------- Lifespan --------
ASYNC_MODE = bool(int(os.getenv("ASYNC_MODE","0")))
if ASYNC_MODE:
    from .app_asyncoutbox import start as bus_start, stop as bus_stop, enqueue, head_hex as _head

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.client = httpx.AsyncClient(
        timeout=httpx.Timeout(connect=0.5, read=2.0, write=1.0),
        limits=httpx.Limits(max_keepalive_connections=100, max_connections=200),
    )
    setup_tracing(app)
    if ASYNC_MODE:
        await bus_start()
    yield
    await app.state.client.aclose()
    if ASYNC_MODE:
        await bus_stop()

app = FastAPI(title="tcd-gateway", version="0.2.0", lifespan=lifespan)

@app.middleware("http")
async def _metrics_mw(request: Request, call_next):
    t0 = time.perf_counter()
    resp = None
    try:
        resp = await call_next(request)
        return resp
    finally:
        dt = time.perf_counter() - t0
        path = request.url.path
        code = getattr(resp, "status_code", 500)
        LATENCY.labels(path=path).observe(dt)
        REQ_TOTAL.labels(path=path, code=str(code)).inc()

@app.get("/healthz")
def healthz(): return {"ok": True, "service":"gateway"}

@app.get("/metrics")
def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/infer", response_model=InferOut)
async def infer(req: InferIn, request: Request, auth: dict = Depends(require_jwt)):
    c: httpx.AsyncClient = request.app.state.client
    decision = (await call_sidecar(c, req)).get("decision", "allow")
    if decision == "block":
        answer = "[blocked by policy]"
    else:
        pr = await call_provider(c, req.prompt)
        answer = pr.get("answer","")

    if not ASYNC_MODE:
        # 同步写 Vault
        prev = await vault_last_head(c, req.tenant)
        body = {
            "ts_ns": int(time.time_ns()),
            "tenant": req.tenant,
            "prev": prev,
            "ctx": {"tenant":req.tenant,"user":req.user,"session":req.session,"model":req.model_id,"decision":decision},
            "body": {"req":{"prompt":req.prompt}, "comp":{"answer":answer}}
        }
        body_json = canonical_json(body)
        sig = sign_body(req.tenant, body_json)
        headers = {"X-Sig": sig} if sig else {}
        r = await c.post(f"{VAULT_URL}/receipts", json={"body":body}, headers=headers)
        if r.status_code not in (200,201):
            raise HTTPException(502, f"vault put failed {r.status_code}: {r.text}")
        head = r.json()["head_hex"]
        return InferOut(tenant=req.tenant, user=req.user, session=req.session, answer=answer,
                        queued=False, proposed_head_hex=head)
    else:
        # 异步事件：只入队，快速返回
        prev = await vault_last_head(c, req.tenant)
        body = {
            "ts_ns": int(time.time_ns()),
            "tenant": req.tenant,
            "prev": prev,
            "ctx": {"tenant":req.tenant,"user":req.user,"session":req.session,"model":req.model_id,"decision":decision},
            "body": {"req":{"prompt":req.prompt}, "comp":{"answer":answer}}
        }
        bjson = canonical_json(body)
        proposed = _head(req.tenant, bjson)
        event = {"event_type":"receipt_proposed.v1","tenant":req.tenant,"head_hex":proposed, **body}
        enqueue(event)
        return InferOut(tenant=req.tenant, user=req.user, session=req.session, answer=answer,
                        queued=True, proposed_head_hex=proposed)