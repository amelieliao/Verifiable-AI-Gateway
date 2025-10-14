# FILE: sidecar/app.py
from __future__ import annotations
import os, time, logging
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from pythonjsonlogger import jsonlogger
from contextlib import asynccontextmanager

class Settings(BaseSettings):
    SIDECAR_PORT: int = 8081
    LOG_LEVEL: str = "INFO"
    OTEL_EXPORTER_OTLP_ENDPOINT: str | None = None
    class Config: env_file=".env"; extra="ignore"
s = Settings()

def setup_logging():
    h=logging.StreamHandler(); h.setFormatter(jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    r=logging.getLogger(); r.handlers.clear(); r.addHandler(h); r.setLevel(s.LOG_LEVEL)
setup_logging(); logger=logging.getLogger("sidecar")

def setup_tracing(app:FastAPI):
    try:
        if not s.OTEL_EXPORTER_OTLP_ENDPOINT: return
        from opentelemetry import trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        tp=TracerProvider(resource=Resource(attributes={"service.name":"tcd-sidecar"}))
        tp.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=s.OTEL_EXPORTER_OTLP_ENDPOINT)))
        trace.set_tracer_provider(tp); FastAPIInstrumentor.instrument_app(app)
    except Exception as e:
        logger.warning("otel init failed: %s", e)

REQ=Counter("tcd_sidecar_requests_total","",{ "path":"", "code":""})
LAT=Histogram("tcd_sidecar_latency_seconds","",{ "path":""}, buckets=(0.001,0.005,0.01,0.025,0.05,0.1,0.25))
INFO=Gauge("tcd_sidecar_info","",{ "version":""}); INFO.labels(version="0.2.0").set(1.0)

class DiagnoseIn(BaseModel):
    tenant:str; user:str; session:str; prompt:str; model_id:str="demo"
class DiagnoseOut(BaseModel):
    decision:str="allow"; degrade:dict={"temperature":0.7,"top_p":0.9}

@asynccontextmanager
async def lifespan(app:FastAPI):
    setup_tracing(app); yield
app=FastAPI(title="tcd-sidecar", version="0.2.0", lifespan=lifespan)

@app.middleware("http")
async def mw(request:Request, call_next):
    t0=time.perf_counter(); resp=None
    try:
        resp=await call_next(request); return resp
    finally:
        LAT.labels(path=request.url.path).observe(time.perf_counter()-t0)
        REQ.labels(path=request.url.path, code=str(getattr(resp,"status_code",500))).inc()

@app.get("/healthz")
def healthz(): return {"ok":True,"service":"sidecar"}

@app.get("/metrics")
def metrics(): return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/diagnose", response_model=DiagnoseOut)
def diagnose(_: DiagnoseIn):
    # demo：全部放行，可根据策略文件调整
    return DiagnoseOut()
