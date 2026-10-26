from contextlib import asynccontextmanager
from fastapi import FastAPI
from . import api, bus, ws


@asynccontextmanager
async def lifespan(app):
    await bus.r.ping()
    yield
    await bus.r.aclose()


app = FastAPI(title="MT5 Gateway", lifespan=lifespan)
app.include_router(ws.router)
app.include_router(api.router)


@app.get("/healthz")
async def healthz():
    await bus.r.ping()
    return {"ok": True}
