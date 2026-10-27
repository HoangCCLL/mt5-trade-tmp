from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from . import api, bus, ws


@asynccontextmanager
async def lifespan(app):
    await bus.r.ping()
    yield
    await bus.r.aclose()


app = FastAPI(title="MT5 Gateway", lifespan=lifespan)
app.include_router(ws.router)
app.include_router(api.router)
app.mount("/ui", StaticFiles(directory=Path(__file__).parent.parent / "ui", html=True), name="ui")  # same origin: no CORS, no domain config


@app.get("/healthz")
async def healthz():
    await bus.r.ping()
    return {"ok": True}
