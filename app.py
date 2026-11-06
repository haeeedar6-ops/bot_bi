"""خادم FastAPI: لوحة تحكم + حلقة تداول خلفية + شات Ryn الحقيقي (جاهز لـ Render)."""

import asyncio
import hmac
import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from ryn_brain import AIUnavailable, RynGroqAssistant, RynMemoryManager
from trading_engine import RayTradingEngine

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
APP_PASSWORD = os.getenv("APP_PASSWORD", "")

memory_manager = RynMemoryManager()
ryn_ai = RynGroqAssistant(memory_manager)
engine = RayTradingEngine(memory_manager, ryn_ai)
ryn_ai.context_provider = engine.snapshot


async def bot_loop() -> None:
    while True:
        try:
            await asyncio.to_thread(engine.tick)
        except Exception as exc:  # لا نسمح لأي خطأ بقتل الحلقة
            print(f"[loop error]: {exc}")
        await asyncio.sleep(engine.poll_seconds)


@asynccontextmanager
async def lifespan(_: FastAPI):
    task = asyncio.create_task(bot_loop())
    yield
    task.cancel()


app = FastAPI(title="Ryn AI — بوت تداول Binance", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")


def require_auth(x_app_key: str = Header(default="")) -> None:
    if APP_PASSWORD and not hmac.compare_digest(x_app_key.encode(), APP_PASSWORD.encode()):
        raise HTTPException(status_code=401, detail="كلمة السر غلط")


class SettingsPayload(BaseModel):
    symbol: Optional[str] = None
    drop_buy_pct: Optional[float] = None
    rise_sell_pct: Optional[float] = None
    order_size_usdt: Optional[float] = None
    stop_loss_pct: Optional[float] = None


class ChatPayload(BaseModel):
    message: str = Field(min_length=1, max_length=1000)


@app.api_route("/", methods=["GET", "HEAD"])
async def index_page() -> FileResponse:
    return FileResponse(os.path.join(BASE_DIR, "templates", "index.html"), headers={"Cache-Control": "no-cache"})


@app.get("/sw.js")
async def service_worker() -> FileResponse:
    return FileResponse(os.path.join(BASE_DIR, "static", "sw.js"), media_type="application/javascript",
                        headers={"Service-Worker-Allowed": "/", "Cache-Control": "no-cache"})


@app.get("/api/health")
async def health() -> JSONResponse:  # للـ UptimeRobot حتى لا ينام السيرفر على Render
    return JSONResponse({"ok": True})


@app.get("/api/status", dependencies=[Depends(require_auth)])
async def get_status() -> JSONResponse:
    return JSONResponse(engine.snapshot())


@app.post("/api/bot/start", dependencies=[Depends(require_auth)])
async def bot_start() -> JSONResponse:
    try:
        await asyncio.to_thread(engine.start)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse(engine.snapshot())


@app.post("/api/bot/stop", dependencies=[Depends(require_auth)])
async def bot_stop() -> JSONResponse:
    engine.stop()
    return JSONResponse(engine.snapshot())


@app.post("/api/settings", dependencies=[Depends(require_auth)])
async def save_settings(payload: SettingsPayload) -> JSONResponse:
    try:
        await asyncio.to_thread(engine.update_settings, payload.model_dump(exclude_none=True))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse(engine.snapshot())


@app.get("/api/chat/history", dependencies=[Depends(require_auth)])
async def chat_history() -> JSONResponse:
    return JSONResponse({"messages": memory_manager.read_memory()["chat_history"]})


@app.post("/api/chat", dependencies=[Depends(require_auth)])
async def chat_endpoint(payload: ChatPayload) -> JSONResponse:
    try:
        result = await asyncio.to_thread(ryn_ai.chat_with_trader, payload.message.strip())
    except AIUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"تعذر الاتصال بـ Groq: {str(exc)[:200]}")
    return JSONResponse(result)


@app.post("/api/chat/clear", dependencies=[Depends(require_auth)])
async def chat_clear() -> JSONResponse:
    memory_manager.clear_chat()
    return JSONResponse({"ok": True})


@app.get("/api/memory", dependencies=[Depends(require_auth)])
async def get_memory() -> JSONResponse:
    return JSONResponse(engine.snapshot())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)
