"""خادم FastAPI الرئيسي لمنصة وبوت تداول RAY/USDT مع المساعد الذكي Ryn (جاهز للنشر على Render)."""

import os
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from ryn_brain import RynGroqAssistant, RynMemoryManager
from trading_engine import RayTradingEngine

app = FastAPI(title="Ryn AI — RAY/USDT Trading Platform & Bot")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

memory_manager = RynMemoryManager()
engine = RayTradingEngine(memory_manager)
ryn_ai = RynGroqAssistant(memory_manager)


class ChatPayload(BaseModel):
    message: str


@app.get("/", response_class=HTMLResponse)
async def index_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/status")
async def get_status() -> JSONResponse:
    metrics = engine.fetch_live_metrics()
    mem = memory_manager.read_memory()
    mem["cycle_state"]["live_price"] = metrics["live_price"]
    memory_manager.save_memory(mem)
    return JSONResponse(mem)


@app.post("/api/chat")
async def chat_endpoint(payload: ChatPayload) -> JSONResponse:
    result = ryn_ai.chat_with_trader(payload.message)
    return JSONResponse({"result": result, "memory": memory_manager.read_memory()})


@app.get("/api/memory")
async def get_memory() -> JSONResponse:
    return JSONResponse(memory_manager.read_memory())


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
