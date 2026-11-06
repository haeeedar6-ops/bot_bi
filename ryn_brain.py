"""ذاكرة Ryn (حفظ آمن على القرص) + عقل الذكاء الاصطناعي عبر Groq."""

import copy
import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

try:
    from groq import Groq
except Exception:  # المكتبة غير مثبتة
    Groq = None  # type: ignore


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class AIUnavailable(Exception):
    """مفتاح Groq غير مضاف أو الخدمة غير متاحة."""


def _deep_merge(defaults: Dict[str, Any], loaded: Dict[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(defaults)
    for k, v in (loaded or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


class RynMemoryManager:
    """مخزن الحالة الوحيد: الإعدادات + الدورة + المحفظة + الصفقات + ذاكرة Ryn + المحادثة."""

    def __init__(self, file_path: str = "./ryn_memory.json") -> None:
        self.file_path = os.getenv("MEMORY_FILE_PATH", file_path)
        self.lock = threading.RLock()
        self.state: Dict[str, Any] = self._load()
        self.save()

    def _default_memory(self) -> Dict[str, Any]:
        return {
            "assistant_name": "Ryn",
            "last_updated": _now_iso(),
            "settings": {
                "symbol": os.getenv("TRADING_PAIR", "RAY/USDT").upper(),
                "drop_buy_pct": float(os.getenv("DROP_BUY_PERCENT", "2.0")),
                "rise_sell_pct": float(os.getenv("RISE_SELL_PERCENT", "2.0")),
                "order_size_usdt": float(os.getenv("TRADE_ORDER_USDT", "50.0")),
                "stop_loss_pct": float(os.getenv("STOP_LOSS_PERCENT", "0")),
            },
            "bot": {
                "running": False,
                "last_error": None,
                "halt_reason": None,
                "last_tick": None,
                "price_ok": False,
                "last_ai_decision": None,
            },
            "cycle_state": {
                "stage": "IDLE_WATCHING",
                "reference_price": None,
                "live_price": None,
                "buy_trigger_price": None,
                "lowest_dip_tracked": None,
                "actual_entry_price": None,
                "target_sell_price": None,
                "stop_price": None,
                "position_amount": 0.0,
                "position_cost": 0.0,
            },
            "wallet": {
                "base": None,
                "quote": None,
                "base_free": None,
                "quote_free": None,
                "synced_at": None,
            },
            "stats": {"completed_cycles": 0, "net_realized_profit_usdt": 0.0},
            "ryn_self_written_market_memory": [],
            "trades_history": [],
            "chat_history": [],
        }

    def _load(self) -> Dict[str, Any]:
        defaults = self._default_memory()
        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                return _deep_merge(defaults, json.load(f))
        except FileNotFoundError:
            return defaults
        except Exception:
            try:  # ملف تالف: احتفظ بنسخة ولا تضيعه
                os.replace(self.file_path, self.file_path + ".corrupt")
            except Exception:
                pass
            return defaults

    def save(self) -> None:
        with self.lock:
            self.state["last_updated"] = _now_iso()
            folder = os.path.dirname(os.path.abspath(self.file_path))
            os.makedirs(folder, exist_ok=True)
            tmp = self.file_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.state, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self.file_path)  # كتابة ذرّية: لا يتلف الملف لو انقطع التشغيل

    # ---- واجهة الاستخدام ----
    def read_memory(self) -> Dict[str, Any]:
        with self.lock:
            return copy.deepcopy(self.state)

    def update(self, fn: Callable[[Dict[str, Any]], None]) -> None:
        with self.lock:
            fn(self.state)
            self.save()

    def write_self_observation(self, tag: str, note: str) -> None:
        def _w(s: Dict[str, Any]) -> None:
            s["ryn_self_written_market_memory"].insert(
                0, {"time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"), "tag": tag, "note": note}
            )
            s["ryn_self_written_market_memory"] = s["ryn_self_written_market_memory"][:30]

        self.update(_w)

    def add_chat(self, role: str, content: str) -> None:
        def _w(s: Dict[str, Any]) -> None:
            s["chat_history"].append({"role": role, "content": content, "time": _now_iso()})
            s["chat_history"] = s["chat_history"][-40:]

        self.update(_w)

    def clear_chat(self) -> None:
        self.update(lambda s: s.__setitem__("chat_history", []))


class RynGroqAssistant:
    def __init__(self, memory_manager: RynMemoryManager) -> None:
        self.memory_manager = memory_manager
        self.api_key = os.getenv("GROQ_API_KEY", "")
        self.chat_model = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
        self.fast_model = os.getenv("GROQ_FAST_MODEL", "llama-3.1-8b-instant")
        self.context_provider: Optional[Callable[[], Dict[str, Any]]] = None
        self.client = None
        if self.api_key and Groq is not None:
            try:
                self.client = Groq(api_key=self.api_key, timeout=25.0)
            except Exception as exc:
                print(f"[Groq init failed]: {exc}")

    @property
    def enabled(self) -> bool:
        return self.client is not None

    # ---- فحص القاع (يستدعيه محرك التداول) ----
    def check_if_dip_stabilized(self, metrics: Dict[str, Any], lowest: float) -> Dict[str, Any]:
        if not self.client:
            raise AIUnavailable("no key")
        mem = self.memory_manager.read_memory()
        system = (
            "أنت Ryn، محلل لحظي لزوج تداول. السعر نزل عن المرجع بالنسبة المحددة. "
            "قرر: WAIT_FALLING_MORE إذا السوق ما زال ينزل بقوة، أو BUY_NOW_REVERSING إذا استقر القاع وبدأ الارتداد. "
            'أرجع JSON فقط بهذا الشكل: {"action": "...", "reason": "جملة عربية قصيرة"}'
        )
        user = {
            "metrics": metrics,
            "lowest_dip": lowest,
            "recent_notes": mem["ryn_self_written_market_memory"][:3],
        }
        resp = self.client.chat.completions.create(
            model=self.fast_model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
            ],
            temperature=0.1,
            max_tokens=150,
            response_format={"type": "json_object"},
        )
        out = json.loads(resp.choices[0].message.content)
        action = out.get("action")
        if action not in ("WAIT_FALLING_MORE", "BUY_NOW_REVERSING"):
            raise ValueError("رد غير مفهوم من النموذج")
        return {"action": action, "reason": str(out.get("reason", ""))[:200]}

    # ---- المحادثة الحقيقية ----
    def chat_with_trader(self, user_message: str) -> Dict[str, Any]:
        if not self.client:
            raise AIUnavailable("مفتاح GROQ_API_KEY غير مضاف في إعدادات Render، فما بقدر أرد.")
        live = self.context_provider() if self.context_provider else {}
        live_view = {
            "meta": live.get("meta"),
            "settings": live.get("settings"),
            "bot": live.get("bot"),
            "cycle_state": live.get("cycle_state"),
            "wallet": live.get("wallet"),
            "derived": live.get("derived"),
            "stats": live.get("stats"),
            "recent_trades": (live.get("trades_history") or [])[:8],
            "my_notes": (live.get("ryn_self_written_market_memory") or [])[:6],
        }
        system = (
            "أنت Ryn، مساعد تداول ذكي مرتبط ببوت تداول على Binance. جاوب بالعربية بشكل قصير وواضح ومباشر. "
            "اعتمد فقط على البيانات الحية أدناه (السعر، رصيد المحفظة الحقيقي، الإعدادات، الصفقات). "
            "ممنوع تخترع أرقام. إذا المحفظة غير مربوطة أو الرصيد null قل ذلك صراحة. "
            "لا تضمن أرباحاً ولا تتنبأ بالسعر بثقة. أنت لا تنفذ صفقات ولا تغيّر الإعدادات بنفسك؛ "
            "لتغييرها وجّه المستخدم لأزرار اللوحة. إذا طلب منك تسجّل ملاحظة أو لاحظت شيئاً مهماً، ضعه في memory_note. "
            'أرجع JSON فقط: {"reply": "ردك", "memory_note": "ملاحظة اختيارية أو فارغ"}\n\n'
            "البيانات الحية الآن:\n" + json.dumps(live_view, ensure_ascii=False, default=str)
        )
        history = [
            {"role": m["role"], "content": m["content"]}
            for m in self.memory_manager.read_memory()["chat_history"][-12:]
            if m.get("role") in ("user", "assistant")
        ]
        messages = [{"role": "system", "content": system}, *history, {"role": "user", "content": user_message}]
        resp = self.client.chat.completions.create(
            model=self.chat_model,
            messages=messages,
            temperature=0.4,
            max_tokens=700,
            response_format={"type": "json_object"},
        )
        raw = resp.choices[0].message.content or ""
        try:
            parsed = json.loads(raw)
            reply = str(parsed.get("reply") or "").strip() or raw
            note = str(parsed.get("memory_note") or "").strip()
        except Exception:
            reply, note = raw.strip(), ""
        self.memory_manager.add_chat("user", user_message)
        self.memory_manager.add_chat("assistant", reply)
        if note:
            self.memory_manager.write_self_observation("ملاحظة من Ryn", note[:300])
        return {"reply": reply}
