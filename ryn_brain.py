"""ذاكرة Ryn (حفظ آمن على القرص) + شات المتداول المحترف ومحلل السوق عبر Groq.

ملاحظة معمارية:
- الذكاء الاصطناعي مفصول تماماً عن تنفيذ أوامر الشراء والبيع حتى لا يسبب أي تأخير (Latency/Slippage).
- دور الذكاء الاصطناعي هنا حصرياً للشات: يعمل كمتداول ومحلل سوق محترف يقرأ المؤشرات الفنية الحية
  (RSI، المتوسطات المتحركة EMA، الدعوم والمقاومات، السيولة، والتذبذب) ويجيب على أسئلة المستخدم حول السوق.
"""

import copy
import json
import os
import threading
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
    """مخزن الحالة الوحيد: الإعدادات + الدورة + المحفظة + الصفقات + ذاكرة البوت + المحادثة."""

    def __init__(self, file_path: str = "./ryn_memory.json") -> None:
        self.file_path = os.getenv("MEMORY_FILE_PATH", file_path)
        self.lock = threading.RLock()
        self.state: Dict[str, Any] = self._load()
        self.save()

    def _default_memory(self) -> Dict[str, Any]:
        return {
            "assistant_name": "Ryn Pro Trader",
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
                "ws_connected": False,
                "ban_until": None,
                "last_ai_decision": None,
            },
            "cycle_state": {
                "stage": "IDLE_WATCHING",
                "reference_price": None,
                "live_price": None,
                "bid_price": None,
                "ask_price": None,
                "buy_trigger_price": None,
                "lowest_dip_tracked": None,
                "highest_peak_tracked": None,
                "raw_entry_price": None,
                "actual_entry_price": None,
                "target_sell_price": None,
                "stop_price": None,
                "position_amount": 0.0,
                "gross_amount": 0.0,
                "position_cost": 0.0,
                "dip_ticks": 0,
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
                0, {"time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), "tag": tag, "note": note}
            )
            s["ryn_self_written_market_memory"] = s["ryn_self_written_market_memory"][:40]

        self.update(_w)

    def add_chat(self, role: str, content: str) -> None:
        def _w(s: Dict[str, Any]) -> None:
            s["chat_history"].append({"role": role, "content": content, "time": _now_iso()})
            s["chat_history"] = s["chat_history"][-40:]

        self.update(_w)

    def clear_chat(self) -> None:
        self.update(lambda s: s.__setitem__("chat_history", []))


class RynGroqAssistant:
    """شخصية متداول ومحلل سوق محترف (للشات وتحليل السوق فقط دون التدخل في تنفيذ الصفقات)."""

    def __init__(self, memory_manager: RynMemoryManager) -> None:
        self.memory_manager = memory_manager
        self.api_key = os.getenv("GROQ_API_KEY", "")
        self.chat_model = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")
        self.fast_model = os.getenv("GROQ_FAST_MODEL", "openai/gpt-oss-20b")
        self.fallbacks = ["openai/gpt-oss-120b", "qwen/qwen3.6-27b", "openai/gpt-oss-20b"]
        self.context_provider: Optional[Callable[[], Dict[str, Any]]] = None
        self.market_analyzer: Optional[Callable[[], Dict[str, Any]]] = None
        self.client = None
        if self.api_key and Groq is not None:
            try:
                self.client = Groq(api_key=self.api_key, timeout=25.0)
            except Exception as exc:
                print(f"[Groq init failed]: {exc}")

    @property
    def enabled(self) -> bool:
        return self.client is not None

    def _complete(self, model: str, **kwargs: Any) -> Any:
        """استدعاء Groq مع تجربة نماذج بديلة إذا كان النموذج غير موجود."""
        candidates = [model] + [m for m in self.fallbacks if m != model]
        last: Optional[Exception] = None
        for name in candidates:
            try:
                kw = dict(kwargs)
                if name.startswith("openai/gpt-oss"):
                    kw["extra_body"] = {"reasoning_effort": "low"}
                resp = self.client.chat.completions.create(model=name, **kw)
                if not (resp.choices[0].message.content or "").strip():
                    raise ValueError("رد فارغ من النموذج")
                if name != model:
                    self.chat_model = name if model == self.chat_model else self.chat_model
                return resp
            except Exception as exc:
                last = exc
                msg = str(exc).lower()
                if not ("404" in msg or "model" in msg or "decommission" in msg or "فارغ" in msg):
                    break
        raise last  # type: ignore

    # ---- شات المتداول المحترف ومحلل السوق ----
    def chat_with_trader(self, user_message: str) -> Dict[str, Any]:
        if not self.client:
            raise AIUnavailable("مفتاح GROQ_API_KEY غير مضاف في إعدادات السيرفر، أضفه لتفعيل شات تحليل السوق.")
        live = self.context_provider() if self.context_provider else {}
        analysis = {}
        if self.market_analyzer:
            try:
                analysis = self.market_analyzer()
            except Exception:
                analysis = live.get("market_analysis") or {}
        else:
            analysis = live.get("market_analysis") or {}

        live_view = {
            "meta": live.get("meta"),
            "settings": live.get("settings"),
            "bot_status": live.get("bot"),
            "cycle_state": live.get("cycle_state"),
            "wallet": live.get("wallet"),
            "derived": live.get("derived"),
            "stats": live.get("stats"),
            "technical_analysis": analysis,
            "recent_trades": (live.get("trades_history") or [])[:8],
            "market_notes": (live.get("ryn_self_written_market_memory") or [])[:6],
        }
        system = (
            "أنت «Ryn»، متداول عملات رقمية محترف ومحلل فني خبير (Institutional Spot & Scalping Analyst) بخبرة تزيد عن 10 سنوات في أسواق الكريبتو وBinance.\n"
            "مهمتك الوحيدة هي التحدث مع المتداول في الشات، تحليل حركة السوق بعمق واحترافية، قراءة المؤشرات الفنية الحية، وتقديم استشارات تداول ذكية.\n"
            "أنت مفصول تماماً عن محرك التنفيذ الآلي (المحرك يشتري ويبيع تلقائياً وبسرعة فائقة بدون انتظارك)، لذا دورك هو المستشار والمحلل المحترف.\n\n"
            "قواعد شخصيتك وأسلوبك كمتداول محترف:\n"
            "1. تحدث بلهجة متداول محترف، واثق، صريح، ومباشر بالعربية، واستخدم المصطلحات الفنية في مكانها الصحيح (هيكل السوق، الزخم، RSI، المتوسطات EMA9/EMA21/EMA50، الدعم والمقاومة، التذبذب ATR، السيولة والسبريد).\n"
            "2. اعتمد حصرياً على البيانات الحية والمؤشرات الفنية الموجودة في `technical_analysis` و`cycle_state` و`wallet` أدناه. ممنوع اختراع أي أسعار أو أرقام غير موجودة.\n"
            "3. عندما يسألك المستخدم عن السوق أو العملة:\n"
            "   - وضح الاتجاه اللحظي والعام (صاعد / هابط / تجميع عرضي) بناءً على تقاطع EMA9 وEMA21 وEMA50.\n"
            "   - اذكر قراءة مؤشر القوة النسبية (RSI 14) وهل العملة في منطقة تشبع بيعي (فرصة ارتداد) أو تشبع شرائي (حذر من جني أرباح).\n"
            "   - حدد أقرب مستويات الدعم والمقاومة الرقمية من البيانات الحية.\n"
            "   - قدّم نصيحة عملية لإعدادات البوت (مثلاً: هل نسبة النزول للشراء `drop_buy_pct` ونسبة الربح `rise_sell_pct` مناسبتان لتذبذب السوق الحالي أم الأفضل تعديلهما).\n"
            "4. إذا كان البوت داخل صفقة حالياً (`IN_POSITION`)، قارن سعر التكلفة الفعلي (`actual_entry_price`) بالسعر الحالي وهدف البيع (`target_sell_price`) وأعطِ تقييمك الفني للصفقة.\n"
            "5. لا تعد بأرباح مضمونة أبداً، بل تكلم بلغة الاحتمالات وإدارة المخاطر كما يفعل كبار المتداولين.\n"
            '6. أرجع JSON فقط بهذا الشكل: {"reply": "ردك التحليلي المرتب والواضح", "memory_note": "ملاحظة فنية مختصرة لحفظها في الذاكرة أو نص فارغ"}\n\n'
            "البيانات والمؤشرات الفنية الحية الآن:\n" + json.dumps(live_view, ensure_ascii=False, default=str)
        )
        history = [
            {"role": m["role"], "content": m["content"]}
            for m in self.memory_manager.read_memory()["chat_history"][-12:]
            if m.get("role") in ("user", "assistant")
        ]
        messages = [{"role": "system", "content": system}, *history, {"role": "user", "content": user_message}]
        resp = self._complete(
            self.chat_model,
            messages=messages,
            temperature=0.35,
            max_tokens=2000,
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
            self.memory_manager.write_self_observation("تحليل Ryn", note[:300])
        return {"reply": reply}
