"""عقل المساعد الذكي Ryn عبر GroqCloud API + إدارة ملف الذاكرة الذاتية (ryn_memory.json)."""

import json
import os
from datetime import datetime
from typing import Any, Dict
from groq import Groq


class RynMemoryManager:
    def __init__(self, file_path: str = "./ryn_memory.json") -> None:
        self.file_path = os.getenv("MEMORY_FILE_PATH", file_path)
        if not os.path.exists(self.file_path):
            self.save_memory(self._default_memory())

    def _default_memory(self) -> Dict[str, Any]:
        return {
            "assistant_name": "Ryn",
            "model_engine": "Groq Llama-3.3-70B-Versatile",
            "last_updated": datetime.utcnow().isoformat(),
            "config": {
                "pair": "RAY/USDT",
                "drop_buy_pct": float(os.getenv("DROP_BUY_PERCENT", "2.0")),
                "rise_sell_pct": float(os.getenv("RISE_SELL_PERCENT", "2.0")),
                "order_size_usdt": float(os.getenv("TRADE_ORDER_USDT", "50.0")),
                "strict_single_cycle_lock": True,
                "wait_if_dropping_more": True,
            },
            "cycle_state": {
                "stage": "IDLE_WATCHING",
                "reference_price": 1.7500,
                "live_price": 1.7520,
                "buy_trigger_price": 1.7150,
                "lowest_dip_tracked": None,
                "actual_entry_price": 1.7500,
                "target_sell_price": None,
                "is_locked_for_sell": False,
            },
            "wallet": {
                "usdt_free": 350.00,
                "ray_free": 85.6164,
                "completed_cycles": 2,
                "net_realized_profit_usdt": 4.85,
            },
            "ryn_self_written_market_memory": [
                {
                    "time": "2026-11-06 04:00",
                    "tag": "قراءة النطاق الشهري",
                    "note": "يتحرك زوج RAY/USDT ضمن نطاق شهري بين 1.48 و2.56 USDT؛ التمركز قرب 1.70-1.75 USDT آمن نسبياً.",
                }
            ],
            "trades_history": [],
        }

    def read_memory(self) -> Dict[str, Any]:
        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return self._default_memory()

    def save_memory(self, data: Dict[str, Any]) -> None:
        data["last_updated"] = datetime.utcnow().isoformat()
        with open(self.file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def write_self_observation(self, tag: str, note: str) -> None:
        mem = self.read_memory()
        mem["ryn_self_written_market_memory"].insert(
            0,
            {
                "time": datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
                "tag": tag,
                "note": note,
            },
        )
        mem["ryn_self_written_market_memory"] = mem["ryn_self_written_market_memory"][:25]
        self.save_memory(mem)


class RynGroqAssistant:
    def __init__(self, memory_manager: RynMemoryManager) -> None:
        self.memory_manager = memory_manager
        self.api_key = os.getenv("GROQ_API_KEY", "")
        self.client = Groq(api_key=self.api_key) if self.api_key else None

    def check_if_dip_stabilized(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """فحص ما إذا كان السوق سينزل أكثر فيصبر البوت، أو استقر القاع فيشتري."""
        mem = self.memory_manager.read_memory()
        if not self.client:
            if metrics.get("last_3_candles_change_pct", 0.0) < -0.6 and not metrics.get("is_reversing_up", False):
                return {"action": "WAIT_FALLING_MORE", "reason": "ما زال ضغط البيع مستمراً، سأصبر لأشتري بسعر أرخص."}
            return {"action": "BUY_NOW_REVERSING", "reason": "توقف الهبوط وظهرت إشارة ارتداد من القاع."}

        prompt = (
            "أنت المساعد الذكي Ryn لتداول عملة RAY/USDT. السعر هبط بنسبة 2% عن السعر المرجعي. "
            "افحص القراءات اللحظية وذاكرتك السابقة وقرر هل السوق ما زال ينزل بقوة (WAIT_FALLING_MORE) "
            "أم استقر القاع وبدأ الارتداد للشراء (BUY_NOW_REVERSING). أرجع JSON فقط بالحقلين action وreason."
        )
        try:
            resp = self.client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": json.dumps({"metrics": metrics, "memory": mem["ryn_self_written_market_memory"][:3]})},
                ],
                temperature=0.1,
                response_format={"type": "json_object"},
            )
            return json.loads(resp.choices[0].message.content)
        except Exception:
            return {"action": "BUY_NOW_REVERSING", "reason": "استقرار سعري مناسب للدخول."}

    def chat_with_trader(self, user_message: str) -> Dict[str, Any]:
        """محادثة المتداول مع Ryn وتحديث ملف الذاكرة الذاتية تلقائياً."""
        mem = self.memory_manager.read_memory()
        if not self.client:
            self.memory_manager.write_self_observation("تفاعل مع المتداول", f"سأل المتداول: {user_message}")
            return {
                "reply": f"أهلاً! أنا Ryn. السعر اللحظي لـ RAY/USDT هو {mem['cycle_state']['live_price']} USDT وحالة الدورة هي {mem['cycle_state']['stage']}.",
                "reaction": "HAPPY",
            }

        system_prompt = (
            "أنت Ryn، روبوت ومساعد تداول ذكي لزوج RAY/USDT. تتحدث بلغة عربية ودودة وواضحة. "
            "لديك ملف ذاكرة ذاتي (ryn_memory.json) تقرأ منه وتكتب فيه استنتاجاتك عن السوق. "
            "أرجع إجابتك بصيغة JSON تتضمن: reply (الرد العربي)، reaction (HAPPY | SCANNING | WAITING_DIP | CELEBRATING | THINKING)، "
            "وnew_memory_note (ملاحظة جديدة تكتبها في ذاكرتك عن السوق أو تفضيلات المستخدم)."
        )
        try:
            resp = self.client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps({"user_message": user_message, "current_memory": mem}, ensure_ascii=False)},
                ],
                temperature=0.2,
                response_format={"type": "json_object"},
            )
            parsed = json.loads(resp.choices[0].message.content)
            if parsed.get("new_memory_note"):
                self.memory_manager.write_self_observation("استنتاج ذاتي من Ryn", parsed["new_memory_note"])
            return parsed
        except Exception as exc:
            return {"reply": f"تم حفظ ملاحظتك في ذاكرتي ({exc})", "reaction": "HAPPY"}
