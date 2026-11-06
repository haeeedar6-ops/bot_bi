"""محرك التداول الحقيقي عبر CCXT وBinance Spot.

الاستراتيجية: يشتري بعد نزول X% عن أعلى سعر مرجعي (مع صبر وفحص القاع)، ويبيع عند +Y% من سعر الدخول.
قفل الدورة: لا تبدأ دورة جديدة إلا بعد تأكيد البيع وتحديث الرصيد.
لا يوجد أي تداول وهمي: بدون مفاتيح Binance صحيحة لا يعمل البوت.
"""

import os
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import ccxt

IDLE, DIP, POS = "IDLE_WATCHING", "INSPECTING_DIP", "IN_POSITION"
FALLBACK_FEE = 0.001  # 0.1% عند غياب معلومات الرسوم


class OrderError(Exception):
    """خطأ في أمر حقيقي: يوقف البوت لحماية المحفظة."""


class SoftError(Exception):
    """خطأ مؤقت: يظهر للمستخدم والبوت يعيد المحاولة."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def explain_binance_error(exc: Exception, mode: str) -> str:
    """ترجمة أخطاء Binance الشائعة لسبب وحل بالعربي."""
    msg = str(exc)
    if "-2015" in msg or "-2014" in msg or "-1022" in msg:
        other = "حقيقي (live)" if mode == "testnet" else "تجريبي (testnet)"
        return (f"Binance رفض المفاتيح (-2015). الأسباب المحتملة: (1) المفاتيح مو من نفس وضع البوت الحالي ({mode}) "
                f"— غالباً مفاتيحك من وضع {other}. (2) مفتاح مقيّد بعناوين IP وسيرفر Render مو ضمنها. "
                "(3) صلاحية Enable Spot & Margin Trading مو مفعّلة. (4) في فراغ زيادة بالمفتاح أو السر.")
    if "451" in msg or "restricted location" in msg.lower():
        return "Binance بيحجب منطقة سيرفر Render. غيّر Region للسيرفر (مثلاً Frankfurt أو Singapore)."
    return msg[:200]


class RayTradingEngine:
    def __init__(self, memory_manager: Any, assistant: Any = None) -> None:
        self.store = memory_manager
        self.assistant = assistant
        self.mode = os.getenv("TRADING_MODE", "testnet").lower()
        if self.mode not in ("testnet", "live"):
            self.mode = "testnet"
        self.poll_seconds = max(5, int(float(os.getenv("POLL_SECONDS", "10"))))
        self.ai_interval = 30
        self.api_key = os.getenv("EXCHANGE_API_KEY", "").strip()
        self.api_secret = os.getenv("EXCHANGE_API_SECRET", "").strip()
        self.connected = bool(self.api_key and self.api_secret)
        self.exchange = ccxt.binance({
            "apiKey": self.api_key,
            "secret": self.api_secret,
            "enableRateLimit": True,
            "options": {"defaultType": "spot", "adjustForTimeDifference": True},
        })
        if self.mode == "testnet":
            self.exchange.set_sandbox_mode(True)
        self.tick_lock = threading.Lock()
        self._markets_loaded = False
        self._last_wallet_sync = 0.0
        self._last_ai_ts = 0.0

    # ------------------------------------------------------------------ أدوات
    @property
    def symbol(self) -> str:
        return self.store.read_memory()["settings"]["symbol"]

    def _ensure_market(self, symbol: str) -> Dict[str, Any]:
        if not self._markets_loaded:
            self.exchange.load_markets()
            self._markets_loaded = True
        if symbol not in self.exchange.markets:
            where = "حساب التجربة (Testnet)" if self.mode == "testnet" else "Binance"
            raise SoftError(f"الزوج {symbol} غير متوفر على {where}.")
        return self.exchange.markets[symbol]

    def _set_error(self, msg: Optional[str]) -> None:
        self.store.update(lambda s: s["bot"].__setitem__("last_error", msg))

    def _halt(self, msg: str) -> None:
        def _w(s: Dict[str, Any]) -> None:
            s["bot"]["running"] = False
            s["bot"]["halt_reason"] = msg
            s["bot"]["last_error"] = None
        self.store.update(_w)
        self.store.write_self_observation("إيقاف تلقائي", msg)

    # ------------------------------------------------------------------ بيانات السوق والمحفظة
    def fetch_live_metrics(self) -> Dict[str, Any]:
        sym = self.symbol
        self._ensure_market(sym)
        ticker = self.exchange.fetch_ticker(sym)
        price = float(ticker["last"])
        try:
            ohlcv = self.exchange.fetch_ohlcv(sym, timeframe="5m", limit=15)
        except Exception:
            ohlcv = []
        closes = [float(c[4]) for c in ohlcv] or [price]
        closes[-1] = price
        change3 = ((closes[-1] - closes[-3]) / closes[-3] * 100) if len(closes) >= 3 and closes[-3] else 0.0
        return {
            "live_price": price,
            "high_24h": float(ticker.get("high") or price),
            "low_24h": float(ticker.get("low") or price),
            "last_3_candles_change_pct": round(change3, 3),
            "is_reversing_up": len(closes) >= 2 and closes[-1] > closes[-2],
        }

    def sync_wallet(self) -> Dict[str, Any]:
        """قراءة الرصيد الحقيقي من Binance وحفظه."""
        market = self._ensure_market(self.symbol)
        bal = self.exchange.fetch_balance()
        free = bal.get("free", {}) or {}
        base, quote = market["base"], market["quote"]

        def _w(s: Dict[str, Any]) -> None:
            s["wallet"] = {
                "base": base,
                "quote": quote,
                "base_free": float(free.get(base) or 0.0),
                "quote_free": float(free.get(quote) or 0.0),
                "synced_at": _now_iso(),
            }
        self.store.update(_w)
        self._last_wallet_sync = time.time()
        return self.store.read_memory()["wallet"]

    # ------------------------------------------------------------------ التحكم
    def start(self) -> None:
        if not self.connected:
            raise ValueError("مفاتيح Binance غير مضافة (EXCHANGE_API_KEY / EXCHANGE_API_SECRET).")
        if self.mode == "live" and not os.getenv("APP_PASSWORD"):
            raise ValueError("الوضع الحقيقي يتطلب كلمة سر للوحة (APP_PASSWORD) حتى لا يتحكم أحد بأموالك.")
        try:
            self._ensure_market(self.symbol)
            self.sync_wallet()
        except SoftError as exc:
            raise ValueError(str(exc))
        except Exception as exc:
            raise ValueError(f"تعذر الاتصال بـ Binance: {explain_binance_error(exc, self.mode)}")

        def _w(s: Dict[str, Any]) -> None:
            s["bot"]["running"] = True
            s["bot"]["halt_reason"] = None
            s["bot"]["last_error"] = None
        self.store.update(_w)
        self.store.write_self_observation("تشغيل", "تم تشغيل البوت من اللوحة.")

    def stop(self) -> None:
        self.store.update(lambda s: s["bot"].__setitem__("running", False))
        self.store.write_self_observation("إيقاف", "تم إيقاف البوت من اللوحة.")

    def update_settings(self, patch: Dict[str, Any]) -> Dict[str, Any]:
        with self.tick_lock:  # لا نغيّر الإعدادات أثناء تنفيذ أمر
            s = self.store.read_memory()
            cfg = dict(s["settings"])
            stage = s["cycle_state"]["stage"]

            def num(key: str, lo: float, hi: float, label: str, allow_zero: bool = False) -> float:
                v = float(patch[key])
                if allow_zero and v == 0:
                    return 0.0
                if not (lo <= v <= hi):
                    raise ValueError(f"{label} لازم بين {lo} و{hi}.")
                return v

            if "drop_buy_pct" in patch:
                cfg["drop_buy_pct"] = num("drop_buy_pct", 0.1, 50, "نسبة النزول للشراء")
            if "rise_sell_pct" in patch:
                cfg["rise_sell_pct"] = num("rise_sell_pct", 0.1, 100, "نسبة الارتفاع للبيع")
            if "stop_loss_pct" in patch:
                cfg["stop_loss_pct"] = num("stop_loss_pct", 0.1, 90, "وقف الخسارة", allow_zero=True)
            if "order_size_usdt" in patch:
                cfg["order_size_usdt"] = num("order_size_usdt", 1, 1_000_000, "مبلغ الشراء")
            if "symbol" in patch and str(patch["symbol"]).upper() != cfg["symbol"]:
                sym = str(patch["symbol"]).upper().strip()
                if not re.fullmatch(r"[A-Z0-9]{2,12}/[A-Z]{3,5}", sym):
                    raise ValueError("صيغة الزوج غلط. مثال: RAY/USDT")
                if stage != IDLE:
                    raise ValueError("ما بتقدر تغيّر الزوج وفي صفقة/فحص شغّال.")
                try:
                    self._ensure_market(sym)
                except SoftError as exc:
                    raise ValueError(str(exc))
                except Exception as exc:
                    raise ValueError(f"تعذر التحقق من الزوج: {exc}")
                cfg["symbol"] = sym

            if self._markets_loaded and cfg["symbol"] in self.exchange.markets:
                min_cost = ((self.exchange.markets[cfg["symbol"]].get("limits") or {}).get("cost") or {}).get("min")
                if min_cost and cfg["order_size_usdt"] < float(min_cost):
                    raise ValueError(f"أقل مبلغ شراء مسموح لهذا الزوج هو {min_cost}.")

            symbol_changed = cfg["symbol"] != s["settings"]["symbol"]

            def _w(st: Dict[str, Any]) -> None:
                st["settings"] = cfg
                if symbol_changed:
                    cs = st["cycle_state"]
                    cs.update(reference_price=None, buy_trigger_price=None, live_price=None, lowest_dip_tracked=None)
                    st["wallet"] = {"base": None, "quote": None, "base_free": None, "quote_free": None, "synced_at": None}
                self._refresh_derived(st)
            self.store.update(_w)
            if symbol_changed:
                self.store.write_self_observation("تغيير الزوج", f"صار الزوج {cfg['symbol']}.")
            return cfg

    @staticmethod
    def _refresh_derived(st: Dict[str, Any]) -> None:
        cfg, cs = st["settings"], st["cycle_state"]
        if cs["stage"] in (IDLE, DIP) and cs.get("reference_price"):
            cs["buy_trigger_price"] = cs["reference_price"] * (1 - cfg["drop_buy_pct"] / 100)
        if cs["stage"] == POS and cs.get("actual_entry_price"):
            e = cs["actual_entry_price"]
            cs["target_sell_price"] = e * (1 + cfg["rise_sell_pct"] / 100)
            cs["stop_price"] = e * (1 - cfg["stop_loss_pct"] / 100) if cfg["stop_loss_pct"] > 0 else None

    # ------------------------------------------------------------------ الحلقة
    def tick(self) -> None:
        if not self.tick_lock.acquire(blocking=False):
            return
        try:
            self._tick()
        finally:
            self.tick_lock.release()

    def _tick(self) -> None:
        try:
            m = self.fetch_live_metrics()
        except Exception as exc:
            def _w(s: Dict[str, Any]) -> None:
                s["bot"]["price_ok"] = False
                s["bot"]["last_error"] = f"تعذر جلب السعر: {explain_binance_error(exc, self.mode)}"
            self.store.update(_w)
            return
        price = m["live_price"]

        def _p(s: Dict[str, Any]) -> None:
            s["cycle_state"]["live_price"] = price
            s["bot"]["last_tick"] = _now_iso()
            s["bot"]["price_ok"] = True
        self.store.update(_p)

        if self.connected and time.time() - self._last_wallet_sync >= 30:
            try:
                self.sync_wallet()
            except Exception as exc:
                self._set_error(f"تعذر قراءة المحفظة: {explain_binance_error(exc, self.mode)}")
                return

        s = self.store.read_memory()
        if not s["bot"]["running"]:
            self._set_error(None)
            return
        try:
            self._run_strategy(price, m)
            self._set_error(None)
        except SoftError as exc:
            self._set_error(str(exc))
        except OrderError as exc:
            self._halt(str(exc))
        except Exception as exc:
            self._set_error(f"خطأ غير متوقع: {str(exc)[:160]}")

    def _is_running(self) -> bool:
        return bool(self.store.read_memory()["bot"]["running"])

    # ------------------------------------------------------------------ الاستراتيجية
    def _run_strategy(self, price: float, m: Dict[str, Any]) -> None:
        s = self.store.read_memory()
        cfg, cs = s["settings"], s["cycle_state"]
        drop = cfg["drop_buy_pct"] / 100

        if cs["stage"] == IDLE:
            ref = cs.get("reference_price")
            if not ref or price > ref:  # المرجع يتبع أعلى سعر حتى لا ينتظر البوت بلا فائدة
                ref = price
            trigger = ref * (1 - drop)

            def _w(st: Dict[str, Any]) -> None:
                st["cycle_state"].update(reference_price=ref, buy_trigger_price=trigger)
            self.store.update(_w)
            if price <= trigger:
                def _d(st: Dict[str, Any]) -> None:
                    st["cycle_state"].update(stage=DIP, lowest_dip_tracked=price)
                self.store.update(_d)
                self.store.write_self_observation(
                    "بداية نزول", f"السعر {price:.4f} نزل {cfg['drop_buy_pct']}% عن المرجع {ref:.4f}. أراقب القاع قبل الشراء.")

        elif cs["stage"] == DIP:
            lowest = min(cs.get("lowest_dip_tracked") or price, price)
            trigger = cs["reference_price"] * (1 - drop)
            if price > trigger * 1.01:  # ارتد فوق نقطة الشراء بلا شراء
                def _back(st: Dict[str, Any]) -> None:
                    st["cycle_state"].update(stage=IDLE, lowest_dip_tracked=None, buy_trigger_price=trigger)
                self.store.update(_back)
                self.store.write_self_observation("إلغاء فحص", "السعر ارتد فوق نقطة الشراء، رجعت للمراقبة.")
                return

            def _low(st: Dict[str, Any]) -> None:
                st["cycle_state"].update(lowest_dip_tracked=lowest, buy_trigger_price=trigger)
            self.store.update(_low)

            bounced = price >= lowest * 1.001
            calm = m["last_3_candles_change_pct"] > -0.6
            if not (price <= trigger and bounced and (m["is_reversing_up"] or calm)):
                return  # ما زال ينزل: صبر

            wallet = self.sync_wallet()
            if (wallet["quote_free"] or 0) < cfg["order_size_usdt"]:
                raise SoftError(
                    f"رصيد {wallet['quote']} غير كافٍ: عندك {wallet['quote_free']:.2f} والمطلوب {cfg['order_size_usdt']:.2f}.")
            if not self._ai_says_buy(m, lowest):
                return
            if not self._is_running():
                return
            self._buy(price)

        elif cs["stage"] == POS:
            self.store.update(self._refresh_derived)
            cs = self.store.read_memory()["cycle_state"]
            if price >= cs["target_sell_price"]:
                self._sell(price, "target")
            elif cs.get("stop_price") and price <= cs["stop_price"]:
                self._sell(price, "stop_loss")

    def _ai_says_buy(self, m: Dict[str, Any], lowest: float) -> bool:
        if not self.assistant or not getattr(self.assistant, "enabled", False):
            return True  # بدون ذكاء اصطناعي نعتمد الشروط الفنية أعلاه
        if time.time() - self._last_ai_ts < self.ai_interval:
            return False
        self._last_ai_ts = time.time()
        try:
            dec = self.assistant.check_if_dip_stabilized(m, lowest)
        except Exception:
            return True  # تعذر الذكاء الاصطناعي: نعتمد الشروط الفنية
        dec["ts"] = _now_iso()
        self.store.update(lambda st: st["bot"].__setitem__("last_ai_decision", dec))
        return dec["action"] == "BUY_NOW_REVERSING"

    # ------------------------------------------------------------------ الأوامر الحقيقية
    @staticmethod
    def _fees(order: Dict[str, Any], currency: str) -> Tuple[float, bool]:
        fees = order.get("fees") or ([order["fee"]] if order.get("fee") else [])
        total, found = 0.0, False
        for f in fees:
            if f and f.get("cost") is not None:
                found = True
                if f.get("currency") == currency:
                    total += float(f["cost"])
        return total, found

    def _confirm(self, order: Dict[str, Any], symbol: str) -> Dict[str, Any]:
        oid = order.get("id")
        for _ in range(15):
            status = order.get("status")
            if status == "closed":
                return order
            if status in ("canceled", "expired", "rejected"):
                raise OrderError(f"الأمر {oid} حالته {status}. تحقق من Binance.")
            time.sleep(1)
            order = self.exchange.fetch_order(oid, symbol)
        raise OrderError(f"لم يتأكد اكتمال الأمر {oid}. تحقق من محفظتك على Binance يدوياً قبل التشغيل.")

    def _buy(self, price: float) -> None:
        s = self.store.read_memory()
        cfg, sym = s["settings"], s["settings"]["symbol"]
        market = self._ensure_market(sym)
        base, quote = market["base"], market["quote"]
        cost = cfg["order_size_usdt"]
        min_cost = ((market.get("limits") or {}).get("cost") or {}).get("min")
        if min_cost and cost < float(min_cost):
            raise SoftError(f"مبلغ الشراء أقل من الحد الأدنى للزوج ({min_cost}).")
        try:
            try:
                order = self.exchange.create_market_buy_order_with_cost(sym, cost)
            except (AttributeError, ccxt.NotSupported):
                amount = float(self.exchange.amount_to_precision(sym, cost / price))
                order = self.exchange.create_market_buy_order(sym, amount)
            order = self._confirm(order, sym)
        except OrderError:
            raise
        except Exception as exc:
            raise OrderError(f"فشل أمر الشراء، تحقق من محفظتك: {str(exc)[:200]}") from exc

        filled = float(order.get("filled") or 0)
        if filled <= 0:
            raise OrderError("أمر الشراء رجع بكمية صفر. تحقق من Binance.")
        paid = float(order.get("cost") or 0) or float(order.get("average") or price) * filled
        avg = float(order.get("average") or paid / filled)
        fee_base, found = self._fees(order, base)
        fee_quote, _ = self._fees(order, quote)
        if not found:
            fee_base = filled * FALLBACK_FEE
        net_amount = filled - fee_base
        position_cost = paid + fee_quote

        def _w(st: Dict[str, Any]) -> None:
            st["cycle_state"].update(
                stage=POS, actual_entry_price=avg, position_amount=net_amount, position_cost=position_cost,
                lowest_dip_tracked=None)
            self._refresh_derived(st)
            st["trades_history"].insert(0, {
                "id": order.get("id"), "time": _now_iso(), "side": "BUY", "price": avg, "amount": net_amount,
                "cost": position_cost, "profit": None, "reason": "dip", "mode": self.mode})
            st["trades_history"] = st["trades_history"][:100]
        self.store.update(_w)
        self.store.write_self_observation("شراء", f"اشتريت {net_amount:.4f} {base} بمتوسط {avg:.4f}.")
        try:
            self.sync_wallet()
        except Exception:
            pass

    def _sell(self, price: float, reason: str) -> None:
        s = self.store.read_memory()
        sym, cs = s["settings"]["symbol"], s["cycle_state"]
        market = self._ensure_market(sym)
        base, quote = market["base"], market["quote"]
        try:
            wallet = self.sync_wallet()
            amount = min(float(cs["position_amount"]), float(wallet["base_free"] or 0))
            amount = float(self.exchange.amount_to_precision(sym, amount))
            min_amt = ((market.get("limits") or {}).get("amount") or {}).get("min")
            if amount <= 0 or (min_amt and amount < float(min_amt)):
                raise OrderError(f"كمية {base} المتاحة للبيع ({amount}) صغيرة أو صفر. تحقق من محفظتك.")
            order = self.exchange.create_market_sell_order(sym, amount)
            order = self._confirm(order, sym)
        except OrderError:
            raise
        except Exception as exc:
            raise OrderError(f"فشل أمر البيع، تحقق من محفظتك: {str(exc)[:200]}") from exc

        filled = float(order.get("filled") or amount)
        got = float(order.get("cost") or 0) or float(order.get("average") or price) * filled
        avg = float(order.get("average") or got / filled)
        fee_quote, _ = self._fees(order, quote)
        profit = (got - fee_quote) - float(cs["position_cost"])

        def _w(st: Dict[str, Any]) -> None:
            st["cycle_state"].update(
                stage=IDLE, reference_price=avg, buy_trigger_price=avg * (1 - st["settings"]["drop_buy_pct"] / 100),
                actual_entry_price=None, target_sell_price=None, stop_price=None,
                position_amount=0.0, position_cost=0.0, lowest_dip_tracked=None)
            st["stats"]["completed_cycles"] += 1
            st["stats"]["net_realized_profit_usdt"] = round(st["stats"]["net_realized_profit_usdt"] + profit, 6)
            st["trades_history"].insert(0, {
                "id": order.get("id"), "time": _now_iso(), "side": "SELL", "price": avg, "amount": filled,
                "cost": got - fee_quote, "profit": profit, "reason": reason, "mode": self.mode})
            st["trades_history"] = st["trades_history"][:100]
        self.store.update(_w)
        label = "بيع بالهدف" if reason == "target" else "بيع بوقف الخسارة"
        self.store.write_self_observation(label, f"بعت {filled:.4f} {base} بمتوسط {avg:.4f}، النتيجة {profit:+.4f} {quote}.")
        try:
            self.sync_wallet()  # قفل الدورة: لا دورة جديدة قبل تحديث الرصيد
        except Exception:
            pass

    # ------------------------------------------------------------------ لقطة للواجهة
    def snapshot(self) -> Dict[str, Any]:
        s = self.store.read_memory()
        cs, w = s["cycle_state"], s["wallet"]
        price = cs.get("live_price")
        sym = s["settings"]["symbol"]
        base, quote = (sym.split("/") + ["", ""])[:2]
        equity = None
        if price is not None and w.get("quote_free") is not None and w.get("base_free") is not None:
            equity = w["quote_free"] + w["base_free"] * price
        unreal = None
        if cs["stage"] == POS and price is not None:
            unreal = price * cs["position_amount"] - cs["position_cost"]
        s["derived"] = {"equity": equity, "unrealized_pnl": unreal}
        s["meta"] = {
            "mode": self.mode, "connected": self.connected, "symbol": sym, "base": base, "quote": quote,
            "auth_required": bool(os.getenv("APP_PASSWORD")), "ai_enabled": bool(getattr(self.assistant, "enabled", False)),
            "poll_seconds": self.poll_seconds, "server_time": _now_iso(),
        }
        s["trades_history"] = s["trades_history"][:50]
        s.pop("chat_history", None)
        return s
