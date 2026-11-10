"""محرك التداول الذكي والسريع عبر CCXT وBinance Spot (بدون أي اعتماد على الذكاء الاصطناعي في التنفيذ).

المزايا الرئيسية:
1. تنفيذ فوري ومستقل 100% عن الذكاء الاصطناعي: لا يوجد أي انتظار لرد نموذج لغوي عند الشراء أو البيع.
2. فحص فائق السرعة (كل ثانيتين افتراضياً) مع كاش ذكي للشموع لتفادي حظر Binance وتسريع التقاط القاع والقمة.
3. حساب دقيق لسعر التكلفة الفعلي (شامل العمولة المقتطعة) ومطابقة كمية البيع مع فلاتر LOT_SIZE وstepSize في Binance.
4. بيع ذكي مضمون عند الهدف، مع حماية الأرباح التلقائية (Trailing Profit Lock) عند انعكاس السعر بعد اقترابه من الهدف، ودعم البيع والشراء الفوري اليدوي.
5. محلل سوق فني مدمج يحسب المؤشرات الحية (RSI 14, EMA 9/21/50, الدعم والمقاومة، التذبذب ATR، والسيولة) ليغذي شات المتداول المحترف.
"""

from decimal import Decimal, ROUND_DOWN
import math
import os
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import ccxt

IDLE, DIP, POS = "IDLE_WATCHING", "INSPECTING_DIP", "IN_POSITION"
FALLBACK_FEE = 0.001  # 0.1% عمولة Binance Spot الافتراضية عند عدم توفر بيانات العمولة


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
        return (
            f"Binance رفض المفاتيح (-2015). الأسباب المحتملة: (1) المفاتيح مو من نفس وضع البوت الحالي ({mode}) "
            f"— غالباً مفاتيحك من وضع {other}. (2) مفتاح مقيّد بعناوين IP وسيرفر Render مو ضمنها. "
            "(3) صلاحية Enable Spot & Margin Trading مو مفعّلة. (4) في فراغ زيادة بالمفتاح أو السر."
        )
    if "-2010" in msg or "insufficient balance" in msg.lower():
        return "رصيد غير كافٍ في محفظة Binance لتنفيذ الأمر، أو الكمية محتجزة في طلب مفتوح آخر."
    if "notional" in msg.lower() or "-1013" in msg:
        return "قيمة الأمر أقل من الحد الأدنى المسموح في Binance (MIN_NOTIONAL، غالباً 5 إلى 10 USDT). ارفع مبلغ الشراء."
    if "451" in msg or "restricted location" in msg.lower():
        return "Binance بيحجب منطقة سيرفر Render. غيّر Region للسيرفر (مثلاً Frankfurt أو Singapore)."
    return msg[:220]


def _calc_ema(values: List[float], period: int) -> Optional[float]:
    if not values:
        return None
    if len(values) < period:
        return sum(values) / len(values)
    k = 2.0 / (period + 1)
    ema = sum(values[:period]) / period
    for v in values[period:]:
        ema = v * k + ema * (1 - k)
    return ema


def _calc_rsi(closes: List[float], period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return 50.0 if len(closes) >= 2 else None
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(0.0, diff))
        losses.append(max(0.0, -diff))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


class RayTradingEngine:
    def __init__(self, memory_manager: Any, assistant: Any = None) -> None:
        self.store = memory_manager
        self.assistant = assistant
        self.mode = os.getenv("TRADING_MODE", "testnet").lower()
        if self.mode not in ("testnet", "live"):
            self.mode = "testnet"
        # فحص سريع كل ثانيتين افتراضياً لالتقاط السعر الحقيقي فوراً دون تأخير
        self.poll_seconds = max(1, int(float(os.getenv("POLL_SECONDS", "2"))))
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
        self._ohlcv_cache: List[List[float]] = []
        self._ohlcv_cache_ts: float = 0.0
        self._ohlcv_cache_sym: str = ""
        self._recent_ticks: List[float] = []
        self._last_analysis: Dict[str, Any] = {}

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

    def _floor_amount(self, symbol: str, amount: float) -> float:
        """تقريب الكمية للأسفل دائماً (Floor) حسب stepSize ودقة الزوج في Binance لمنع رفض أوامر البيع."""
        if amount <= 0:
            return 0.0
        market = self._ensure_market(symbol)
        # فحص stepSize من معلومات فلتر LOT_SIZE إن وجد
        filters = (market.get("info") or {}).get("filters") or []
        for f in filters:
            if f.get("filterType") == "LOT_SIZE" and f.get("stepSize"):
                step_str = str(f["stepSize"])
                step_dec = Decimal(step_str)
                if step_dec > 0:
                    amt_dec = Decimal(str(amount))
                    floored = float((amt_dec // step_dec) * step_dec)
                    return float(self.exchange.amount_to_precision(symbol, floored))
        prec_str = self.exchange.amount_to_precision(symbol, amount)
        val = float(prec_str)
        if val > amount and amount > 0:
            # في حال قام amount_to_precision بالتقريب للأعلى، ننقص جزءاً ضئيلاً لضمان عدم تجاوز الرصيد الحقيقي
            val = float(self.exchange.amount_to_precision(symbol, amount * 0.999999))
            if val > amount:
                # تقريب عشري صارم للأسفل
                decimals = len(prec_str.split(".")[1]) if "." in prec_str else 0
                factor = 10 ** decimals
                val = math.floor(amount * factor) / factor
        return max(0.0, val)

    def _set_error(self, msg: Optional[str]) -> None:
        self.store.update(lambda s: s["bot"].__setitem__("last_error", msg))

    def _halt(self, msg: str) -> None:
        def _w(s: Dict[str, Any]) -> None:
            s["bot"]["running"] = False
            s["bot"]["halt_reason"] = msg
            s["bot"]["last_error"] = None
        self.store.update(_w)
        self.store.write_self_observation("إيقاف تلقائي", msg)

    # ------------------------------------------------------------------ بيانات السوق والتحليل الفني
    def fetch_live_metrics(self) -> Dict[str, Any]:
        sym = self.symbol
        self._ensure_market(sym)
        ticker = self.exchange.fetch_ticker(sym)
        price = float(ticker["last"])
        bid = float(ticker.get("bid") or price)
        ask = float(ticker.get("ask") or price)

        now = time.time()
        # تحديث الشموع كل 12 ثانية أو عند تغير الزوج، أو فوراً في بيئة الاختبار
        if (not self._ohlcv_cache) or (self._ohlcv_cache_sym != sym) or (now - self._ohlcv_cache_ts >= 12) or (self.poll_seconds >= 5):
            try:
                self._ohlcv_cache = self.exchange.fetch_ohlcv(sym, timeframe="5m", limit=55) or []
                self._ohlcv_cache_ts = now
                self._ohlcv_cache_sym = sym
            except Exception:
                if not self._ohlcv_cache:
                    self._ohlcv_cache = []

        ohlcv = list(self._ohlcv_cache)
        closes = [float(c[4]) for c in ohlcv] or [price]
        closes[-1] = price

        self._recent_ticks.append(price)
        if len(self._recent_ticks) > 30:
            self._recent_ticks = self._recent_ticks[-30:]

        change3 = ((closes[-1] - closes[-3]) / closes[-3] * 100) if len(closes) >= 3 and closes[-3] else 0.0
        prev_tick = self._recent_ticks[-2] if len(self._recent_ticks) >= 2 else (closes[-2] if len(closes) >= 2 else price)
        is_reversing_up = (len(closes) >= 2 and closes[-1] > closes[-2]) or (price > prev_tick)

        # حساب المؤشرات الفنية الحية لشات المتداول المحترف
        rsi_14 = _calc_rsi(closes, 14)
        ema_9 = _calc_ema(closes, 9)
        ema_21 = _calc_ema(closes, 21)
        ema_50 = _calc_ema(closes, 50)
        recent_highs = [float(c[2]) for c in ohlcv[-20:] if len(c) > 3 and float(c[2]) > 0] or [price]
        recent_lows = [float(c[3]) for c in ohlcv[-20:] if len(c) > 3 and float(c[3]) > 0] or [price]
        resistance = max(recent_highs + [price])
        support = min(recent_lows + [price])
        spread_pct = ((ask - bid) / price * 100) if price > 0 else 0.0

        trend = "عرضي متوازن"
        if ema_9 and ema_21:
            if price > ema_9 > ema_21:
                trend = "صاعد قوي (Bullish)"
            elif price < ema_9 < ema_21:
                trend = "هابط ضاغط (Bearish)"
            elif price > ema_21:
                trend = "ميل صاعد لحظي"
            else:
                trend = "ميل هابط لحظي"

        self._last_analysis = {
            "symbol": sym,
            "live_price": price,
            "bid_price": bid,
            "ask_price": ask,
            "spread_pct": round(spread_pct, 4),
            "high_24h": float(ticker.get("high") or price),
            "low_24h": float(ticker.get("low") or price),
            "rsi_14": round(rsi_14, 2) if rsi_14 is not None else None,
            "ema_9": round(ema_9, 6) if ema_9 is not None else None,
            "ema_21": round(ema_21, 6) if ema_21 is not None else None,
            "ema_50": round(ema_50, 6) if ema_50 is not None else None,
            "nearest_support": support,
            "nearest_resistance": resistance,
            "market_trend": trend,
            "last_3_candles_change_pct": round(change3, 3),
            "is_reversing_up": is_reversing_up,
        }

        return {
            "live_price": price,
            "bid_price": bid,
            "ask_price": ask,
            "high_24h": float(ticker.get("high") or price),
            "low_24h": float(ticker.get("low") or price),
            "last_3_candles_change_pct": round(change3, 3),
            "is_reversing_up": is_reversing_up,
            "rsi_14": self._last_analysis["rsi_14"],
            "market_trend": trend,
        }

    def get_market_analysis(self) -> Dict[str, Any]:
        """تزويد شات الذكاء الاصطناعي بأحدث قراءة فنية شاملة للسوق."""
        if not self._last_analysis:
            try:
                self.fetch_live_metrics()
            except Exception:
                pass
        return dict(self._last_analysis)

    def sync_wallet(self) -> Dict[str, Any]:
        """قراءة الرصيد الحقيقي من Binance وحفظه بدقة كاملة."""
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
        self.store.write_self_observation("تشغيل", "تم تشغيل محرك التداول الذكي السريع (بدون تدخل الذكاء الاصطناعي في التنفيذ).")

    def stop(self) -> None:
        self.store.update(lambda s: s["bot"].__setitem__("running", False))
        self.store.write_self_observation("إيقاف", "تم إيقاف البوت من اللوحة.")

    def force_buy(self) -> Dict[str, Any]:
        """شراء فوري بسعر السوق بناءً على طلب المستخدم المباشر."""
        with self.tick_lock:
            if not self.connected:
                raise ValueError("مفاتيح Binance غير مضافة.")
            s = self.store.read_memory()
            if s["cycle_state"]["stage"] == POS:
                raise ValueError("البوت داخل صفقة حالياً. قم ببيع الصفقة الحالية أولاً.")
            m = self.fetch_live_metrics()
            wallet = self.sync_wallet()
            cfg = s["settings"]
            if (wallet["quote_free"] or 0) < cfg["order_size_usdt"]:
                raise ValueError(
                    f"رصيد {wallet['quote']} غير كافٍ: المتاح {wallet['quote_free']:.2f} والمطلوب {cfg['order_size_usdt']:.2f}."
                )
            self._buy(m["ask_price"] or m["live_price"], reason="manual_buy")
            return self.snapshot()

    def force_sell(self) -> Dict[str, Any]:
        """بيع فوري بسعر السوق بناءً على طلب المستخدم المباشر."""
        with self.tick_lock:
            if not self.connected:
                raise ValueError("مفاتيح Binance غير مضافة.")
            m = self.fetch_live_metrics()
            wallet = self.sync_wallet()
            base_free = float(wallet.get("base_free") or 0.0)
            if base_free <= 0:
                raise ValueError(f"لا يوجد رصيد متاح من {wallet.get('base') or 'العملة'} للبيع.")
            s = self.store.read_memory()
            if s["cycle_state"]["stage"] != POS:
                # في حال كان لدى المستخدم رصيد عملة في المحفظة ويريد بيعه فوراً عبر البوت
                sell_price = m["bid_price"] or m["live_price"]
                def _prep(st: Dict[str, Any]) -> None:
                    cs = st["cycle_state"]
                    if not cs.get("position_amount") or cs["position_amount"] <= 0:
                        cs["position_amount"] = base_free
                    if not cs.get("position_cost") or cs["position_cost"] <= 0:
                        entry = cs.get("actual_entry_price") or sell_price
                        cs["position_cost"] = base_free * entry
                self.store.update(_prep)
            self._sell(m["bid_price"] or m["live_price"], reason="manual_sell")
            return self.snapshot()

    def reset_cycle(self) -> Dict[str, Any]:
        """إعادة ضبط السعر المرجعي وحالة الدورة للمراقبة من السعر الحالي."""
        with self.tick_lock:
            price = None
            try:
                m = self.fetch_live_metrics()
                price = m["live_price"]
            except Exception:
                pass

            def _w(st: Dict[str, Any]) -> None:
                cfg = st["settings"]
                cs = st["cycle_state"]
                cs.update(
                    stage=IDLE,
                    reference_price=price,
                    buy_trigger_price=(price * (1 - cfg["drop_buy_pct"] / 100)) if price else None,
                    lowest_dip_tracked=None,
                    highest_peak_tracked=None,
                    raw_entry_price=None,
                    actual_entry_price=None,
                    target_sell_price=None,
                    stop_price=None,
                    position_amount=0.0,
                    position_cost=0.0,
                    dip_ticks=0,
                )
            self.store.update(_w)
            self.store.write_self_observation("إعادة ضبط", "تم تصفير الدورة وتحديث السعر المرجعي للسعر الحالي.")
            return self.snapshot()

    def update_settings(self, patch: Dict[str, Any]) -> Dict[str, Any]:
        with self.tick_lock:
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
                    raise ValueError("صيغة الزوج غلط. مثال: BTC/USDT")
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
                    cs.update(
                        reference_price=None,
                        buy_trigger_price=None,
                        live_price=None,
                        bid_price=None,
                        ask_price=None,
                        lowest_dip_tracked=None,
                        highest_peak_tracked=None,
                        dip_ticks=0,
                    )
                    st["wallet"] = {"base": None, "quote": None, "base_free": None, "quote_free": None, "synced_at": None}
                    self._ohlcv_cache = []
                    self._recent_ticks = []
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
            s["cycle_state"]["bid_price"] = m.get("bid_price", price)
            s["cycle_state"]["ask_price"] = m.get("ask_price", price)
            s["bot"]["last_tick"] = _now_iso()
            s["bot"]["price_ok"] = True
        self.store.update(_p)

        if self.connected and time.time() - self._last_wallet_sync >= 20:
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

    # ------------------------------------------------------------------ الاستراتيجية الذكية السريعة (بدون AI)
    def _run_strategy(self, price: float, m: Dict[str, Any]) -> None:
        s = self.store.read_memory()
        cfg, cs = s["settings"], s["cycle_state"]
        drop = cfg["drop_buy_pct"] / 100

        if cs["stage"] == IDLE:
            ref = cs.get("reference_price")
            if not ref or price > ref:  # المرجع يتبع القمة تلقائياً
                ref = price
            trigger = ref * (1 - drop)

            def _w(st: Dict[str, Any]) -> None:
                st["cycle_state"].update(reference_price=ref, buy_trigger_price=trigger, dip_ticks=0)
            self.store.update(_w)

            if price <= trigger:
                def _d(st: Dict[str, Any]) -> None:
                    st["cycle_state"].update(stage=DIP, lowest_dip_tracked=price, dip_ticks=1)
                self.store.update(_d)
                self.store.write_self_observation(
                    "وصـول لنقطة الشراء",
                    f"السعر {price:.4f} وصل لنقطة الشراء ({trigger:.4f}). ألتقط أدنى قاع لحظي للشراء الفوري."
                )

        elif cs["stage"] == DIP:
            prev_lowest = cs.get("lowest_dip_tracked") or price
            lowest = min(prev_lowest, price)
            trigger = cs["reference_price"] * (1 - drop)
            dip_ticks = int(cs.get("dip_ticks") or 0) + 1

            # إذا ارتد السعر عالياً جداً فوق نقطة الشراء دون تنفيذ، نعود للمراقبة ونحدّث المرجع
            if price > trigger * 1.01:
                def _back(st: Dict[str, Any]) -> None:
                    st["cycle_state"].update(
                        stage=IDLE, lowest_dip_tracked=None, buy_trigger_price=trigger, dip_ticks=0
                    )
                self.store.update(_back)
                self.store.write_self_observation("إلغاء فحص", "السعر ارتد فوق منطقة الشراء، عدت للمراقبة.")
                return

            def _low(st: Dict[str, Any]) -> None:
                st["cycle_state"].update(lowest_dip_tracked=lowest, buy_trigger_price=trigger, dip_ticks=dip_ticks)
            self.store.update(_low)

            # طالما السعر يسجل قاعاً جديداً الآن، ننتظر النبضة التالية لنشتري بأرخص سعر ممكن في القاع
            if price < prev_lowest:
                return

            # بمجرد توقف الهبوط أو بداية أي ارتداد طفيف من القاع (دون انتظار الذكاء الاصطناعي أو انتظار ارتفاع كبير)
            calm = m["last_3_candles_change_pct"] > -0.6
            reversing = bool(m.get("is_reversing_up")) or (price > prev_lowest)
            # نسمح بهامش طفيف جداً فوق trigger (0.15%) حتى لا تضيع الصفقة إذا ارتد السعر لحظة الشراء
            within_buy_zone = price <= trigger * 1.0015

            if not (within_buy_zone and reversing and ( m["is_reversing_up"] or calm )):
                return

            wallet = self.sync_wallet()
            if (wallet["quote_free"] or 0) < cfg["order_size_usdt"]:
                raise SoftError(
                    f"رصيد {wallet['quote']} غير كافٍ: عندك {wallet['quote_free']:.2f} والمطلوب {cfg['order_size_usdt']:.2f}."
                )
            if not self._is_running():
                return
            self._buy(m.get("ask_price") or price, reason="dip")

        elif cs["stage"] == POS:
            self.store.update(self._refresh_derived)
            cs = self.store.read_memory()["cycle_state"]
            entry = float(cs.get("actual_entry_price") or price)
            target = float(cs.get("target_sell_price") or (entry * (1 + cfg["rise_sell_pct"] / 100)))
            stop = cs.get("stop_price")

            # تحديث أعلى قمة وصلها السعر أثناء الصفقة
            prev_peak = float(cs.get("highest_peak_tracked") or entry)
            peak = max(prev_peak, price)
            if peak != cs.get("highest_peak_tracked"):
                self.store.update(lambda st: st["cycle_state"].__setitem__("highest_peak_tracked", peak))

            # 1) البيع الفوري بمجرد الوصول لهدف البيع
            if price >= target:
                self._sell(m.get("bid_price") or price, "target")
                return

            # 2) البيع الذكي لحماية الربح (Trailing Profit Lock):
            # إذا اقترب السعر جداً من الهدف (حقق >= 85% من نسبة الهدف وكان ربحه الصافي فوق العمولة ممتازاً) ثم عكس اتجاهه للهبوط
            target_gain = target - entry
            current_gain = price - entry
            peak_gain = peak - entry
            min_profitable_gain = entry * 0.0025  # +0.25% على الأقل فوق سعر التكلفة الفعلي لضمان ربح صافي بعد عمولة البيع
            if (
                target_gain > 0
                and peak_gain >= max(target_gain * 0.85, min_profitable_gain)
                and current_gain >= entry * 0.0018
                and price <= peak - (peak_gain * 0.30)
            ):
                self._sell(m.get("bid_price") or price, "trailing_profit")
                return

            # 3) البيع الفوري عند وقف الخسارة
            if stop and price <= float(stop):
                self._sell(m.get("bid_price") or price, "stop_loss")

    # ------------------------------------------------------------------ الأوامر الحقيقية الدقيقة
    @staticmethod
    def _fees(order: Dict[str, Any], currency: str) -> Tuple[float, bool]:
        fees = list(order.get("fees") or ([order["fee"]] if order.get("fee") else []))
        # استخراج العمولات من fills الخاصة بـ Binance إذا لم تكن مجمعة في fees
        info_fills = (order.get("info") or {}).get("fills") or []
        if not fees and isinstance(info_fills, list):
            for fl in info_fills:
                if isinstance(fl, dict) and fl.get("commission") is not None:
                    fees.append({
                        "cost": float(fl.get("commission") or 0),
                        "currency": fl.get("commissionAsset"),
                    })
        total, found = 0.0, False
        for f in fees:
            if f and f.get("cost") is not None:
                found = True
                if f.get("currency") == currency:
                    total += float(f["cost"])
        return total, found

    def _confirm(self, order: Dict[str, Any], symbol: str) -> Dict[str, Any]:
        oid = order.get("id")
        initial_fees = order.get("fees")
        initial_fee = order.get("fee")
        initial_info = order.get("info")
        for _ in range(15):
            status = order.get("status")
            if status == "closed":
                if not order.get("fees") and initial_fees:
                    order["fees"] = initial_fees
                if not order.get("fee") and initial_fee:
                    order["fee"] = initial_fee
                if initial_info and isinstance(order.get("info"), dict) and "fills" not in order["info"] and "fills" in initial_info:
                    order["info"]["fills"] = initial_info["fills"]
                return order
            if status in ("canceled", "expired", "rejected"):
                raise OrderError(f"الأمر {oid} حالته {status}. تحقق من Binance.")
            time.sleep(0.5)
            order = self.exchange.fetch_order(oid, symbol)
        raise OrderError(f"لم يتأكد اكتمال الأمر {oid}. تحقق من محفظتك على Binance يدوياً قبل التشغيل.")

    def _buy(self, price: float, reason: str = "dip") -> None:
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
            raise OrderError(f"فشل أمر الشراء، تحقق من محفظتك: {explain_binance_error(exc, self.mode)}") from exc

        filled = float(order.get("filled") or 0)
        if filled <= 0:
            raise OrderError("أمر الشراء رجع بكمية صفر. تحقق من Binance.")
        paid = float(order.get("cost") or 0) or float(order.get("average") or price) * filled
        raw_avg = float(order.get("average") or (paid / filled))
        fee_base, found_base = self._fees(order, base)
        fee_quote, _ = self._fees(order, quote)
        if not found_base:
            fee_base = filled * FALLBACK_FEE
        net_amount = max(0.0, filled - fee_base)
        position_cost = paid + fee_quote
        # سعر التكلفة الحقيقي للوحدة الصافية بعد خصم العمولة (يطابق سعر التكلفة الفعلي في Binance)
        effective_entry_price = (position_cost / net_amount) if net_amount > 0 else raw_avg

        try:
            wallet_after = self.sync_wallet()
            # إذا كان رصيد المحفظة الفعلي يغطي الكمية الإجمالية (بفضل وجود غبار سابق)، نحفظ الكمية القابلة للبيع بدقة
            actual_free = float(wallet_after.get("base_free") or 0.0)
            if actual_free >= filled:
                sellable_candidate = filled
            elif actual_free > 0:
                sellable_candidate = actual_free
            else:
                sellable_candidate = net_amount
        except Exception:
            sellable_candidate = net_amount

        def _w(st: Dict[str, Any]) -> None:
            st["cycle_state"].update(
                stage=POS,
                raw_entry_price=raw_avg,
                actual_entry_price=effective_entry_price,
                position_amount=sellable_candidate,
                gross_amount=filled,
                position_cost=position_cost,
                lowest_dip_tracked=None,
                highest_peak_tracked=effective_entry_price,
                dip_ticks=0,
            )
            self._refresh_derived(st)
            st["trades_history"].insert(0, {
                "id": order.get("id"),
                "time": _now_iso(),
                "side": "BUY",
                "price": effective_entry_price,
                "raw_price": raw_avg,
                "amount": sellable_candidate,
                "cost": position_cost,
                "profit": None,
                "reason": reason,
                "mode": self.mode,
            })
            st["trades_history"] = st["trades_history"][:100]
        self.store.update(_w)
        self.store.write_self_observation(
            "شراء فعلي",
            f"اشتريت {sellable_candidate:.6f} {base} بسعر تنفيذ {raw_avg:.4f} (سعر التكلفة شامل العمولة: {effective_entry_price:.4f})."
        )

    def _sell(self, price: float, reason: str) -> None:
        s = self.store.read_memory()
        sym, cs = s["settings"]["symbol"], s["cycle_state"]
        market = self._ensure_market(sym)
        base, quote = market["base"], market["quote"]
        try:
            wallet = self.sync_wallet()
            free_base = float(wallet.get("base_free") or 0.0)
            gross_amt = float(cs.get("gross_amount") or cs.get("position_amount") or 0.0)
            # إذا كان الرصيد المتاح في المحفظة قريباً من حجم الصفقة (شاملاً أي غبار متراكم)، نستخدمه بالكامل
            # حتى لا يؤدي خصم العمولة (0.0000999) إلى كسر stepSize وبيع كمية أقل (0.00009 بدل 0.00010)
            if gross_amt <= 0 or free_base <= gross_amt * 1.25:
                raw_target_amount = free_base
            else:
                raw_target_amount = min(free_base, gross_amt)

            amount = self._floor_amount(sym, raw_target_amount)
            min_amt = ((market.get("limits") or {}).get("amount") or {}).get("min")
            if amount <= 0 or (min_amt and amount < float(min_amt)):
                raise OrderError(f"كمية {base} المتاحة للبيع ({amount}) أقل من الحد الأدنى للزوج ({min_amt}). تحقق من محفظتك.")
            order = self.exchange.create_market_sell_order(sym, amount)
            order = self._confirm(order, sym)
        except OrderError:
            raise
        except Exception as exc:
            raise OrderError(f"فشل أمر البيع، تحقق من محفظتك: {explain_binance_error(exc, self.mode)}") from exc

        filled = float(order.get("filled") or amount)
        got = float(order.get("cost") or 0) or float(order.get("average") or price) * filled
        avg = float(order.get("average") or (got / filled))
        fee_quote, found_quote = self._fees(order, quote)
        if not found_quote:
            fee_quote = got * FALLBACK_FEE
        net_got = got - fee_quote
        pos_cost = float(cs.get("position_cost") or (filled * float(cs.get("actual_entry_price") or price)))
        profit = net_got - pos_cost

        def _w(st: Dict[str, Any]) -> None:
            st["cycle_state"].update(
                stage=IDLE,
                reference_price=avg,
                buy_trigger_price=avg * (1 - st["settings"]["drop_buy_pct"] / 100),
                raw_entry_price=None,
                actual_entry_price=None,
                target_sell_price=None,
                stop_price=None,
                position_amount=0.0,
                gross_amount=0.0,
                position_cost=0.0,
                lowest_dip_tracked=None,
                highest_peak_tracked=None,
                dip_ticks=0,
            )
            st["stats"]["completed_cycles"] += 1
            st["stats"]["net_realized_profit_usdt"] = round(st["stats"]["net_realized_profit_usdt"] + profit, 6)
            st["trades_history"].insert(0, {
                "id": order.get("id"),
                "time": _now_iso(),
                "side": "SELL",
                "price": avg,
                "amount": filled,
                "cost": net_got,
                "profit": profit,
                "reason": reason,
                "mode": self.mode,
            })
            st["trades_history"] = st["trades_history"][:100]
        self.store.update(_w)
        reasons_map = {
            "target": "بيع بالهدف",
            "trailing_profit": "بيع ذكي لحماية الربح",
            "stop_loss": "بيع بوقف الخسارة",
            "manual_sell": "بيع فوري يدوي",
        }
        label = reasons_map.get(reason, "بيع")
        self.store.write_self_observation(label, f"بعت {filled:.6f} {base} بسعر {avg:.4f}، الصافي {profit:+.4f} {quote}.")
        try:
            self.sync_wallet()  # قفل الدورة: تحديث الرصيد فوراً بعد البيع
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
            sell_amt = float(cs.get("position_amount") or 0.0)
            if w.get("base_free") and float(w["base_free"]) <= sell_amt * 1.25:
                try:
                    sell_amt = self._floor_amount(sym, float(w["base_free"])) or sell_amt
                except Exception:
                    pass
            gross_val = price * sell_amt
            est_sell_fee = gross_val * FALLBACK_FEE
            unreal = (gross_val - est_sell_fee) - float(cs.get("position_cost") or 0.0)
        s["derived"] = {"equity": equity, "unrealized_pnl": unreal}
        s["market_analysis"] = self._last_analysis
        s["meta"] = {
            "mode": self.mode,
            "connected": self.connected,
            "symbol": sym,
            "base": base,
            "quote": quote,
            "auth_required": bool(os.getenv("APP_PASSWORD")),
            "ai_enabled": bool(getattr(self.assistant, "enabled", False)),
            "poll_seconds": self.poll_seconds,
            "server_time": _now_iso(),
        }
        s["trades_history"] = s["trades_history"][:50]
        s.pop("chat_history", None)
        return s
