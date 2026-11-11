"""محرك التداول الذكي والسريع عبر Binance WebSocket المباشر + CCXT (بدون حظر IP وبدون انتظار AI).

الحلول المعمارية الجذرية:
1. بث WebSocket لحظي مباشر (Zero-Weight WebSocket Stream): يستقبل السعر الحي وأفضل عرض/طلب (@bookTicker و@miniTicker)
   مباشرة من `wss://data-stream.binance.vision` بوزن طلبات = صفر (0 Request Weight).
2. تهيئة CCXT القياسية النظيفة لحساب Spot: يتم ترك `ccxt.binance` على إعداداته المعتمدة (`defaultType: spot`) بدون فلاتر تسبب
   `KeyError: spot` في هيكل الحساب الداخلي.
3. قراءة الرصيد المتوافقة (Safe Wallet Sync): قراءة رصيد محفظة Spot بشكل مباشر وآمن، ودعم fallback لطلب `fetch_balance` القياسي.
4. تخفيض استهلاك `fetch_balance` بنسبة 95%: مزامنة المحفظة في الخلفية تتم كل 5 دقائق فقط (أو عند تنفيذ صفقة حقيقية).
5. درع الحماية الذكي من الحظر (IP Ban Shield): إذا واجه السيرفر تهدئة مؤقتة، يوقف طلبات REST الخاصة حتى ينتهي وقت الحظر تماماً.
"""

import base64
from decimal import Decimal
import json
import math
import os
import re
import socket
import ssl
import struct
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import ccxt
try:
    import httpx
except Exception:
    httpx = None  # type: ignore

IDLE, DIP, POS = "IDLE_WATCHING", "INSPECTING_DIP", "IN_POSITION"
FALLBACK_FEE = 0.001  # 0.1% عمولة Binance Spot الافتراضية عند عدم توفر بيانات العمولة


class OrderError(Exception):
    """خطأ في أمر حقيقي: يوقف البوت لحماية المحفظة."""


class SoftError(Exception):
    """خطأ مؤقت: يظهر للمستخدم والبوت يعيد المحاولة."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def extract_ban_until_ts(exc: Exception) -> Optional[float]:
    """استخراج طابع وقت انتهاء الحظر (بالثواني) من رسالة خطأ Binance -1003 / 418 إن وجد."""
    msg = str(exc)
    m = re.search(r"banned until\s+(\d{10,15})", msg, re.IGNORECASE)
    if m:
        val = float(m.group(1))
        if val > 1e11:
            val = val / 1000.0
        return val + 3.0
    if "418" in msg or "-1003" in msg or "429" in msg or "too much request weight" in msg.lower():
        return time.time() + 120.0
    return None


def explain_binance_error(exc: Exception, mode: str) -> str:
    """ترجمة أخطاء Binance الشائعة لسبب وحل بالعربي."""
    msg = str(exc)
    ban_ts = extract_ban_until_ts(exc)
    if ban_ts is not None:
        rem = max(0, int(ban_ts - time.time()))
        mins, secs = divmod(rem, 60)
        return (
            f"حظر مؤقت للـ IP المشترك من Binance (-1003/418) ينتهي تلقائياً بعد {mins} د و{secs} ث. "
            "تم تفعيل درع التهدئة التلقائي والتحويل للبث المباشر (WebSocket) حتى ينفك الحظر دون تمديده."
        )
    if "spot" in msg.lower() and ("keyerror" in msg.lower() or "type" in msg.lower() or msg.strip() == "'spot'"):
        return "جاري تهيئة مزامنة المحفظة الفورية (Spot) مع Binance..."
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


class BinanceWSFeed:
    """عميل WebSocket خفيف ومدمج ببايثون القياسي (بدون مكتبات خارجية) لبث أسعار Binance بوزن = صفر."""

    def __init__(self, mode: str = "live") -> None:
        self.mode = mode
        self.symbol: str = ""
        self.lock = threading.Lock()
        self.data: Dict[str, Any] = {
            "symbol": "",
            "last": None,
            "bid": None,
            "ask": None,
            "high": None,
            "low": None,
            "updated_at": 0.0,
            "connected": False,
        }
        self._stop = False
        self._thread: Optional[threading.Thread] = None

    def start(self, symbol: str) -> None:
        sym_clean = symbol.replace("/", "").lower()
        with self.lock:
            if self.symbol == sym_clean and self._thread and self._thread.is_alive():
                return
            self.symbol = sym_clean
            self.data = {
                "symbol": sym_clean,
                "last": None,
                "bid": None,
                "ask": None,
                "high": None,
                "low": None,
                "updated_at": 0.0,
                "connected": False,
            }
        if not self._thread or not self._thread.is_alive():
            self._stop = False
            self._thread = threading.Thread(target=self._run_loop, daemon=True)
            self._thread.start()

    def get_tick(self, symbol: str) -> Optional[Dict[str, Any]]:
        sym_clean = symbol.replace("/", "").lower()
        with self.lock:
            if self.data.get("symbol") != sym_clean or self.data.get("last") is None:
                return None
            if time.time() - float(self.data.get("updated_at") or 0) > 25:
                return None
            return dict(self.data)

    @property
    def is_connected(self) -> bool:
        with self.lock:
            return bool(self.data.get("connected")) and (time.time() - float(self.data.get("updated_at") or 0) < 30)

    @staticmethod
    def _recv_exact(sock: ssl.SSLSocket, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("WebSocket closed")
            buf.extend(chunk)
        return bytes(buf)

    def _read_frame(self, sock: ssl.SSLSocket) -> Tuple[int, bytes]:
        b1, b2 = self._recv_exact(sock, 2)
        opcode = b1 & 0x0F
        masked = bool(b2 & 0x80)
        length = b2 & 0x7F
        if length == 126:
            length = struct.unpack("!H", self._recv_exact(sock, 2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self._recv_exact(sock, 8))[0]
        mask_key = self._recv_exact(sock, 4) if masked else b""
        payload = self._recv_exact(sock, length) if length > 0 else b""
        if masked:
            payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
        return opcode, payload

    @staticmethod
    def _send_frame(sock: ssl.SSLSocket, opcode: int, payload: bytes = b"") -> None:
        header = bytearray([0x80 | (opcode & 0x0F)])
        mask_key = os.urandom(4)
        length = len(payload)
        if length < 126:
            header.append(0x80 | length)
        elif length < 65536:
            header.append(0x80 | 126)
            header.extend(struct.pack("!H", length))
        else:
            header.append(0x80 | 127)
            header.extend(struct.pack("!Q", length))
        header.extend(mask_key)
        masked_payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
        sock.sendall(bytes(header) + masked_payload)

    def _run_loop(self) -> None:
        hosts = (
            [("testnet.binance.vision", 443), ("data-stream.binance.vision", 443)]
            if self.mode == "testnet"
            else [("data-stream.binance.vision", 443), ("stream.binance.com", 9443), ("stream.binance.com", 443)]
        )
        host_idx = 0
        while not self._stop:
            sym = self.symbol
            if not sym:
                time.sleep(1)
                continue
            host, port = hosts[host_idx % len(hosts)]
            path = f"/stream?streams={sym}@miniTicker/{sym}@bookTicker"
            try:
                ctx = ssl.create_default_context()
                with socket.create_connection((host, port), timeout=10) as raw_sock:
                    with ctx.wrap_socket(raw_sock, server_hostname=host) as ws:
                        ws.settimeout(20)
                        key = base64.b64encode(os.urandom(16)).decode("ascii")
                        req = (
                            f"GET {path} HTTP/1.1\r\n"
                            f"Host: {host}:{port}\r\n"
                            "Upgrade: websocket\r\n"
                            "Connection: Upgrade\r\n"
                            f"Sec-WebSocket-Key: {key}\r\n"
                            "Sec-WebSocket-Version: 13\r\n\r\n"
                        )
                        ws.sendall(req.encode("ascii"))
                        resp = b""
                        while b"\r\n\r\n" not in resp:
                            chunk = ws.recv(1024)
                            if not chunk:
                                raise ConnectionError("Handshake failed")
                            resp += chunk
                        if b" 101 " not in resp.split(b"\r\n")[0]:
                            raise ConnectionError("Upgrade rejected")

                        with self.lock:
                            self.data["connected"] = True

                        while not self._stop and self.symbol == sym:
                            opcode, payload = self._read_frame(ws)
                            if opcode == 0x8:  # Close
                                break
                            if opcode == 0x9:  # Ping -> Pong
                                self._send_frame(ws, 0xA, payload)
                                continue
                            if opcode == 0x1:  # Text JSON
                                msg = json.loads(payload.decode("utf-8", errors="ignore"))
                                d = msg.get("data") or msg
                                with self.lock:
                                    if self.symbol != sym:
                                        break
                                    if "c" in d:  # miniTicker
                                        price = float(d["c"])
                                        self.data["last"] = price
                                        self.data["high"] = float(d.get("h") or price)
                                        self.data["low"] = float(d.get("l") or price)
                                        if self.data["bid"] is None:
                                            self.data["bid"] = price
                                        if self.data["ask"] is None:
                                            self.data["ask"] = price
                                        self.data["updated_at"] = time.time()
                                        self.data["connected"] = True
                                    elif "b" in d and "a" in d:  # bookTicker
                                        bid = float(d["b"])
                                        ask = float(d["a"])
                                        self.data["bid"] = bid
                                        self.data["ask"] = ask
                                        if self.data["last"] is None:
                                            self.data["last"] = (bid + ask) / 2.0
                                        self.data["updated_at"] = time.time()
                                        self.data["connected"] = True
            except Exception:
                with self.lock:
                    self.data["connected"] = False
                host_idx += 1
                time.sleep(2.0)


class RayTradingEngine:
    def __init__(self, memory_manager: Any, assistant: Any = None) -> None:
        self.store = memory_manager
        self.assistant = assistant
        self.mode = os.getenv("TRADING_MODE", "testnet").lower()
        if self.mode not in ("testnet", "live"):
            self.mode = "testnet"
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
        self._is_mock_exchange = type(self.exchange).__name__ != "binance"
        self.ws_feed = BinanceWSFeed(mode=self.mode) if not self._is_mock_exchange else None
        if self.ws_feed:
            self.ws_feed.start(self.symbol)
        self.tick_lock = threading.Lock()
        self._markets_loaded = False
        self._last_wallet_sync = 0.0
        self._ban_until_ts = 0.0
        self._ohlcv_cache: List[List[float]] = []
        self._ohlcv_cache_ts: float = 0.0
        self._ohlcv_cache_sym: str = ""
        self._recent_ticks: List[float] = []
        self._last_analysis: Dict[str, Any] = {}

    # ------------------------------------------------------------------ أدوات ودرع الحماية من الحظر
    @property
    def symbol(self) -> str:
        return self.store.read_memory()["settings"]["symbol"]

    def _check_ban(self, exc: Optional[Exception] = None) -> None:
        if exc is not None:
            ban_ts = extract_ban_until_ts(exc)
            if ban_ts is not None:
                self._ban_until_ts = max(self._ban_until_ts, ban_ts)
                iso_ban = datetime.fromtimestamp(self._ban_until_ts, tz=timezone.utc).isoformat(timespec="seconds")
                self.store.update(lambda s: s["bot"].__setitem__("ban_until", iso_ban))

    def _is_api_banned(self) -> bool:
        if self._ban_until_ts > time.time():
            return True
        if self._ban_until_ts > 0:
            self._ban_until_ts = 0.0
            self.store.update(lambda s: s["bot"].__setitem__("ban_until", None))
        return False

    def _ensure_market(self, symbol: str) -> Dict[str, Any]:
        if not self._markets_loaded:
            try:
                self.exchange.load_markets()
                self._markets_loaded = True
            except Exception as exc:
                self._check_ban(exc)
                if not self._is_api_banned():
                    raise
        if not self.exchange.markets or symbol not in self.exchange.markets:
            where = "حساب التجربة (Testnet)" if self.mode == "testnet" else "Binance"
            raise SoftError(f"الزوج {symbol} غير متوفر على {where}.")
        return self.exchange.markets[symbol]

    def _floor_amount(self, symbol: str, amount: float) -> float:
        """تقريب الكمية للأسفل دائماً (Floor) حسب stepSize ودقة الزوج في Binance لمنع رفض أوامر البيع."""
        if amount <= 0:
            return 0.0
        market = self._ensure_market(symbol)
        filters = (market.get("info") or {}).get("filters") or []
        for f in filters:
            if f.get("filterType") == "LOT_SIZE" and f.get("stepSize"):
                step_str = str(f["stepSize"])
                step_dec = Decimal(step_str)
                if step_dec > 0:
                    amt_dec = Decimal(str(amount))
                    floored = float((amt_dec // step_dec) * step_dec)
                    try:
                        return float(self.exchange.amount_to_precision(symbol, floored))
                    except Exception:
                        return floored
        try:
            prec_str = self.exchange.amount_to_precision(symbol, amount)
            val = float(prec_str)
            if val > amount and amount > 0:
                val = float(self.exchange.amount_to_precision(symbol, amount * 0.999999))
                if val > amount:
                    decimals = len(prec_str.split(".")[1]) if "." in prec_str else 0
                    factor = 10 ** decimals
                    val = math.floor(amount * factor) / factor
            return max(0.0, val)
        except Exception:
            return math.floor(amount * 1e5) / 1e5

    def _set_error(self, msg: Optional[str]) -> None:
        self.store.update(lambda s: s["bot"].__setitem__("last_error", msg))

    def _halt(self, msg: str) -> None:
        def _w(s: Dict[str, Any]) -> None:
            s["bot"]["running"] = False
            s["bot"]["halt_reason"] = msg
            s["bot"]["last_error"] = None
        self.store.update(_w)
        self.store.write_self_observation("إيقاف تلقائي", msg)

    # ------------------------------------------------------------------ جلب السعر والشموع بدون حظر (WebSocket + Vision CDN)
    def _fetch_public_ticker_and_klines(self, sym: str) -> Tuple[Dict[str, float], List[List[float]]]:
        if self._is_mock_exchange:
            ticker = self.exchange.fetch_ticker(sym)
            price = float(ticker["last"])
            t_dict = {
                "last": price,
                "bid": float(ticker.get("bid") or price),
                "ask": float(ticker.get("ask") or price),
                "high": float(ticker.get("high") or price),
                "low": float(ticker.get("low") or price),
            }
            now = time.time()
            if (not self._ohlcv_cache) or (self._ohlcv_cache_sym != sym) or (now - self._ohlcv_cache_ts >= 12):
                try:
                    self._ohlcv_cache = self.exchange.fetch_ohlcv(sym, timeframe="5m", limit=55) or []
                    self._ohlcv_cache_ts = now
                    self._ohlcv_cache_sym = sym
                except Exception:
                    if not self._ohlcv_cache:
                        self._ohlcv_cache = []
            return t_dict, list(self._ohlcv_cache)

        if self.ws_feed:
            self.ws_feed.start(sym)

        raw_sym = sym.replace("/", "").upper()
        ws_tick = self.ws_feed.get_tick(sym) if self.ws_feed else None
        t_dict: Optional[Dict[str, float]] = None

        if ws_tick and ws_tick.get("last") is not None:
            p = float(ws_tick["last"])
            t_dict = {
                "last": p,
                "bid": float(ws_tick.get("bid") or p),
                "ask": float(ws_tick.get("ask") or p),
                "high": float(ws_tick.get("high") or p),
                "low": float(ws_tick.get("low") or p),
            }

        now = time.time()
        need_klines = (not self._ohlcv_cache) or (self._ohlcv_cache_sym != sym) or (now - self._ohlcv_cache_ts >= 45)

        if (t_dict is None or need_klines) and httpx is not None:
            base_url = "https://testnet.binance.vision" if self.mode == "testnet" else "https://data-api.binance.vision"
            with httpx.Client(timeout=6.0) as client:
                if t_dict is None:
                    r_t = client.get(f"{base_url}/api/v3/ticker/bookTicker", params={"symbol": raw_sym})
                    r_t.raise_for_status()
                    bt = r_t.json()
                    bid = float(bt.get("bidPrice") or 0)
                    ask = float(bt.get("askPrice") or bid)
                    mid = (bid + ask) / 2.0 if (bid and ask) else (bid or ask)
                    t_dict = {"last": mid, "bid": bid or mid, "ask": ask or mid, "high": mid, "low": mid}

                if need_klines:
                    try:
                        r_k = client.get(
                            f"{base_url}/api/v3/klines",
                            params={"symbol": raw_sym, "interval": "5m", "limit": 55},
                        )
                        if r_k.status_code == 200:
                            raw_k = r_k.json()
                            self._ohlcv_cache = [
                                [float(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])]
                                for c in raw_k
                            ]
                            self._ohlcv_cache_ts = now
                            self._ohlcv_cache_sym = sym
                    except Exception:
                        pass

        if t_dict is None:
            if self._is_api_banned():
                rem = max(1, int(self._ban_until_ts - time.time()))
                raise SoftError(f"بانتظار اتصال البث المباشر WebSocket (فترة تهدئة IP تنتهي خلال {rem} ثانية)...")
            try:
                ticker = self.exchange.fetch_ticker(sym)
                p = float(ticker["last"])
                t_dict = {
                    "last": p,
                    "bid": float(ticker.get("bid") or p),
                    "ask": float(ticker.get("ask") or p),
                    "high": float(ticker.get("high") or p),
                    "low": float(ticker.get("low") or p),
                }
            except Exception as exc:
                self._check_ban(exc)
                raise

        return t_dict, list(self._ohlcv_cache)

    def fetch_live_metrics(self) -> Dict[str, Any]:
        sym = self.symbol
        self._ensure_market(sym)
        t_dict, ohlcv = self._fetch_public_ticker_and_klines(sym)
        price = float(t_dict["last"])
        bid = float(t_dict.get("bid") or price)
        ask = float(t_dict.get("ask") or price)

        closes = [float(c[4]) for c in ohlcv] or [price]
        closes[-1] = price

        self._recent_ticks.append(price)
        if len(self._recent_ticks) > 30:
            self._recent_ticks = self._recent_ticks[-30:]

        change3 = ((closes[-1] - closes[-3]) / closes[-3] * 100) if len(closes) >= 3 and closes[-3] else 0.0
        prev_tick = self._recent_ticks[-2] if len(self._recent_ticks) >= 2 else (closes[-2] if len(closes) >= 2 else price)
        is_reversing_up = (len(closes) >= 2 and closes[-1] > closes[-2]) or (price > prev_tick)

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
            "high_24h": float(t_dict.get("high") or resistance),
            "low_24h": float(t_dict.get("low") or support),
            "rsi_14": round(rsi_14, 2) if rsi_14 is not None else None,
            "ema_9": round(ema_9, 6) if ema_9 is not None else None,
            "ema_21": round(ema_21, 6) if ema_21 is not None else None,
            "ema_50": round(ema_50, 6) if ema_50 is not None else None,
            "nearest_support": support,
            "nearest_resistance": resistance,
            "market_trend": trend,
            "last_3_candles_change_pct": round(change3, 3),
            "is_reversing_up": is_reversing_up,
            "ws_active": bool(self.ws_feed and self.ws_feed.is_connected),
        }

        return {
            "live_price": price,
            "bid_price": bid,
            "ask_price": ask,
            "high_24h": self._last_analysis["high_24h"],
            "low_24h": self._last_analysis["low_24h"],
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

    def sync_wallet(self, force: bool = False) -> Dict[str, Any]:
        """قراءة الرصيد الحقيقي من Binance بأمان وسلاسة."""
        if not force and self._is_api_banned():
            return self.store.read_memory()["wallet"]
        market = self._ensure_market(self.symbol)
        base, quote = market["base"], market["quote"]
        try:
            try:
                bal = self.exchange.fetch_balance()
            except (KeyError, TypeError):
                # توافق في حال عدم وجود spot key في باراميترات ccxt
                bal = self.exchange.fetch_balance({"type": "spot"})
        except Exception as exc:
            self._check_ban(exc)
            raise

        free = bal.get("free", {}) or {}
        # دعم قراءة الرصيد حتى لو كان قاموس الاستجابة خام من Binance API
        base_free = float(free.get(base) or (bal.get(base, {}).get("free") if isinstance(bal.get(base), dict) else 0.0) or 0.0)
        quote_free = float(free.get(quote) or (bal.get(quote, {}).get("free") if isinstance(bal.get(quote), dict) else 0.0) or 0.0)

        def _w(s: Dict[str, Any]) -> None:
            s["wallet"] = {
                "base": base,
                "quote": quote,
                "base_free": base_free,
                "quote_free": quote_free,
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
            if not self._is_api_banned():
                try:
                    self.sync_wallet()
                except Exception as exc:
                    self._check_ban(exc)
                    if not self._is_api_banned():
                        raise
        except SoftError as exc:
            raise ValueError(str(exc))
        except Exception as exc:
            raise ValueError(f"تعذر الاتصال بـ Binance: {explain_binance_error(exc, self.mode)}")

        def _w(s: Dict[str, Any]) -> None:
            s["bot"]["running"] = True
            s["bot"]["halt_reason"] = None
            s["bot"]["last_error"] = None
        self.store.update(_w)
        self.store.write_self_observation("تشغيل", "تم تشغيل محرك التداول الذكي السريع (بث مباشر WebSocket بدون حظر IP).")

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
            wallet = self.sync_wallet(force=True)
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
            wallet = self.sync_wallet(force=True)
            base_free = float(wallet.get("base_free") or 0.0)
            if base_free <= 0:
                raise ValueError(f"لا يوجد رصيد متاح من {wallet.get('base') or 'العملة'} للبيع.")
            s = self.store.read_memory()
            if s["cycle_state"]["stage"] != POS:
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
                    gross_amount=0.0,
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
                    if self.ws_feed:
                        self.ws_feed.start(cfg["symbol"])
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
            self._check_ban(exc)
            def _w(s: Dict[str, Any]) -> None:
                s["bot"]["price_ok"] = False
                s["bot"]["ws_connected"] = bool(self.ws_feed and self.ws_feed.is_connected)
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
            s["bot"]["ws_connected"] = bool(self.ws_feed and self.ws_feed.is_connected)
        self.store.update(_p)

        wallet_sync_interval = 30 if self._is_mock_exchange else 300
        if self.connected and not self._is_api_banned() and time.time() - self._last_wallet_sync >= wallet_sync_interval:
            try:
                self.sync_wallet()
            except Exception as exc:
                self._check_ban(exc)
                if not self._is_api_banned():
                    self._set_error(f"تعذر قراءة المحفظة: {explain_binance_error(exc, self.mode)}")
                    return

        s = self.store.read_memory()
        if not s["bot"]["running"]:
            if not self._is_api_banned():
                self._set_error(None)
            return
        try:
            self._run_strategy(price, m)
            if self._is_api_banned():
                rem = max(1, int(self._ban_until_ts - time.time()))
                mins, secs = divmod(rem, 60)
                self._set_error(
                    f"⚡ السعر يعمل بالبث المباشر (WebSocket) • تهدئة أوامر الحساب الخاصة تنتهي تلقائياً بعد {mins} د و{secs} ث."
                )
            else:
                self._set_error(None)
        except SoftError as exc:
            self._set_error(str(exc))
        except OrderError as exc:
            self._halt(str(exc))
        except Exception as exc:
            self._check_ban(exc)
            self._set_error(f"خطأ غير متوقع: {explain_binance_error(exc, self.mode)}")

    def _is_running(self) -> bool:
        return bool(self.store.read_memory()["bot"]["running"])

    # ------------------------------------------------------------------ الاستراتيجية الذكية السريعة (بدون AI)
    def _run_strategy(self, price: float, m: Dict[str, Any]) -> None:
        s = self.store.read_memory()
        cfg, cs = s["settings"], s["cycle_state"]
        drop = cfg["drop_buy_pct"] / 100

        if cs["stage"] == IDLE:
            ref = cs.get("reference_price")
            if not ref or price > ref:
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
                    "وصول لنقطة الشراء",
                    f"السعر {price:.4f} وصل لنقطة الشراء ({trigger:.4f}). ألتقط أدنى قاع لحظي للشراء الفوري."
                )

        elif cs["stage"] == DIP:
            prev_lowest = cs.get("lowest_dip_tracked") or price
            lowest = min(prev_lowest, price)
            trigger = cs["reference_price"] * (1 - drop)
            dip_ticks = int(cs.get("dip_ticks") or 0) + 1

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

            if price < prev_lowest:
                return

            calm = m["last_3_candles_change_pct"] > -0.6
            reversing = bool(m.get("is_reversing_up")) or (price > prev_lowest)
            within_buy_zone = price <= trigger * 1.0015

            if not (within_buy_zone and reversing and (m["is_reversing_up"] or calm)):
                return

            if self._is_api_banned():
                return

            wallet = self.sync_wallet(force=True)
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

            prev_peak = float(cs.get("highest_peak_tracked") or entry)
            peak = max(prev_peak, price)
            if peak != cs.get("highest_peak_tracked"):
                self.store.update(lambda st: st["cycle_state"].__setitem__("highest_peak_tracked", peak))

            if self._is_api_banned():
                return

            if price >= target:
                self._sell(m.get("bid_price") or price, "target")
                return

            target_gain = target - entry
            current_gain = price - entry
            peak_gain = peak - entry
            min_profitable_gain = entry * 0.0025
            if (
                target_gain > 0
                and peak_gain >= max(target_gain * 0.85, min_profitable_gain)
                and current_gain >= entry * 0.0018
                and price <= peak - (peak_gain * 0.30)
            ):
                self._sell(m.get("bid_price") or price, "trailing_profit")
                return

            if stop and price <= float(stop):
                self._sell(m.get("bid_price") or price, "stop_loss")

    # ------------------------------------------------------------------ الأوامر الحقيقية الدقيقة
    @staticmethod
    def _fees(order: Dict[str, Any], currency: str) -> Tuple[float, bool]:
        fees = list(order.get("fees") or ([order["fee"]] if order.get("fee") else []))
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
            self._check_ban(exc)
            if self._is_api_banned():
                raise SoftError(explain_binance_error(exc, self.mode)) from exc
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
        effective_entry_price = (position_cost / net_amount) if net_amount > 0 else raw_avg

        try:
            wallet_after = self.sync_wallet(force=True)
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
            wallet = self.sync_wallet(force=True)
            free_base = float(wallet.get("base_free") or 0.0)
            gross_amt = float(cs.get("gross_amount") or cs.get("position_amount") or 0.0)
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
            self._check_ban(exc)
            if self._is_api_banned():
                raise SoftError(explain_binance_error(exc, self.mode)) from exc
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
            self.sync_wallet(force=True)
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
            "ws_connected": bool(self.ws_feed and self.ws_feed.is_connected),
            "poll_seconds": self.poll_seconds,
            "server_time": _now_iso(),
        }
        s["trades_history"] = s["trades_history"][:50]
        s.pop("chat_history", None)
        return s
