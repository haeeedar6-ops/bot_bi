"""اختبار منطق البوت الذكي السريع بدون إنترنت (بورصة وهمية للاختبار)."""
import os, sys, tempfile, types

class NotSupported(Exception): pass
class FakeEx:
    def __init__(self, cfg):
        self.price, self.closes = 1.75, [1.75] * 15
        self.bal = {"USDT": 500.0, "RAY": 0.0, "BTC": 0.00000989}
        self.markets = {
            "RAY/USDT": {
                "base": "RAY", "quote": "USDT",
                "limits": {"cost": {"min": 5}, "amount": {"min": 0.1}},
            },
            "BTC/USDT": {
                "base": "BTC", "quote": "USDT",
                "limits": {"cost": {"min": 5}, "amount": {"min": 0.00001}},
                "info": {"filters": [{"filterType": "LOT_SIZE", "stepSize": "0.00001000"}]},
            },
        }
        self.fail_sell = False
    def set_sandbox_mode(self, v): pass
    def load_markets(self): pass
    def fetch_ticker(self, s): return {"last": self.price, "bid": self.price, "ask": self.price, "high": self.price, "low": self.price}
    def fetch_ohlcv(self, s, timeframe=None, limit=None): return [[0, 0, self.price, self.price, c, 100] for c in self.closes]
    def fetch_balance(self): return {"free": dict(self.bal)}
    def amount_to_precision(self, s, a):
        if s == "BTC/USDT":
            return f"{int(round(a * 100000, 6)) / 100000:.5f}"
        return f"{int(a * 100) / 100:.2f}"
    def create_market_buy_order_with_cost(self, s, cost):
        base = s.split("/")[0]
        if s == "BTC/USDT":
            filled = int((cost / self.price) * 100000) / 100000
            actual_cost = filled * self.price
        else:
            filled = cost / self.price
            actual_cost = cost
        fee = filled * 0.001
        self.bal["USDT"] -= actual_cost
        self.bal[base] = self.bal.get(base, 0.0) + (filled - fee)
        return {"id": "b1", "status": "closed", "filled": filled, "cost": actual_cost, "average": self.price,
                "fees": [{"currency": base, "cost": fee}]}
    def create_market_sell_order(self, s, amount):
        if self.fail_sell: raise RuntimeError("boom")
        base = s.split("/")[0]
        if amount > self.bal.get(base, 0.0) + 1e-12:
            raise RuntimeError(f"Insufficient balance: trying to sell {amount} > {self.bal.get(base)}")
        got = amount * self.price; fee = got * 0.001
        self.bal[base] -= amount; self.bal["USDT"] += got - fee
        return {"id": "s1", "status": "closed", "filled": amount, "cost": got, "average": self.price,
                "fees": [{"currency": "USDT", "cost": fee}]}
    def fetch_order(self, oid, s): return {"id": oid, "status": "closed"}

ccxt = types.ModuleType("ccxt"); ccxt.binance = FakeEx; ccxt.NotSupported = NotSupported
sys.modules["ccxt"] = ccxt
sys.modules["groq"] = types.ModuleType("groq")
os.environ.update(MEMORY_FILE_PATH=tempfile.mktemp(suffix=".json"), EXCHANGE_API_KEY="k", EXCHANGE_API_SECRET="s", TRADING_MODE="testnet")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ryn_brain import RynMemoryManager, RynGroqAssistant
from trading_engine import RayTradingEngine

store = RynMemoryManager(); ai = RynGroqAssistant(store)
eng = RayTradingEngine(store, ai); ex = eng.exchange
def st(): return store.read_memory()
def step(price, closes=None):
    ex.price = price
    ex.closes = closes or ex.closes[1:] + [price]
    eng._ohlcv_cache = []
    eng._last_wallet_sync = 0; eng.tick()

eng.start()
step(1.75);                               assert st()["cycle_state"]["stage"] == "IDLE_WATCHING"
step(1.80);                               assert st()["cycle_state"]["reference_price"] == 1.80   # المرجع يتبع القمة
step(1.75);                               assert st()["cycle_state"]["stage"] == "INSPECTING_DIP"
step(1.70, [1.75] * 13 + [1.72, 1.70]);   assert st()["cycle_state"]["stage"] == "INSPECTING_DIP"
step(1.69, [1.75] * 12 + [1.72, 1.70, 1.69]); assert st()["cycle_state"]["stage"] == "INSPECTING_DIP"  # ما زال ينزل
step(1.695, [1.75] * 12 + [1.72, 1.69, 1.695]); s = st()["cycle_state"]
assert s["stage"] == "IN_POSITION", s
assert abs(ex.bal["USDT"] - 450) < 1e-6 and ex.bal["RAY"] > 0
print("buy ok  entry", round(s["actual_entry_price"], 4), "target", round(s["target_sell_price"], 4))
step(1.70);                               assert st()["cycle_state"]["stage"] == "IN_POSITION"
step(1.74); s = st()
assert s["cycle_state"]["stage"] == "IDLE_WATCHING" and s["stats"]["completed_cycles"] == 1, s["bot"]
assert s["trades_history"][0]["side"] == "SELL" and s["trades_history"][0]["profit"] > 0
print("sell ok profit", round(s["stats"]["net_realized_profit_usdt"], 4), "usdt", round(ex.bal["USDT"], 2))

# تعديل الإعدادات
eng.update_settings({"drop_buy_pct": 3, "rise_sell_pct": 1.5, "order_size_usdt": 20, "stop_loss_pct": 4})
assert st()["settings"]["order_size_usdt"] == 20
for bad in ({"drop_buy_pct": 0}, {"order_size_usdt": 2}, {"symbol": "XXX/USDT"}, {"stop_loss_pct": 200}):
    try: eng.update_settings(bad); raise SystemExit(f"لازم يرفض {bad}")
    except ValueError: pass
print("settings validation ok")

# وقف الخسارة
store.update(lambda s: s["cycle_state"].update(reference_price=2.0)); ex.price = 2.0
step(1.90, [2.0] * 13 + [1.91, 1.90]); step(1.90, [2.0] * 13 + [1.90, 1.905]); step(1.905, [2.0] * 13 + [1.90, 1.905])
assert st()["cycle_state"]["stage"] == "IN_POSITION", st()["cycle_state"]
step(1.80); s = st()
assert s["cycle_state"]["stage"] == "IDLE_WATCHING" and s["trades_history"][0]["reason"] == "stop_loss", s["cycle_state"]
print("stop loss ok profit", round(s["trades_history"][0]["profit"], 4))

# اختبار سيناريو الصور (BTC/USDT مع stepSize=0.00001 وبيع الكمية الكاملة 0.00010 بدون اقتطاع خاطئ إلى 0.00009)
eng.update_settings({"symbol": "BTC/USDT", "drop_buy_pct": 0.3, "rise_sell_pct": 0.3, "order_size_usdt": 8.0, "stop_loss_pct": 0})
ex.price = 74731.01
step(74731.01, [74731.01] * 15)
step(74353.56, [74731.01] * 13 + [74500.0, 74353.56])  # نزول تحت نقطة الشراء 74,506.82
step(74353.56, [74731.01] * 12 + [74500.0, 74350.0, 74353.56])  # استقرار القاع -> شراء فوري بدون انتظار AI!
s_btc = st()["cycle_state"]
assert s_btc["stage"] == "IN_POSITION", s_btc
assert s_btc["actual_entry_price"] > 74353.56  # يشمل العمولة ليطابق سعر التكلفة الفعلي
assert abs(ex.bal["BTC"] - 0.00010979) < 1e-7, ex.bal["BTC"]
step(74660.0, [74353.56] * 14 + [74660.0])  # وصول لهدف البيع -> بيع كامل 0.00010 BTC بنجاح!
s_after = st()
assert s_after["cycle_state"]["stage"] == "IDLE_WATCHING"
assert abs(s_after["trades_history"][0]["amount"] - 0.00010) < 1e-8, s_after["trades_history"][0]
assert s_after["trades_history"][0]["profit"] > 0
print("BTC/USDT screenshot scenario ok! Sold full:", s_after["trades_history"][0]["amount"], "profit:", round(s_after["trades_history"][0]["profit"], 5))

# اختبار المؤشرات الفنية لشات المحلل المحترف
ma = eng.get_market_analysis()
assert "rsi_14" in ma and "ema_9" in ma and "nearest_support" in ma and "nearest_resistance" in ma
print("market analysis indicators ok:", ma["market_trend"], "RSI:", ma["rsi_14"])

# فشل أمر حقيقي يوقف البوت ولا يزوّر نجاحاً
store.update(lambda s: s["cycle_state"].update(stage="IN_POSITION", actual_entry_price=70000.0, position_amount=0.001, position_cost=70.0))
ex.bal["BTC"] = 0.001; ex.fail_sell = True; step(75000.0)
s = st(); assert s["bot"]["running"] is False and s["bot"]["halt_reason"], s["bot"]
print("halt on failed order ok:", s["bot"]["halt_reason"][:40])

# بدون مفاتيح: ما يشتغل
eng.connected = False
try: eng.start(); raise SystemExit("لازم يرفض")
except ValueError as e: print("no-keys ok:", str(e)[:40])
print("ALL TESTS PASSED")
