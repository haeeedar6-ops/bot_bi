import os
import time
import math
import json
import logging
from datetime import datetime
from threading import Thread

from flask import Flask, jsonify
from binance.client import Client
from binance.exceptions import BinanceAPIException
import google.generativeai as genai

# ==========================================
# 1. الإعدادات (كلها قابلة للتغيير من متغيرات Render)
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def env_float(name, default):
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return float(default)


BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")

# وضع التجربة: افتراضياً شغّال (ما بينفذ أوامر حقيقية). لتفعيل التداول الحقيقي حط DRY_RUN=false
DRY_RUN = os.environ.get("DRY_RUN", "true").lower() != "false"
# استرجاع صفقة من رصيد المحفظة (خطر: ممكن يبيع عملات كنت مخزنها). افتراضياً مطفي
RECOVER_FROM_BALANCE = os.environ.get("RECOVER_FROM_BALANCE", "false").lower() == "true"

SYMBOL = os.environ.get("SYMBOL", "TAOUSDT")
COIN_ASSET = os.environ.get("COIN_ASSET", "TAO")
USDT_TO_TRADE = env_float("USDT_TO_TRADE", 10.0)

BUY_DROP_RATIO = env_float("BUY_DROP_RATIO", 0.99)       # شراء عند هبوط 1% عن أعلى سعر قريب
SELL_GAIN_RATIO = env_float("SELL_GAIN_RATIO", 1.012)    # جني ربح عند +1.2%
STOP_LOSS_RATIO = env_float("STOP_LOSS_RATIO", 0.97)     # وقف خسارة عند -3%

LOOP_SLEEP = 15               # ثواني بين كل فحص
WAIT_COOLDOWN = 300           # انتظار بعد قرار WAIT (ثواني)
STOP_LOSS_COOLDOWN = 1800     # انتظار بعد وقف الخسارة (ثواني)
KLINE_LIMIT = 12              # عدد شموع 15 دقيقة للمرجع (~3 ساعات)

STATE_FILE = os.environ.get("STATE_FILE", "bot_state_dry.json" if DRY_RUN else "bot_state.json")

client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    ai_model = genai.GenerativeModel(GEMINI_MODEL)
else:
    ai_model = None
    logger.warning("ما في مفتاح Gemini، البوت رح يعتمد على الفلتر الرياضي بس.")

bot_status = {
    "mode": "DRY_RUN" if DRY_RUN else "LIVE",
    "in_position": False,
    "buy_price": 0.0,
    "qty": 0.0,
    "current_price": 0.0,
    "reference_price": 0.0,
    "last_check": "",
    "last_event": "",
    "total_profit_usdt": 0.0,
}

# ==========================================
# 2. خادم الويب (Flask)
# ==========================================
app = Flask(__name__)


@app.route('/')
def home():
    return "Trading Bot is Running!"


@app.route('/health')
def health():
    return "ok"


@app.route('/status')
def status():
    return jsonify(bot_status)


def run_web_server():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)


# ==========================================
# 3. الحالة (حفظ واسترجاع)
# ==========================================
def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                data = json.load(f)
            return {
                "in_position": bool(data.get("in_position", False)),
                "buy_price": float(data.get("buy_price", 0.0)),
                "qty": float(data.get("qty", 0.0)),
            }
        except Exception as e:
            logger.warning(f"تعذر قراءة ملف الحالة: {e}")
    return {"in_position": False, "buy_price": 0.0, "qty": 0.0}


def save_state(in_position, buy_price, qty):
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump({"in_position": in_position, "buy_price": buy_price, "qty": qty}, f)
    except Exception as e:
        logger.error(f"خطأ بحفظ الحالة: {e}")


def set_event(text):
    bot_status["last_event"] = f"{datetime.now().strftime('%H:%M:%S')} - {text}"
    logger.info(text)


# ==========================================
# 4. بيانات السوق وقواعد الزوج
# ==========================================
def get_market_data():
    ticker = client.get_ticker(symbol=SYMBOL)
    current_price = float(ticker['lastPrice'])
    avg_24h_price = float(ticker['weightedAvgPrice'])

    klines = client.get_klines(symbol=SYMBOL, interval=Client.KLINE_INTERVAL_15MINUTE, limit=KLINE_LIMIT)
    recent_closes = [float(k[4]) for k in klines]
    return current_price, avg_24h_price, recent_closes


def get_symbol_rules():
    """بترجع (step_size, min_qty, min_notional) من فلاتر Binance."""
    info = client.get_symbol_info(SYMBOL)
    step, min_qty, min_notional = 0.0001, 0.0, 5.0
    for f in info['filters']:
        if f['filterType'] == 'LOT_SIZE':
            step = float(f['stepSize'])
            min_qty = float(f['minQty'])
        elif f['filterType'] in ('NOTIONAL', 'MIN_NOTIONAL'):
            min_notional = float(f.get('minNotional', min_notional))
    return step, min_qty, min_notional


def format_qty(qty, step):
    precision = max(0, int(round(-math.log10(step))))
    floored = math.floor(qty / step + 1e-9) * step
    return round(floored, precision)


# ==========================================
# 5. فلاتر الشراء (رياضي + Gemini)
# ==========================================
def math_filter_ok(current_price, avg_24h, recent):
    """فلتر بسيط: ما نشتري وقت الانهيار السريع."""
    if current_price < avg_24h * 0.95:
        logger.info("الفلتر الرياضي: السعر أقل من متوسط 24 ساعة بأكثر من 5% (ممنوع).")
        return False
    if len(recent) >= 4:
        last3 = recent[-3:]
        falling = last3[0] > last3[1] > last3[2]
        drop = (recent[-4] - current_price) / recent[-4]
        if falling and drop > 0.02:
            logger.info("الفلتر الرياضي: هبوط متواصل وسريع (ممنوع).")
            return False
    return True


def gemini_says_buy(current_price, avg_24h, recent):
    if not ai_model:
        return True
    prompt = f"""
    أنت خبير مخاطر تداول خوارزمي. نراقب عملة {SYMBOL}.
    السعر الحالي: {current_price}
    متوسط سعر 24 ساعة: {avg_24h}
    إغلاقات آخر شموع (15 دقيقة) من الأقدم للأحدث: {recent}

    السعر نزل 1% وصار مهيأ للشراء. هل هالهبوط طبيعي وآمن للشراء الارتدادي،
    أو في انهيار حاد مستمر يستوجب الانتظار؟
    أجب بكلمة وحدة فقط: BUY أو WAIT
    """
    try:
        response = ai_model.generate_content(prompt)
        text = response.text.strip().upper()
        decision = "BUY" if "BUY" in text and "WAIT" not in text else "WAIT"
        logger.info(f"قرار Gemini: {decision}")
        return decision == "BUY"
    except Exception as e:
        logger.warning(f"تعذر الوصول لـ Gemini ({e}) - اعتمدنا على الفلتر الرياضي.")
        return True  # الفلتر الرياضي مرّ قبلها، فما في خطر إضافي


# ==========================================
# 6. تنفيذ الأوامر
# ==========================================
def avg_fill_price_and_qty(order, fallback_price):
    """متوسط سعر مرجّح + الكمية الصافية بعد خصم العمولة إذا انخصمت من العملة."""
    fills = order.get('fills') or []
    if not fills:
        return fallback_price, float(order.get('executedQty', 0.0))
    total_qty = sum(float(f['qty']) for f in fills)
    total_cost = sum(float(f['qty']) * float(f['price']) for f in fills)
    commission_in_coin = sum(float(f['commission']) for f in fills if f.get('commissionAsset') == COIN_ASSET)
    avg_price = total_cost / total_qty if total_qty else fallback_price
    return avg_price, total_qty - commission_in_coin


def do_buy(current_price, step, min_qty, min_notional):
    qty = format_qty(USDT_TO_TRADE / current_price, step)
    if qty < min_qty or qty * current_price < min_notional:
        set_event(f"الكمية {qty} أقل من الحد الأدنى للزوج. زيد USDT_TO_TRADE.")
        return None

    if DRY_RUN:
        set_event(f"[تجربة] شراء وهمي {qty} {COIN_ASSET} بسعر {current_price}")
        return current_price, qty

    usdt_balance = float(client.get_asset_balance(asset='USDT')['free'])
    if usdt_balance < USDT_TO_TRADE:
        set_event("رصيد USDT ما بكفي لتنفيذ الصفقة.")
        return None

    order = client.order_market_buy(symbol=SYMBOL, quantity=qty)
    buy_price, net_qty = avg_fill_price_and_qty(order, current_price)
    set_event(f"تم الشراء: {net_qty:.6f} {COIN_ASSET} بمتوسط {buy_price:.4f}")
    return buy_price, net_qty


def do_sell(qty, current_price, step, min_qty):
    if DRY_RUN:
        set_event(f"[تجربة] بيع وهمي {qty} {COIN_ASSET} بسعر {current_price}")
        return current_price

    free_coin = float(client.get_asset_balance(asset=COIN_ASSET)['free'])
    sell_qty = format_qty(min(qty, free_coin), step)
    if sell_qty < min_qty or sell_qty <= 0:
        set_event("الكمية المتاحة للبيع صفر (يمكن انباعت يدوياً). تصفير الحالة.")
        return None

    order = client.order_market_sell(symbol=SYMBOL, quantity=sell_qty)
    sell_price, _ = avg_fill_price_and_qty(order, current_price)
    set_event(f"تم البيع: {sell_qty} {COIN_ASSET} بمتوسط {sell_price:.4f}")
    return sell_price


def recover_state(current_price):
    state = load_state()

    if state["in_position"]:
        if DRY_RUN:
            return True, state["buy_price"], state["qty"]
        try:
            free_coin = float(client.get_asset_balance(asset=COIN_ASSET)['free'])
            if free_coin >= state["qty"] * 0.9:
                logger.info(f"استرجاع صفقة محفوظة بسعر {state['buy_price']}")
                return True, state["buy_price"], min(state["qty"], free_coin)
            logger.warning("الصفقة المحفوظة ما إلها رصيد بالمحفظة (انباعت يدوياً؟). تصفير.")
            save_state(False, 0.0, 0.0)
        except Exception as e:
            logger.warning(f"تعذر التحقق من الرصيد: {e}")
            return True, state["buy_price"], state["qty"]

    if RECOVER_FROM_BALANCE and not DRY_RUN:
        try:
            free_coin = float(client.get_asset_balance(asset=COIN_ASSET)['free'])
            if free_coin * current_price > 5.0:
                for t in reversed(client.get_my_trades(symbol=SYMBOL, limit=10)):
                    if t['isBuyer']:
                        price = float(t['price'])
                        save_state(True, price, free_coin)
                        logger.info(f"استرجاع صفقة من المحفظة بسعر {price}")
                        return True, price, free_coin
        except Exception as e:
            logger.warning(f"خطأ أثناء فحص المحفظة: {e}")

    return False, 0.0, 0.0


# ==========================================
# 7. المحرك الرئيسي
# ==========================================
def trading_bot_loop():
    logger.info(f"تشغيل البوت - الوضع: {'تجربة (DRY_RUN)' if DRY_RUN else 'حقيقي (LIVE)'}")

    while True:  # انتظار نجاح الاتصال الأول
        try:
            step, min_qty, min_notional = get_symbol_rules()
            current_price, _, _ = get_market_data()
            break
        except Exception as e:
            logger.error(f"فشل الاتصال الأولي ({e}). إعادة المحاولة بعد 20 ثانية.")
            time.sleep(20)

    in_position, buy_price, qty = recover_state(current_price)
    cooldown_until = 0.0
    logger.info(f"انطلق البوت | داخل صفقة؟ {in_position} | سعر الشراء: {buy_price}")

    while True:
        try:
            current_price, avg_24h, recent = get_market_data()
            reference = max(recent + [current_price])  # أعلى سعر قريب = المرجع المتحرك

            bot_status.update({
                "current_price": current_price,
                "reference_price": reference,
                "in_position": in_position,
                "buy_price": buy_price,
                "qty": qty,
                "last_check": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })

            if not in_position:
                if time.time() >= cooldown_until and current_price <= reference * BUY_DROP_RATIO:
                    logger.info(f"السعر {current_price} هبط عن المرجع {reference}. فحص الفلاتر...")

                    if math_filter_ok(current_price, avg_24h, recent) and \
                            gemini_says_buy(current_price, avg_24h, recent):
                        result = do_buy(current_price, step, min_qty, min_notional)
                        if result:
                            buy_price, qty = result
                            in_position = True
                            save_state(True, buy_price, qty)
                        else:
                            cooldown_until = time.time() + WAIT_COOLDOWN
                    else:
                        set_event("تأجيل الشراء (الفلتر قال WAIT).")
                        cooldown_until = time.time() + WAIT_COOLDOWN
            else:
                hit_tp = current_price >= buy_price * SELL_GAIN_RATIO
                hit_sl = current_price <= buy_price * STOP_LOSS_RATIO

                if hit_tp or hit_sl:
                    sell_price = do_sell(qty, current_price, step, min_qty)
                    if sell_price is not None:
                        pnl = (sell_price - buy_price) * qty
                        bot_status["total_profit_usdt"] = round(bot_status["total_profit_usdt"] + pnl, 4)
                        set_event(("جني ربح" if hit_tp else "وقف خسارة") + f" | النتيجة: {pnl:+.4f} USDT")
                    in_position, buy_price, qty = False, 0.0, 0.0
                    save_state(False, 0.0, 0.0)
                    if hit_sl:
                        cooldown_until = time.time() + STOP_LOSS_COOLDOWN

            time.sleep(LOOP_SLEEP)

        except BinanceAPIException as e:
            if e.status_code in (418, 429) or e.code == -1003:
                logger.error("تجاوز حد الطلبات (Rate Limit). انتظار 90 ثانية.")
                time.sleep(90)
            else:
                logger.error(f"خطأ Binance: {e}")
                time.sleep(20)
        except Exception as e:
            logger.error(f"خطأ بالحلقة الرئيسية: {e}")
            time.sleep(20)


# ==========================================
# 8. التشغيل
# ==========================================
if __name__ == "__main__":
    t = Thread(target=run_web_server, daemon=True)
    t.start()
    trading_bot_loop()
