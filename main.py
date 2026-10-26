import os
import time
import math
import json
import hmac
import logging
from functools import wraps
from datetime import datetime
from threading import Thread

from flask import Flask, jsonify, request, Response
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
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "")

# التداول حقيقي افتراضياً. للتجربة بدون أوامر حقيقية حط DRY_RUN=true
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"
RECOVER_FROM_BALANCE = os.environ.get("RECOVER_FROM_BALANCE", "false").lower() == "true"

SYMBOL = os.environ.get("SYMBOL", "TAOUSDT")
COIN_ASSET = os.environ.get("COIN_ASSET", "TAO")

BUY_DROP_RATIO = env_float("BUY_DROP_RATIO", 0.99)       # شراء عند هبوط 1% عن أعلى سعر قريب
SELL_GAIN_RATIO = env_float("SELL_GAIN_RATIO", 1.012)    # جني ربح عند +1.2%
STOP_LOSS_RATIO = env_float("STOP_LOSS_RATIO", 0.97)     # وقف خسارة عند -3%
FEE_RATE = env_float("FEE_RATE", 0.001)                  # عمولة Binance لكل جهة (0.1%)

LOOP_SLEEP = 15
WAIT_COOLDOWN = 300
STOP_LOSS_COOLDOWN = 1800
KLINE_LIMIT = 12

STATE_FILE = os.environ.get("STATE_FILE", "bot_state_dry.json" if DRY_RUN else "bot_state.json")

client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    ai_model = genai.GenerativeModel(GEMINI_MODEL)
else:
    ai_model = None
    logger.warning("ما في مفتاح Gemini، البوت رح يعتمد على الفلتر الرياضي بس.")

# إعدادات يتحكم فيها الموقع
control = {
    "running": True,            # تشغيل/إيقاف الشراء الجديد
    "sell_now": False,          # طلب بيع فوري
    "mode": os.environ.get("TRADE_MODE", "usdt"),            # usdt = مبلغ ثابت | percent = نسبة من الرصيد
    "value": env_float("TRADE_VALUE", env_float("USDT_TO_TRADE", 10.0)),
}
runtime = {"cooldown_until": 0.0}

bot_status = {
    "mode": "DRY_RUN" if DRY_RUN else "LIVE",
    "symbol": SYMBOL,
    "coin": COIN_ASSET,
    "current_price": 0.0,
    "reference_price": 0.0,
    "buy_trigger": 0.0,
    "last_check": "",
    "last_event": "",
    "usdt_free": 0.0,
    "coin_free": 0.0,
    "total_value": 0.0,
    "next_trade_usdt": 0.0,
}

# ==========================================
# 2. الحالة (حفظ واسترجاع)
# ==========================================
state = {
    "in_position": False, "buy_price": 0.0, "qty": 0.0,
    "history": [], "total_profit": 0.0, "wins": 0, "losses": 0,
}


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                data = json.load(f)
            state["in_position"] = bool(data.get("in_position", False))
            state["buy_price"] = float(data.get("buy_price", 0.0))
            state["qty"] = float(data.get("qty", 0.0))
            state["history"] = list(data.get("history", []))[-30:]
            state["total_profit"] = float(data.get("total_profit", 0.0))
            state["wins"] = int(data.get("wins", 0))
            state["losses"] = int(data.get("losses", 0))
            if data.get("mode") in ("usdt", "percent"):
                control["mode"] = data["mode"]
                control["value"] = float(data.get("value", control["value"]))
        except Exception as e:
            logger.warning(f"تعذر قراءة ملف الحالة: {e}")


def save_state():
    try:
        data = dict(state)
        data["mode"] = control["mode"]
        data["value"] = control["value"]
        with open(STATE_FILE, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        logger.error(f"خطأ بحفظ الحالة: {e}")


def set_event(text):
    bot_status["last_event"] = f"{datetime.now().strftime('%H:%M:%S')} - {text}"
    logger.info(text)


# ==========================================
# 3. الموقع (لوحة التحكم)
# ==========================================
app = Flask(__name__)

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>لوحة البوت</title>
<style>
*{box-sizing:border-box}
[hidden]{display:none!important}
body{margin:0;font-family:system-ui,-apple-system,"Segoe UI",Tahoma,sans-serif;background:#0f1419;color:#e6edf3;padding:12px;max-width:520px;margin:auto}
h1{font-size:18px;margin:6px 0 12px;display:flex;justify-content:space-between;align-items:center}
.card{background:#1a2230;border-radius:14px;padding:14px;margin-bottom:12px}
.big{font-size:30px;font-weight:700;direction:ltr;text-align:right}
.lbl{font-size:12px;color:#8b98a9;margin-bottom:4px}
.row{display:flex;gap:10px}
.row>div{flex:1}
.val{font-size:18px;font-weight:600;direction:ltr;text-align:right}
.green{color:#3fb950}.red{color:#f85149}.gray{color:#8b98a9}.gold{color:#e3b341}
.badge{display:inline-block;padding:6px 12px;border-radius:20px;font-size:14px;font-weight:600}
.b-pos{background:#12361f;color:#3fb950}
.b-wait{background:#2b2f3a;color:#8b98a9}
.b-stop{background:#3b1d1d;color:#f85149}
.b-cool{background:#3a3115;color:#e3b341}
.mode{font-size:12px;padding:3px 10px;border-radius:12px}
.m-dry{background:#3a3115;color:#e3b341}.m-live{background:#12361f;color:#3fb950}
button{width:100%;padding:13px;border:0;border-radius:10px;font-size:16px;font-weight:600;color:#fff;cursor:pointer}
.btn-start{background:#238636}.btn-stop{background:#b62324}.btn-sell{background:#9e6a03}.btn-save{background:#1f6feb}
button:disabled{opacity:.4}
input,select{width:100%;padding:11px;border-radius:10px;border:1px solid #2d3a4f;background:#0f1419;color:#e6edf3;font-size:16px}
table{width:100%;border-collapse:collapse;font-size:13px}
td,th{padding:6px 4px;text-align:right;border-bottom:1px solid #243044}
th{color:#8b98a9;font-weight:500}
.ltr{direction:ltr;display:inline-block}
.small{font-size:12px;color:#8b98a9;margin-top:6px}
#err{color:#f85149;font-size:14px;margin-top:8px}
</style>
</head>
<body>

<div id="login" class="card" hidden>
  <h1>🔒 دخول</h1>
  <input id="pw" type="password" placeholder="كلمة السر" autocomplete="current-password">
  <div style="height:10px"></div>
  <button class="btn-save" onclick="doLogin()">دخول</button>
  <div id="err"></div>
</div>

<div id="app" hidden>
  <h1><span id="title">🤖 البوت</span><span id="modeBadge" class="mode"></span></h1>

  <div class="card">
    <div class="lbl">السعر اللحظي</div>
    <div class="big" id="price">-</div>
    <div class="small">حالة البوت: <span id="stateBadge" class="badge b-wait">-</span></div>
  </div>

  <div class="card" id="posCard">
    <div class="lbl" id="posTitle">الصفقة الحالية</div>
    <div class="big" id="pnl">-</div>
    <div class="row" style="margin-top:10px">
      <div><div class="lbl">سعر الشراء</div><div class="val" id="buyP">-</div></div>
      <div><div class="lbl">الكمية</div><div class="val" id="qty">-</div></div>
    </div>
    <div class="row" style="margin-top:10px">
      <div><div class="lbl">هدف الربح</div><div class="val green" id="tp">-</div></div>
      <div><div class="lbl">وقف الخسارة</div><div class="val red" id="sl">-</div></div>
    </div>
  </div>

  <div class="card">
    <div class="row">
      <div><div class="lbl">USDT المتوفر</div><div class="val" id="usdt">-</div></div>
      <div><div class="lbl" id="coinLbl">العملة</div><div class="val" id="coin">-</div></div>
    </div>
    <div class="row" style="margin-top:10px">
      <div><div class="lbl">القيمة الكلية</div><div class="val" id="total">-</div></div>
      <div><div class="lbl">الشراء القادم</div><div class="val" id="next">-</div></div>
    </div>
  </div>

  <div class="card">
    <div class="row">
      <div><div class="lbl">إجمالي الربح</div><div class="val" id="tprofit">-</div></div>
      <div><div class="lbl">رابحة / خاسرة</div><div class="val" id="wl">-</div></div>
    </div>
    <div class="small">الأرباح محسوبة بعد خصم عمولة تقديرية</div>
  </div>

  <div class="card">
    <button id="toggleBtn" onclick="toggleRun()">-</button>
    <div style="height:10px"></div>
    <button class="btn-sell" id="sellBtn" onclick="sellNow()">بيع فوري للصفقة</button>
    <div class="small">الإيقاف بيمنع الشراء الجديد بس. الصفقة المفتوحة بتضل مراقبة.</div>
  </div>

  <div class="card">
    <div class="lbl">كم يشتري البوت بكل صفقة</div>
    <div class="row">
      <div><select id="setMode"><option value="usdt">مبلغ ثابت (USDT)</option><option value="percent">نسبة من الرصيد (%)</option></select></div>
      <div><input id="setVal" type="number" step="any" min="1"></div>
    </div>
    <div style="height:10px"></div>
    <button class="btn-save" onclick="saveSettings()">حفظ</button>
    <div class="small" id="setMsg"></div>
  </div>

  <div class="card">
    <div class="lbl">آخر حدث</div>
    <div id="event" style="font-size:14px">-</div>
    <div class="small">آخر فحص: <span id="lastCheck">-</span></div>
  </div>

  <div class="card">
    <div class="lbl">آخر الصفقات</div>
    <table><thead><tr><th>الوقت</th><th>النتيجة</th><th>%</th><th>السبب</th></tr></thead><tbody id="hist"></tbody></table>
  </div>

  <button onclick="logout()" style="background:#2b2f3a">خروج</button>
</div>

<script>
let pw = localStorage.getItem('pw') || '';
let timer = null;
let lastData = null;
let formInit = false;

function $(id){return document.getElementById(id)}
function fmt(n,d){ if(n===null||n===undefined||isNaN(n)) return '-'; return Number(n).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d}) }
function clsPn(n){ return n>0?'green':(n<0?'red':'gray') }

async function api(path, body){
  const opts = {headers:{'X-Auth':pw,'Content-Type':'application/json'}};
  if(body){ opts.method='POST'; opts.body=JSON.stringify(body); }
  const r = await fetch(path, opts);
  const j = await r.json().catch(()=>({}));
  if(r.status===401 || r.status===503){ const e=new Error(j.error||'خطأ'); e.auth=true; throw e; }
  if(!r.ok){ throw new Error(j.error||'خطأ'); }
  return j;
}

async function doLogin(){
  pw = $('pw').value;
  $('err').textContent='';
  try{
    await api('/api/status');
    localStorage.setItem('pw', pw);
    start();
  }catch(e){ $('err').textContent = e.message; }
}

function logout(){
  localStorage.removeItem('pw'); pw='';
  clearInterval(timer);
  $('app').hidden=true; $('login').hidden=false;
}

function start(){
  $('login').hidden=true; $('app').hidden=false;
  refresh();
  clearInterval(timer);
  timer = setInterval(refresh, 5000);
}

async function refresh(){
  try{
    const d = await api('/api/status');
    lastData = d;
    render(d);
  }catch(e){
    if(e.auth){ logout(); $('err').textContent=e.message; }
  }
}

function render(d){
  const s=d.status, st=d.state, c=d.control;
  $('title').textContent = '🤖 ' + s.symbol;
  const mb=$('modeBadge');
  mb.textContent = s.mode==='LIVE' ? 'حقيقي' : 'تجربة';
  mb.className = 'mode ' + (s.mode==='LIVE'?'m-live':'m-dry');

  $('price').textContent = fmt(s.current_price,4)+' $';

  const sb=$('stateBadge');
  const map={in_position:['داخل صفقة (اشترى، بانتظار البيع)','b-pos'],
             stopped:['متوقف','b-stop'],
             cooldown:['استراحة قصيرة','b-cool'],
             waiting:['بانتظار فرصة شراء','b-wait']};
  const m=map[d.state_code]||map.waiting;
  sb.textContent=m[0]; sb.className='badge '+m[1];

  if(st.in_position){
    const pct=(s.current_price/st.buy_price-1)*100;
    $('posTitle').textContent = pct>=0 ? 'الصفقة الحالية (ربح)' : 'الصفقة الحالية (خسارة)';
    const p=$('pnl'); p.textContent=(pct>=0?'+':'')+fmt(pct,2)+' %'; p.className='big '+clsPn(pct);
    $('buyP').textContent=fmt(st.buy_price,4);
    $('qty').textContent=fmt(st.qty,4);
    $('tp').textContent=fmt(d.tp_price,4);
    $('sl').textContent=fmt(d.sl_price,4);
    $('sellBtn').disabled=false;
  }else{
    $('posTitle').textContent='ما في صفقة مفتوحة';
    const p=$('pnl'); p.textContent='-'; p.className='big gray';
    $('buyP').textContent='-'; $('qty').textContent='-'; $('tp').textContent='-'; $('sl').textContent='-';
    $('sellBtn').disabled=true;
  }

  $('usdt').textContent=fmt(s.usdt_free,2)+' $';
  $('coinLbl').textContent=s.coin;
  $('coin').textContent=fmt(s.coin_free,4);
  $('total').textContent=fmt(s.total_value,2)+' $';
  $('next').textContent=fmt(s.next_trade_usdt,2)+' $';

  const tp=st.total_profit;
  const t=$('tprofit'); t.textContent=(tp>=0?'+':'')+fmt(tp,4)+' $'; t.className='val '+clsPn(tp);
  $('wl').textContent=st.wins+' / '+st.losses;

  const tb=$('toggleBtn');
  if(c.running){ tb.textContent='⏸ إيقاف الشراء'; tb.className='btn-stop'; }
  else { tb.textContent='▶ تشغيل البوت'; tb.className='btn-start'; }

  if(!formInit){ $('setMode').value=c.mode; $('setVal').value=c.value; formInit=true; }

  $('event').textContent=s.last_event||'-';
  $('lastCheck').textContent=s.last_check||'-';

  const h=$('hist'); h.innerHTML='';
  (st.history||[]).slice().reverse().slice(0,10).forEach(x=>{
    const tr=document.createElement('tr');
    const cells=[x.time.slice(5,16), (x.pnl>=0?'+':'')+fmt(x.pnl,3)+'$', (x.pct>=0?'+':'')+fmt(x.pct,2)+'%', x.reason];
    cells.forEach((v,i)=>{
      const td=document.createElement('td'); td.textContent=v;
      if(i===1||i===2) td.className='ltr '+clsPn(x.pnl);
      tr.appendChild(td);
    });
    h.appendChild(tr);
  });
}

async function toggleRun(){
  if(!lastData) return;
  const action = lastData.control.running ? 'stop' : 'start';
  try{ await api('/api/control',{action}); refresh(); }catch(e){ alert(e.message); }
}

async function sellNow(){
  if(!confirm('أكيد بدك تبيع الصفقة هلأ؟')) return;
  try{ await api('/api/control',{action:'sell_now'}); $('setMsg').textContent='تم طلب البيع، بينفذ خلال ثواني.'; refresh(); }catch(e){ alert(e.message); }
}

async function saveSettings(){
  try{
    await api('/api/settings',{mode:$('setMode').value, value:parseFloat($('setVal').value)});
    $('setMsg').textContent='انحفظ ✅';
    refresh();
  }catch(e){ $('setMsg').textContent=e.message; }
}

if(pw){ api('/api/status').then(start).catch(()=>{ $('login').hidden=false; }); }
else { $('login').hidden=false; }
</script>
</body>
</html>
"""


def require_auth(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not DASHBOARD_PASSWORD:
            return jsonify({"error": "حط DASHBOARD_PASSWORD بمتغيرات Render أول شي"}), 503
        supplied = request.headers.get("X-Auth", "")
        if not hmac.compare_digest(supplied.encode(), DASHBOARD_PASSWORD.encode()):
            time.sleep(1)
            return jsonify({"error": "كلمة السر غلط"}), 401
        return fn(*args, **kwargs)
    return wrapper


def current_state_code():
    if state["in_position"]:
        return "in_position"
    if not control["running"]:
        return "stopped"
    if time.time() < runtime["cooldown_until"]:
        return "cooldown"
    return "waiting"


@app.route('/')
def home():
    return Response(DASHBOARD_HTML, mimetype="text/html")


@app.route('/health')
def health():
    return "ok"


@app.route('/api/status')
@require_auth
def api_status():
    tp_price = state["buy_price"] * SELL_GAIN_RATIO if state["in_position"] else 0.0
    sl_price = state["buy_price"] * STOP_LOSS_RATIO if state["in_position"] else 0.0
    return jsonify({
        "status": bot_status,
        "state": state,
        "control": control,
        "state_code": current_state_code(),
        "tp_price": tp_price,
        "sl_price": sl_price,
    })


@app.route('/api/control', methods=['POST'])
@require_auth
def api_control():
    action = (request.get_json(silent=True) or {}).get("action")
    if action == "start":
        control["running"] = True
        set_event("تم تشغيل البوت من الموقع")
    elif action == "stop":
        control["running"] = False
        set_event("تم إيقاف الشراء من الموقع")
    elif action == "sell_now":
        if not state["in_position"]:
            return jsonify({"error": "ما في صفقة مفتوحة"}), 400
        control["sell_now"] = True
        set_event("طلب بيع فوري من الموقع")
    else:
        return jsonify({"error": "أمر غير معروف"}), 400
    return jsonify({"ok": True})


@app.route('/api/settings', methods=['POST'])
@require_auth
def api_settings():
    data = request.get_json(silent=True) or {}
    mode = data.get("mode")
    try:
        value = float(data.get("value"))
    except (TypeError, ValueError):
        return jsonify({"error": "القيمة لازم تكون رقم"}), 400
    if mode not in ("usdt", "percent"):
        return jsonify({"error": "النوع غلط"}), 400
    if mode == "percent" and not (1 <= value <= 95):
        return jsonify({"error": "النسبة لازم بين 1 و 95"}), 400
    if mode == "usdt" and not (1 <= value <= 100000):
        return jsonify({"error": "المبلغ لازم يكون 1 USDT على الأقل"}), 400
    control["mode"] = mode
    control["value"] = value
    save_state()
    set_event(f"تغيير حجم الصفقة: {value} {'USDT' if mode == 'usdt' else '% من الرصيد'}")
    return jsonify({"ok": True})


def run_web_server():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)


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


def refresh_balances(price):
    try:
        acc = client.get_account()
        bal = {b['asset']: float(b['free']) for b in acc['balances']}
        bot_status["usdt_free"] = bal.get('USDT', 0.0)
        bot_status["coin_free"] = bal.get(COIN_ASSET, 0.0)
        bot_status["total_value"] = bot_status["usdt_free"] + bot_status["coin_free"] * price
    except Exception as e:
        logger.warning(f"تعذر جلب الرصيد: {e}")


def compute_trade_amount():
    usdt_free = bot_status["usdt_free"]
    if control["mode"] == "percent":
        amount = usdt_free * control["value"] / 100.0
    else:
        amount = control["value"]
    if usdt_free > 0:
        amount = min(amount, usdt_free * 0.995)
    return amount


# ==========================================
# 5. فلاتر الشراء (رياضي + Gemini)
# ==========================================
def math_filter_ok(current_price, avg_24h, recent):
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
        return True


# ==========================================
# 6. تنفيذ الأوامر
# ==========================================
def avg_fill_price_and_qty(order, fallback_price):
    fills = order.get('fills') or []
    if not fills:
        return fallback_price, float(order.get('executedQty', 0.0))
    total_qty = sum(float(f['qty']) for f in fills)
    total_cost = sum(float(f['qty']) * float(f['price']) for f in fills)
    commission_in_coin = sum(float(f['commission']) for f in fills if f.get('commissionAsset') == COIN_ASSET)
    avg_price = total_cost / total_qty if total_qty else fallback_price
    return avg_price, total_qty - commission_in_coin


def do_buy(usdt_amount, current_price, step, min_qty, min_notional):
    qty = format_qty(usdt_amount / current_price, step)
    if qty < min_qty or qty * current_price < min_notional:
        set_event(f"المبلغ {usdt_amount:.2f}$ صغير على الحد الأدنى للزوج. زيد حجم الصفقة.")
        return None

    if DRY_RUN:
        set_event(f"[تجربة] شراء وهمي {qty} {COIN_ASSET} بسعر {current_price}")
        return current_price, qty

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


def record_trade(buy_price, sell_price, qty, reason):
    gross = (sell_price - buy_price) * qty
    fees = FEE_RATE * (buy_price + sell_price) * qty
    pnl = gross - fees
    pct = pnl / (buy_price * qty) * 100 if buy_price * qty else 0.0
    state["history"].append({
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "buy": buy_price, "sell": sell_price, "qty": qty,
        "pnl": round(pnl, 4), "pct": round(pct, 3), "reason": reason,
    })
    state["history"] = state["history"][-30:]
    state["total_profit"] = round(state["total_profit"] + pnl, 4)
    if pnl >= 0:
        state["wins"] += 1
    else:
        state["losses"] += 1
    return pnl, pct


def recover_position(current_price):
    if state["in_position"]:
        if DRY_RUN:
            return
        try:
            free_coin = float(client.get_asset_balance(asset=COIN_ASSET)['free'])
            if free_coin >= state["qty"] * 0.9:
                state["qty"] = min(state["qty"], free_coin)
                logger.info(f"استرجاع صفقة محفوظة بسعر {state['buy_price']}")
                return
            logger.warning("الصفقة المحفوظة ما إلها رصيد بالمحفظة (انباعت يدوياً؟). تصفير.")
            state.update({"in_position": False, "buy_price": 0.0, "qty": 0.0})
            save_state()
        except Exception as e:
            logger.warning(f"تعذر التحقق من الرصيد: {e}")
            return

    if RECOVER_FROM_BALANCE and not DRY_RUN:
        try:
            free_coin = float(client.get_asset_balance(asset=COIN_ASSET)['free'])
            if free_coin * current_price > 5.0:
                for t in reversed(client.get_my_trades(symbol=SYMBOL, limit=10)):
                    if t['isBuyer']:
                        price = float(t['price'])
                        state.update({"in_position": True, "buy_price": price, "qty": free_coin})
                        save_state()
                        logger.info(f"استرجاع صفقة من المحفظة بسعر {price}")
                        return
        except Exception as e:
            logger.warning(f"خطأ أثناء فحص المحفظة: {e}")


# ==========================================
# 7. المحرك الرئيسي
# ==========================================
def trading_bot_loop():
    logger.info(f"تشغيل البوت - الوضع: {'تجربة (DRY_RUN)' if DRY_RUN else 'حقيقي (LIVE)'}")

    while True:
        try:
            step, min_qty, min_notional = get_symbol_rules()
            current_price, _, _ = get_market_data()
            break
        except Exception as e:
            logger.error(f"فشل الاتصال الأولي ({e}). إعادة المحاولة بعد 20 ثانية.")
            time.sleep(20)

    recover_position(current_price)
    logger.info(f"انطلق البوت | داخل صفقة؟ {state['in_position']} | سعر الشراء: {state['buy_price']}")

    while True:
        try:
            current_price, avg_24h, recent = get_market_data()
            reference = max(recent + [current_price])

            refresh_balances(current_price)
            bot_status.update({
                "current_price": current_price,
                "reference_price": reference,
                "buy_trigger": reference * BUY_DROP_RATIO,
                "next_trade_usdt": compute_trade_amount(),
                "last_check": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })

            if not state["in_position"]:
                control["sell_now"] = False
                if control["running"] and time.time() >= runtime["cooldown_until"] \
                        and current_price <= reference * BUY_DROP_RATIO:
                    logger.info(f"السعر {current_price} هبط عن المرجع {reference}. فحص الفلاتر...")

                    if math_filter_ok(current_price, avg_24h, recent) and \
                            gemini_says_buy(current_price, avg_24h, recent):
                        result = do_buy(compute_trade_amount(), current_price, step, min_qty, min_notional)
                        if result:
                            buy_price, qty = result
                            state.update({"in_position": True, "buy_price": buy_price, "qty": qty})
                            save_state()
                        else:
                            runtime["cooldown_until"] = time.time() + WAIT_COOLDOWN
                    else:
                        set_event("تأجيل الشراء (الفلتر قال WAIT).")
                        runtime["cooldown_until"] = time.time() + WAIT_COOLDOWN
            else:
                buy_price, qty = state["buy_price"], state["qty"]
                hit_tp = current_price >= buy_price * SELL_GAIN_RATIO
                hit_sl = current_price <= buy_price * STOP_LOSS_RATIO
                manual = control["sell_now"]

                if hit_tp or hit_sl or manual:
                    control["sell_now"] = False
                    sell_price = do_sell(qty, current_price, step, min_qty)
                    if sell_price is not None:
                        reason = "ربح" if hit_tp else ("وقف خسارة" if hit_sl else "بيع يدوي")
                        pnl, pct = record_trade(buy_price, sell_price, qty, reason)
                        set_event(f"{reason} | النتيجة: {pnl:+.4f} USDT ({pct:+.2f}%)")
                    state.update({"in_position": False, "buy_price": 0.0, "qty": 0.0})
                    save_state()
                    if hit_sl:
                        runtime["cooldown_until"] = time.time() + STOP_LOSS_COOLDOWN

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
load_state()

if __name__ == "__main__":
    t = Thread(target=run_web_server, daemon=True)
    t.start()
    trading_bot_loop()
