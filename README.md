# Ryn — بوت تداول Binance (RAY/USDT)

بوت حقيقي: يتصل بمحفظتك على Binance، يشتري بعد نزول X% ويبيع عند +Y%، ومعه شات Ryn (Groq) يشوف بياناتك الحية.

## الإعداد على Render (Environment)
- `EXCHANGE_API_KEY` و `EXCHANGE_API_SECRET` : مفاتيح Binance (للتجربة: مفاتيح testnet.binance.vision)
- `GROQ_API_KEY` : للشات وفحص القاع (النموذج الافتراضي openai/gpt-oss-120b، وتقدر تغيّره بـ `GROQ_MODEL`)
- `APP_PASSWORD` : كلمة سر اللوحة (إلزامية في الوضع الحقيقي)
- `TRADING_MODE` : `testnet` (تجريبي) أو `live` (حقيقي)
- `MEMORY_FILE_PATH` : مثلاً `/data/ryn_memory.json` مع Disk حتى ما تضيع الذاكرة

## ملاحظات
- على Binance لا تفعّل صلاحية السحب للمفتاح، وحدّد IP إن أمكن.
- ضع رابط `/api/health` في UptimeRobot حتى لا ينام السيرفر (البوت يتوقف وقت النوم).
- اختبار المنطق بدون إنترنت: `python tests/test_engine.py`
