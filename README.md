# Cartela Bingo

## Deploy on Render
- Replace `app/main.py`, `app/static/index.html`, `app/__init__.py`, and `requirements.txt` in GitHub with these files.
- Render environment variables: `DATABASE_URL`, `TELEGRAM_BOT_TOKEN`, `WEB_APP_URL=https://YOUR-RENDER-SERVICE.onrender.com/app`, `PAYMENT_INGEST_API_KEY` (long random secret), and optional `SUPPORT_URL`.
- Start command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`.
- Open bot and send `/start`; users must open their own referral deep link and press Start.

## Automatic payment integration
`POST /api/payments/records` expects a normalized transaction record from an authorized provider or bank integration. Header: `X-Payment-Api-Key: <PAYMENT_INGEST_API_KEY>`. Supported provider identifiers: `telebirr`, `cbe_birr`, `ebirr`, `cbe_bank`. The backend matches provider, exact amount and a reference found in the record, then credits once per unique external transaction reference.

This does NOT magically read SMS or bank statements. Each provider needs a legitimate authorized API/export/webhook adapter that submits records. Do not scrape banking apps, collect OTPs, or trust screenshots. Do not enable real-money stakes until the game has been security-reviewed and you have verified applicable Ethiopian legal, licensing and payment-provider requirements.

The game engine uses in-memory live state for one Render instance; this is a working MVP prototype, not a production-certified real-money game. Before scaling, move game state and locking to Redis/PostgreSQL and add atomic joins, reconnect recovery, refunds on cancelled games, payout caps, and automated tests.
