
import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine

from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
SUPPORT_URL = os.getenv("SUPPORT_URL", "")
WEB_APP_URL = os.getenv("WEB_APP_URL", "")

engine = None
telegram_app = None

MENUS = {
    "en": [
        ["🚀 Start"],
        ["🎱 Play Now", "💰 Check Balance"],
        ["➕ Make Deposit", "📜 Transaction History"],
        ["🏆 Leaderboard", "🎯 Win Patterns"],
        ["👥 Invite Friends", "👤 Change Username"],
        ["🆘 Support", "🌐 English / አማርኛ"],
    ],
    "am": [
        ["🚀 ጀምር"],
        ["🎱 አሁን ይጫወቱ", "💰 ቀሪ ሂሳብ"],
        ["➕ ተቀማጭ ያድርጉ", "📜 የግብይት ታሪክ"],
        ["🏆 የደረጃ ሰንጠረዥ", "🎯 የማሸነፊያ ቅጦች"],
        ["👥 ጓደኞችን ይጋብዙ", "👤 ስም ይቀይሩ"],
        ["🆘 ድጋፍ", "🌐 English / አማርኛ"],
    ],
}

ACTIONS = {
    "🎱 Play Now": "play",
    "🎱 አሁን ይጫወቱ": "play",
    "💰 Check Balance": "balance",
    "💰 ቀሪ ሂሳብ": "balance",
    "➕ Make Deposit": "deposit",
    "➕ ተቀማጭ ያድርጉ": "deposit",
    "📜 Transaction History": "history",
    "📜 የግብይት ታሪክ": "history",
    "🏆 Leaderboard": "leaderboard",
    "🏆 የደረጃ ሰንጠረዥ": "leaderboard",
    "🎯 Win Patterns": "patterns",
    "🎯 የማሸነፊያ ቅጦች": "patterns",
    "👥 Invite Friends": "invite",
    "👥 ጓደኞችን ይጋብዙ": "invite",
    "👤 Change Username": "username",
    "👤 ስም ይቀይሩ": "username",
    "🆘 Support": "support",
    "🆘 ድጋፍ": "support",
}


async def db_execute(query, params=None, fetch=False):
    if engine is None:
        return [] if fetch else None

    async with engine.begin() as conn:
        result = await conn.execute(text(query), params or {})
        if fetch:
            return [dict(row) for row in result.mappings().all()]
        return result


async def ensure_user(update: Update):
    user = update.effective_user
    if user is None or engine is None:
        return

    await db_execute(
        """
        INSERT INTO bingo_users (telegram_id, username)
        VALUES (:telegram_id, :username)
        ON CONFLICT (telegram_id)
        DO UPDATE SET username = EXCLUDED.username
        """,
        {
            "telegram_id": user.id,
            "username": user.username or user.full_name,
        },
    )

    await db_execute(
        """
        INSERT INTO bingo_wallets (telegram_id, balance)
        VALUES (:telegram_id, 0)
        ON CONFLICT (telegram_id) DO NOTHING
        """,
        {"telegram_id": user.id},
    )


async def get_language(update: Update):
    if engine is None or update.effective_user is None:
        return "en"

    rows = await db_execute(
        "SELECT language FROM bingo_users WHERE telegram_id=:uid",
        {"uid": update.effective_user.id},
        fetch=True,
    )
    if rows and rows[0]["language"] in ("en", "am"):
        return rows[0]["language"]
    return "en"


async def set_language(update: Update, language: str):
    if engine is not None and update.effective_user:
        await db_execute(
            "UPDATE bingo_users SET language=:lang WHERE telegram_id=:uid",
            {"lang": language, "uid": update.effective_user.id},
        )


async def show_menu(update: Update, language="en"):
    if update.effective_message is None:
        return

    await ensure_user(update)

    if language == "am":
        message = (
            "🎱 ወደ ቴሌግራም ቢንጎ እንኳን በደህና መጡ!\n\n"
            "ከታች ካሉት አማራጮች ይምረጡ።"
        )
    else:
        message = (
            "🎱 Welcome to Telegram Bingo!\n\n"
            "Choose an option from the menu below."
        )

    await update.effective_message.reply_text(
        message,
        reply_markup=ReplyKeyboardMarkup(
            MENUS[language],
            resize_keyboard=True,
            is_persistent=True,
        ),
    )


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_user(update)
    language = await get_language(update)

    # Store referral code if supplied: /start ref_123456
    if context.args and context.args[0].startswith("ref_"):
        try:
            referrer_id = int(context.args[0][4:])
            player_id = update.effective_user.id

            if referrer_id != player_id:
                await db_execute(
                    """
                    INSERT INTO bingo_referrals (player_id, referrer_id)
                    VALUES (:player, :referrer)
                    ON CONFLICT (player_id) DO NOTHING
                    """,
                    {"player": player_id, "referrer": referrer_id},
                )
        except (ValueError, TypeError):
            pass

    await show_menu(update, language)


async def menu_handler(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    if update.effective_message is None:
        return

    choice = (update.effective_message.text or "").strip()
    user = update.effective_user

    await ensure_user(update)
    language = await get_language(update)

    if choice in ("🚀 Start", "🚀 ጀምር"):
        await show_menu(update, language)
        return

    if choice == "🌐 English / አማርኛ":
        language = "am" if language == "en" else "en"
        await set_language(update, language)
        await show_menu(update, language)
        return

    action = ACTIONS.get(choice)

    if action is None:
        if language == "am":
            await update.effective_message.reply_text(
                "ከምናሌው ውስጥ አንዱን አማራጭ ይምረጡ።"
            )
        else:
            await update.effective_message.reply_text(
                "Please choose an option from the menu."
            )
        return

    if action == "balance":
        rows = await db_execute(
            """
            SELECT balance FROM bingo_wallets
            WHERE telegram_id=:uid
            """,
            {"uid": user.id},
            fetch=True,
        )
        balance = float(rows[0]["balance"]) if rows else 0.0
        if language == "am":
            message = f"💰 ቀሪ ሂሳብዎ፦ {balance:.2f} ብር"
        else:
            message = f"💰 Your balance: {balance:.2f} Birr"

    elif action == "history":
        rows = await db_execute(
            """
            SELECT transaction_type, amount, status, created_at
            FROM bingo_transactions
            WHERE telegram_id=:uid
            ORDER BY id DESC LIMIT 10
            """,
            {"uid": user.id},
            fetch=True,
        )
        if not rows:
            message = (
                "📜 ገና ምንም ግብይት የለም።"
                if language == "am"
                else "📜 You have no transactions yet."
            )
        else:
            lines = ["📜 የግብይት ታሪክ፦" if language == "am"
                     else "📜 Recent transactions:"]
            for row in rows:
                amount = float(row["amount"])
                lines.append(
                    f"• {row['transaction_type']}: {amount:.2f} Birr "
                    f"({row['status']})"
                )
            message = "\n".join(lines)

    elif action == "deposit":
        message = (
            "➕ የተቀማጭ አማራጮች፦ Telebirr፣ CBE Birr፣ eBirr፣ CBE Bank።\n"
            "የክፍያ ማረጋገጫ እስኪዘጋጅ ድረስ ክፍያ አይላኩ።"
            if language == "am"
            else
            "➕ Payment methods planned: Telebirr, CBE Birr, eBirr, and CBE Bank.\n"
            "Deposits are not enabled yet. Do not send money until payment verification is configured."
        )

    elif action == "play":
        if WEB_APP_URL:
            message = (
                "🎱 Open the Bingo game:\n" + WEB_APP_URL
                if language == "en"
                else "🎱 የቢንጎ ጨዋታውን ይክፈቱ፦\n" + WEB_APP_URL
            )
        else:
            message = (
                "🎱 የጨዋታው ገጽ ገና አልተዘጋጀም።"
                if language == "am"
                else "🎱 The game page is not configured yet."
            )

    elif action == "leaderboard":
        message = (
            "🏆 የደረጃ ሰንጠረዡ የነጥብ ስርዓቱ ሲዘጋጅ ይታያል።"
            if language == "am"
            else "🏆 The leaderboard will be available when scoring is implemented."
        )

    elif action == "patterns":
        message = (
            "🎯 የማሸነፊያ ቅጦች በቅርቡ ይጨመራሉ።"
            if language == "am"
            else "🎯 Winning pattern examples will be added to the game screen."
        )

    elif action == "invite":
        bot_username = context.bot.username
        message = (
            f"👥 Invite friends using your personal link:\n"
            f"https://t.me/{bot_username}?start=ref_{user.id}"
        )

    elif action == "username":
        context.user_data["awaiting_username"] = True
        message = (
            "👤 አዲስ የሚታይ ስምዎን ይላኩ።"
            if language == "am"
            else "👤 Send the display name you want to use."
        )

    elif action == "support":
        message = (
            f"🆘 Support:\n{SUPPORT_URL}"
            if SUPPORT_URL
            else (
                "🆘 የድጋፍ አገናኝ ገና አልተዘጋጀም።"
                if language == "am"
                else "🆘 Support contact has not been configured yet."
            )
        )

    else:
        message = "Please try again."

    await update.effective_message.reply_text(message)


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_message is None:
        return

    if context.user_data.get("awaiting_username"):
        new_name = (update.effective_message.text or "").strip()
        if not new_name or len(new_name) > 100:
            await update.effective_message.reply_text(
                "Please enter a name between 1 and 100 characters."
            )
            return

        await ensure_user(update)
        await db_execute(
            "UPDATE bingo_users SET username=:name WHERE telegram_id=:uid",
            {"name": new_name, "uid": update.effective_user.id},
        )
        context.user_data["awaiting_username"] = False
        await update.effective_message.reply_text(
            f"✅ Display name updated to: {new_name}"
        )
        return

    await menu_handler(update, context)


def configure_telegram_bot():
    bot = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    bot.add_handler(CommandHandler("start", start_command))
    bot.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler)
    )
    return bot


@asynccontextmanager
async def lifespan(app: FastAPI):
    global engine, telegram_app

    if DATABASE_URL:
        url = DATABASE_URL
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql+asyncpg://", 1)
        elif url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)

        engine = create_async_engine(url, pool_pre_ping=True)

        try:
            async with engine.begin() as conn:
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS bingo_users (
                        id BIGSERIAL PRIMARY KEY,
                        telegram_id BIGINT UNIQUE NOT NULL,
                        username VARCHAR(100),
                        language VARCHAR(10) NOT NULL DEFAULT 'en',
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """))
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS bingo_wallets (
                        telegram_id BIGINT PRIMARY KEY REFERENCES bingo_users(telegram_id),
                        balance NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (balance >= 0),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """))
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS bingo_transactions (
                        id BIGSERIAL PRIMARY KEY,
                        telegram_id BIGINT NOT NULL REFERENCES bingo_users(telegram_id),
                        transaction_type VARCHAR(30) NOT NULL,
                        amount NUMERIC(14,2) NOT NULL,
                        status VARCHAR(20) NOT NULL DEFAULT 'pending',
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """))
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS bingo_referrals (
                        id BIGSERIAL PRIMARY KEY,
                        player_id BIGINT UNIQUE NOT NULL REFERENCES bingo_users(telegram_id),
                        referrer_id BIGINT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """))
            logger.info("PostgreSQL connected and tables initialized.")
        except SQLAlchemyError:
            logger.exception("PostgreSQL initialization failed.")
            await engine.dispose()
            engine = None
    else:
        logger.error("DATABASE_URL is missing.")

    if TELEGRAM_BOT_TOKEN:
        try:
            telegram_app = configure_telegram_bot()
            await telegram_app.initialize()
            await telegram_app.start()
            if telegram_app.updater:
                await telegram_app.updater.start_polling()
            logger.info("Telegram bot started.")
        except Exception:
            logger.exception("Telegram bot failed to start.")
            if telegram_app:
                try:
                    await telegram_app.stop()
                    await telegram_app.shutdown()
                except Exception:
                    pass
                telegram_app = None
    else:
        logger.error("TELEGRAM_BOT_TOKEN is missing.")

    yield

    if telegram_app:
        if telegram_app.updater:
            await telegram_app.updater.stop()
        await telegram_app.stop()
        await telegram_app.shutdown()

    if engine:
        await engine.dispose()


app = FastAPI(title="Telegram Bingo", lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def home():
    return """
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width,initial-scale=1">
      <title>Telegram Bingo</title>
    </head>
    <body>
      <h1>🎱 Telegram Bingo</h1>
      <p>Server is running successfully.</p>
      <p><a href="/health">Server health</a></p>
      <p><a href="/health/database">Database health</a></p>
    </body>
    </html>
    """


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/health/database")
async def database_health():
    if engine is None:
        return {"status": "not_connected"}

    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return {"status": "connected"}
    except SQLAlchemyError:
        logger.exception("Database health check failed.")
        return {"status": "error"}
