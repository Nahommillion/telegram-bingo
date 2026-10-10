
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

engine = None
telegram_app = None


MENUS = {
    "en": [
        ["🎱 Play Now", "💰 Check Balance"],
        ["➕ Make Deposit", "📜 Transaction History"],
        ["🏆 Leaderboard", "🎯 Win Patterns"],
        ["👥 Invite Friends", "👤 Change Username"],
        ["🆘 Support", "🌐 English / አማርኛ"],
    ],
    "am": [
        ["🎱 አሁን ይጫወቱ", "💰 ቀሪ ሂሳብ"],
        ["➕ ተቀማጭ ያድርጉ", "📜 የግብይት ታሪክ"],
        ["🏆 የደረጃ ሰንጠረዥ", "🎯 የማሸነፊያ ቅጦች"],
        ["👥 ጓደኞችን ይጋብዙ", "👤 ስም ይቀይሩ"],
        ["🆘 ድጋፍ", "🌐 English / አማርኛ"],
    ],
}


async def save_user(update: Update, language: str):
    if engine is None or update.effective_user is None:
        return

    user = update.effective_user

    try:
        async with engine.begin() as connection:
            await connection.execute(
                text("""
                    INSERT INTO bingo_users
                        (telegram_id, username, language)
                    VALUES
                        (:telegram_id, :username, :language)
                    ON CONFLICT (telegram_id)
                    DO UPDATE SET
                        username = EXCLUDED.username,
                        language = EXCLUDED.language
                """),
                {
                    "telegram_id": user.id,
                    "username": user.username or user.full_name,
                    "language": language,
                },
            )
    except SQLAlchemyError:
        logger.exception("Could not save Telegram user")


async def get_language(update: Update) -> str:
    if engine is None or update.effective_user is None:
        return "en"

    try:
        async with engine.connect() as connection:
            result = await connection.execute(
                text("""
                    SELECT language FROM bingo_users
                    WHERE telegram_id = :telegram_id
                """),
                {"telegram_id": update.effective_user.id},
            )
            language = result.scalar_one_or_none()
            return language if language in ("en", "am") else "en"
    except SQLAlchemyError:
        logger.exception("Could not read user language")
        return "en"


async def show_menu(update: Update, language: str, welcome: bool = False):
    if update.effective_message is None:
        return

    await save_user(update, language)

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

    keyboard = ReplyKeyboardMarkup(
        MENUS[language],
        resize_keyboard=True,
        is_persistent=True,
    )

    await update.effective_message.reply_text(
        message,
        reply_markup=keyboard,
    )


async def start_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    await show_menu(update, "en", welcome=True)


async def menu_handler(
    update: Update, context: ContextTypes.DEFAULT_TYPE
):
    if update.effective_message is None:
        return

    choice = update.effective_message.text or ""
    language = await get_language(update)

    if choice == "🌐 English / አማርኛ":
        language = "am" if language == "en" else "en"
        await show_menu(update, language)
        return

    english_actions = {
        "🎱 Play Now": "play",
        "💰 Check Balance": "balance",
        "➕ Make Deposit": "deposit",
        "📜 Transaction History": "history",
        "🏆 Leaderboard": "leaderboard",
        "🎯 Win Patterns": "patterns",
        "👥 Invite Friends": "invite",
        "👤 Change Username": "username",
        "🆘 Support": "support",
    }

    amharic_actions = {
        "🎱 አሁን ይጫወቱ": "play",
        "💰 ቀሪ ሂሳብ": "balance",
        "➕ ተቀማጭ ያድርጉ": "deposit",
        "📜 የግብይት ታሪክ": "history",
        "🏆 የደረጃ ሰንጠረዥ": "leaderboard",
        "🎯 የማሸነፊያ ቅጦች": "patterns",
        "👥 ጓደኞችን ይጋብዙ": "invite",
        "👤 ስም ይቀይሩ": "username",
        "🆘 ድጋፍ": "support",
    }

    action = {**english_actions, **amharic_actions}.get(choice)

    if action is None:
        await show_menu(update, language)
        return

    if action == "play":
        message = (
            "🎱 The Bingo game lobby is coming soon."
            if language == "en"
            else "🎱 የቢንጎ ጨዋታ ሎቢ በቅርቡ ይዘጋጃል።"
        )
    elif action == "balance":
        message = (
            "💰 Your wallet is not configured yet."
            if language == "en"
            else "💰 የኪስ ቦርሳዎ ገና አልተዘጋጀም።"
        )
    elif action == "deposit":
        message = (
            "➕ Deposit options will be added after payment verification is configured."
            if language == "en"
            else "➕ የተቀማጭ አማራጮች የክፍያ ማረጋገጫ ከተዘጋጀ በኋላ ይጨመራሉ።"
        )
    elif action == "history":
        message = (
            "📜 Transaction history will be available when the wallet is implemented."
            if language == "en"
            else "📜 የግብይት ታሪክ የኪስ ቦርሳው ሲዘጋጅ ይገኛል።"
        )
    elif action == "leaderboard":
        message = (
            "🏆 The leaderboard will appear when scoring is implemented."
            if language == "en"
            else "🏆 የደረጃ ሰንጠረዡ የነጥብ ስርዓቱ ሲዘጋጅ ይታያል።"
        )
    elif action == "patterns":
        message = (
            "🎯 Winning patterns will be added soon."
            if language == "en"
            else "🎯 የማሸነፊያ ቅጦች በቅርቡ ይጨመራሉ።"
        )
    elif action == "invite":
        bot_username = context.bot.username
        message = (
            f"👥 Invite friends using this link:\n"
            f"https://t.me/{bot_username}?start=ref_{update.effective_user.id}"
        )
    elif action == "username":
        message = (
            "👤 Username changes will be added soon."
            if language == "en"
            else "👤 የተጠቃሚ ስም መቀየር በቅርቡ ይጨመራል።"
        )
    else:
        message = (
            "🆘 Support details will be added soon."
            if language == "en"
            else "🆘 የድጋፍ መረጃ በቅርቡ ይጨመራል።"
        )

    await update.effective_message.reply_text(message)


def configure_telegram_bot():
    bot = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    bot.add_handler(CommandHandler("start", start_command))
    bot.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, menu_handler)
    )
    return bot


@asynccontextmanager
async def lifespan(app: FastAPI):
    global engine, telegram_app

    if DATABASE_URL:
        database_url = DATABASE_URL

        if database_url.startswith("postgres://"):
            database_url = database_url.replace(
                "postgres://", "postgresql+asyncpg://", 1
            )
        elif database_url.startswith("postgresql://"):
            database_url = database_url.replace(
                "postgresql://", "postgresql+asyncpg://", 1
            )

        engine = create_async_engine(
            database_url,
            pool_pre_ping=True,
        )

        try:
            async with engine.begin() as connection:
                await connection.execute(text("""
                    CREATE TABLE IF NOT EXISTS bingo_users (
                        id BIGSERIAL PRIMARY KEY,
                        telegram_id BIGINT UNIQUE,
                        username VARCHAR(100),
                        language VARCHAR(10) NOT NULL DEFAULT 'en',
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """))
            logger.info("PostgreSQL connected.")
        except SQLAlchemyError:
            logger.exception("PostgreSQL connection failed.")
            await engine.dispose()
            engine = None

    if TELEGRAM_BOT_TOKEN:
        try:
            telegram_app = configure_telegram_bot()
            await telegram_app.initialize()
            await telegram_app.start()

            if telegram_app.updater is not None:
                await telegram_app.updater.start_polling(
                    drop_pending_updates=False
                )

            logger.info("Telegram bot started.")
        except Exception:
            logger.exception("Telegram bot could not start.")
            if telegram_app is not None:
                try:
                    await telegram_app.stop()
                    await telegram_app.shutdown()
                except Exception:
                    pass
                telegram_app = None
    else:
        logger.warning("TELEGRAM_BOT_TOKEN is not configured.")

    yield

    if telegram_app is not None:
        if telegram_app.updater is not None:
            await telegram_app.updater.stop()
        await telegram_app.stop()
        await telegram_app.shutdown()

    if engine is not None:
        await engine.dispose()


app = FastAPI(title="Telegram Bingo", lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def home():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Telegram Bingo</title>
    </head>
    <body>
        <h1>🎱 Telegram Bingo</h1>
        <p>Server is running successfully.</p>
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
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
        return {"status": "connected"}
    except SQLAlchemyError:
        logger.exception("Database health check failed.")
        return {"status": "error"}
