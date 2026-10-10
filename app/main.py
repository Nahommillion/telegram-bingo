
import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    WebAppInfo,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ==================================================
# CONFIGURATION
# ==================================================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
WEB_APP_URL = os.getenv("WEB_APP_URL", "").strip()
SUPPORT_URL = os.getenv("SUPPORT_URL", "").strip()

engine = None
telegram_app = None


# ==================================================
# BILINGUAL KEYBOARDS
# ==================================================

MENUS = {
    "en": [
        ["🎮 Play Now", "💰 Check Balance"],
        ["➕ Make Deposit", "🆘 Support"],
        ["📖 Instructions", "🏆 Win Patterns"],
        ["✏️ Change Username", "👥 Invite Friends"],
        ["🥇 Leaderboard", "📜 Transaction History"],
        ["🌐 Change Language"],
    ],
    "am": [
        ["🎮 አሁን ይጫወቱ", "💰 ቀሪ ሂሳብ"],
        ["➕ ገንዘብ ያስገቡ", "🆘 ድጋፍ"],
        ["📖 መመሪያ", "🏆 የማሸነፊያ ቅጦች"],
        ["✏️ ስም ይቀይሩ", "👥 ጓደኞችን ይጋብዙ"],
        ["🥇 ደረጃ ሰንጠረዥ", "📜 የግብይት ታሪክ"],
        ["🌐 ቋንቋ ይቀይሩ"],
    ],
}

ACTIONS = {
    # English
    "🎮 Play Now": "play",
    "💰 Check Balance": "balance",
    "➕ Make Deposit": "deposit",
    "🆘 Support": "support",
    "📖 Instructions": "instructions",
    "🏆 Win Patterns": "patterns",
    "✏️ Change Username": "username",
    "👥 Invite Friends": "invite",
    "🥇 Leaderboard": "leaderboard",
    "📜 Transaction History": "history",
    "🌐 Change Language": "language",

    # Amharic
    "🎮 አሁን ይጫወቱ": "play",
    "💰 ቀሪ ሂሳብ": "balance",
    "➕ ገንዘብ ያስገቡ": "deposit",
    "🆘 ድጋፍ": "support",
    "📖 መመሪያ": "instructions",
    "🏆 የማሸነፊያ ቅጦች": "patterns",
    "✏️ ስም ይቀይሩ": "username",
    "👥 ጓደኞችን ይጋብዙ": "invite",
    "🥇 ደረጃ ሰንጠረዥ": "leaderboard",
    "📜 የግብይት ታሪክ": "history",
    "🌐 ቋንቋ ይቀይሩ": "language",
}


# ==================================================
# DATABASE SETUP
# ==================================================

async def setup_database():
    global engine

    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is missing from Render Environment."
        )

    if DATABASE_URL.startswith("postgres://"):
        database_url = DATABASE_URL.replace(
            "postgres://", "postgresql+asyncpg://", 1
        )
    elif DATABASE_URL.startswith("postgresql://"):
        database_url = DATABASE_URL.replace(
            "postgresql://", "postgresql+asyncpg://", 1
        )
    else:
        database_url = DATABASE_URL

    engine = create_async_engine(
        database_url,
        pool_pre_ping=True,
    )

    async with engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bingo_users (
                id BIGSERIAL PRIMARY KEY,
                telegram_id BIGINT UNIQUE NOT NULL,
                username VARCHAR(100),
                display_name VARCHAR(100),
                language VARCHAR(5) NOT NULL DEFAULT 'en',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))

        await conn.execute(text("""
            ALTER TABLE bingo_users
            ADD COLUMN IF NOT EXISTS display_name VARCHAR(100)
        """))

        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bingo_wallets (
                id BIGSERIAL PRIMARY KEY,
                telegram_id BIGINT UNIQUE NOT NULL,
                balance NUMERIC(14, 2) NOT NULL DEFAULT 0,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))

        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bingo_transactions (
                id BIGSERIAL PRIMARY KEY,
                telegram_id BIGINT NOT NULL,
                transaction_type VARCHAR(30) NOT NULL,
                amount NUMERIC(14, 2) NOT NULL,
                status VARCHAR(30) NOT NULL DEFAULT 'pending',
                description TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))

        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bingo_referrals (
                id BIGSERIAL PRIMARY KEY,
                referrer_telegram_id BIGINT NOT NULL,
                referred_telegram_id BIGINT UNIQUE NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))

    logger.info("Database tables are ready.")


async def ensure_user(user):
    if engine is None:
        raise RuntimeError("Database is not initialized.")

    default_language = (
        "am"
        if (user.language_code or "").lower().startswith("am")
        else "en"
    )

    async with engine.begin() as conn:
        await conn.execute(
            text("""
                INSERT INTO bingo_users
                    (telegram_id, username, display_name, language)
                VALUES
                    (:tid, :username, :display_name, :language)
                ON CONFLICT (telegram_id)
                DO UPDATE SET username = EXCLUDED.username
            """),
            {
                "tid": user.id,
                "username": user.username,
                "display_name": user.first_name or "Player",
                "language": default_language,
            },
        )

        await conn.execute(
            text("""
                INSERT INTO bingo_wallets (telegram_id, balance)
                VALUES (:tid, 0)
                ON CONFLICT (telegram_id) DO NOTHING
            """),
            {"tid": user.id},
        )


async def get_language(telegram_id):
    async with engine.connect() as conn:
        result = await conn.execute(
            text("""
                SELECT language
                FROM bingo_users
                WHERE telegram_id = :tid
            """),
            {"tid": telegram_id},
        )
        row = result.first()

    return row[0] if row and row[0] in ("en", "am") else "en"


async def set_language(telegram_id, language):
    async with engine.begin() as conn:
        await conn.execute(
            text("""
                UPDATE bingo_users
                SET language = :language
                WHERE telegram_id = :tid
            """),
            {"tid": telegram_id, "language": language},
        )


async def get_balance(telegram_id):
    async with engine.connect() as conn:
        result = await conn.execute(
            text("""
                SELECT balance
                FROM bingo_wallets
                WHERE telegram_id = :tid
            """),
            {"tid": telegram_id},
        )
        row = result.first()

    return float(row[0]) if row else 0.0


# ==================================================
# KEYBOARD AND WELCOME MESSAGES
# ==================================================

def make_keyboard(language):
    return ReplyKeyboardMarkup(
        MENUS[language],
        resize_keyboard=True,
        one_time_keyboard=False,
        is_persistent=False,
        input_field_placeholder=(
            "Choose a Bingo option 🎱"
            if language == "en"
            else "የቢንጎ አማራጭ ይምረጡ 🎱"
        ),
    )


def welcome_message(language):
    if language == "am":
        return (
            "🎉🎱 እንኳን ወደ Bingo በደህና መጡ!\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "👋 ሰላም! ከእኛ ጋር ለመጫወት ዝግጁ ነዎት?\n\n"
            "🎮 አሁን ይጫወቱ\n"
            "💰 ቀሪ ሂሳብዎን ይመልከቱ\n"
            "🏆 የማሸነፊያ ቅጦችን ይመልከቱ\n"
            "👥 ጓደኞችዎን ይጋብዙ\n\n"
            "👇 ከታች ካሉት የቁልፍ ሰሌዳ አዝራሮች "
            "አንዱን በመጫን ይቀጥሉ!\n\n"
            "🌍 Welcome to Bingo! Choose a button below."
        )

    return (
        "🎉🎱 Welcome to Bingo!\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "👋 Hello, Bingo player! Ready to join the fun?\n\n"
        "🎮 Play a game\n"
        "💰 Check your balance\n"
        "🏆 Explore winning patterns\n"
        "👥 Invite your friends\n\n"
        "👇 Tap any button on the keyboard below "
        "to open the section you want!\n\n"
        "🇪🇹 እንኳን ወደ Bingo በደህና መጡ! "
        "ከታች ያለውን አዝራር ይምረጡ።"
    )


async def show_menu(update: Update, language=None):
    if language is None:
        language = await get_language(update.effective_user.id)

    await update.effective_message.reply_text(
        welcome_message(language),
        reply_markup=make_keyboard(language),
    )


# ==================================================
# START COMMAND AND REFERRALS
# ==================================================

async def start_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    user = update.effective_user
    await ensure_user(user)

    if context.args:
        referral_arg = context.args[0]

        if referral_arg.startswith("ref_"):
            try:
                referrer_id = int(referral_arg[4:])

                if referrer_id != user.id:
                    async with engine.begin() as conn:
                        await conn.execute(
                            text("""
                                INSERT INTO bingo_referrals
                                    (referrer_telegram_id,
                                     referred_telegram_id)
                                VALUES (:referrer, :referred)
                                ON CONFLICT (referred_telegram_id)
                                DO NOTHING
                            """),
                            {
                                "referrer": referrer_id,
                                "referred": user.id,
                            },
                        )
            except ValueError:
                logger.info("Invalid referral code.")

    # Remove any old keyboard, then show the new dashboard keyboard.
    await update.effective_message.reply_text(
        "🎱 Bingo menu is ready!",
        reply_markup=ReplyKeyboardRemove(),
    )

    await show_menu(update)


# ==================================================
# KEYBOARD BUTTON HANDLER
# ==================================================

async def menu_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    message = update.effective_message
    user = update.effective_user
    selected = (message.text or "").strip()

    await ensure_user(user)
    language = await get_language(user.id)

    # Handle a pending display-name change.
    if context.user_data.get("waiting_for_username"):
        new_name = selected.strip()

        if selected.startswith("/"):
            context.user_data["waiting_for_username"] = False
            await message.reply_text(
                "Name change cancelled."
                if language == "en"
                else "የስም ለውጡ ተሰርዟል።",
                reply_markup=make_keyboard(language),
            )
            return

        if not new_name or len(new_name) > 100:
            await message.reply_text(
                "Please enter a name from 1 to 100 characters."
                if language == "en"
                else "እባክዎ ከ1 እስከ 100 ቁምፊ ያለውን ስም ያስገቡ።",
                reply_markup=make_keyboard(language),
            )
            return

        async with engine.begin() as conn:
            await conn.execute(
                text("""
                    UPDATE bingo_users
                    SET display_name = :name
                    WHERE telegram_id = :tid
                """),
                {"name": new_name, "tid": user.id},
            )

        context.user_data["waiting_for_username"] = False

        await message.reply_text(
            f"✅ Your display name is now: {new_name}"
            if language == "en"
            else f"✅ አዲሱ ስምዎ: {new_name}",
            reply_markup=make_keyboard(language),
        )
        return

    action = ACTIONS.get(selected)

    if not action:
        await message.reply_text(
            "Please choose an option using the keyboard below. 👇"
            if language == "en"
            else "እባክዎ ከታች ካለው የቁልፍ ሰሌዳ አማራጭ ይምረጡ። 👇",
            reply_markup=make_keyboard(language),
        )
        return

    # --------------------------------------------------
    # PLAY NOW
    # --------------------------------------------------
    if action == "play":
        if WEB_APP_URL:
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    text=(
                        "🎮 Open Bingo Game"
                        if language == "en"
                        else "🎮 ቢንጎን ይክፈቱ"
                    ),
                    web_app=WebAppInfo(url=WEB_APP_URL),
                )
            ]])

            await message.reply_text(
                (
                    "🎱 Ready to play!\n"
                    "Open the Bingo app below."
                    if language == "en"
                    else
                    "🎱 ለመጫወት ዝግጁ ነዎት!\n"
                    "ከታች ያለውን የቢንጎ መተግበሪያ ይክፈቱ።"
                ),
                reply_markup=keyboard,
            )
        else:
            await message.reply_text(
                (
                    "🎮 The Bingo game is being prepared. "
                    "The game lobby is not available yet."
                    if language == "en"
                    else
                    "🎮 የቢንጎ ጨዋታ እየተዘጋጀ ነው። "
                    "የጨዋታ ሎቢው ገና አልተዘጋጀም።"
                ),
                reply_markup=make_keyboard(language),
            )

    # --------------------------------------------------
    # CHECK BALANCE
    # --------------------------------------------------
    elif action == "balance":
        balance = await get_balance(user.id)

        await message.reply_text(
            (
                f"💰 Your Bingo balance: {balance:,.2f} Birr\n"
                "🔒 Only verified transactions affect your balance."
                if language == "en"
                else
                f"💰 የቢንጎ ቀሪ ሂሳብዎ: {balance:,.2f} ብር\n"
                "🔒 የተረጋገጡ ግብይቶች ብቻ ሂሳብዎን ይቀይራሉ።"
            ),
            reply_markup=make_keyboard(language),
        )

    # --------------------------------------------------
    # DEPOSIT
    # --------------------------------------------------
    elif action == "deposit":
        await message.reply_text(
            (
                "➕ Deposits are not enabled yet.\n\n"
                "🛡️ Please do not send money to anyone claiming "
                "to deposit funds into this bot. Payment verification "
                "must be configured first."
                if language == "en"
                else
                "➕ የገንዘብ ማስገባት አገልግሎት ገና አልተጀመረም።\n\n"
                "🛡️ የክፍያ ማረጋገጫ እስኪዘጋጅ ድረስ "
                "ለማንም ገንዘብ አይላኩ።"
            ),
            reply_markup=make_keyboard(language),
        )

    # --------------------------------------------------
    # SUPPORT
    # --------------------------------------------------
    elif action == "support":
        if SUPPORT_URL:
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    text=(
                        "🆘 Contact Support"
                        if language == "en"
                        else "🆘 ድጋፍን ያግኙ"
                    ),
                    url=SUPPORT_URL,
                )
            ]])

            await message.reply_text(
                (
                    "🆘 Need help? Tap below to contact support."
                    if language == "en"
                    else "🆘 እገዛ ይፈልጋሉ? ድጋፍን ለማግኘት ከታች ይጫኑ።"
                ),
                reply_markup=keyboard,
            )
        else:
            await message.reply_text(
                (
                    "🆘 Support contact has not been configured yet."
                    if language == "en"
                    else "🆘 የድጋፍ አድራሻ ገና አልተዘጋጀም።"
                ),
                reply_markup=make_keyboard(language),
            )

    # --------------------------------------------------
    # INSTRUCTIONS
    # --------------------------------------------------
    elif action == "instructions":
        await message.reply_text(
            (
                "📖 HOW TO PLAY BINGO\n\n"
                "1️⃣ Open Play Now.\n"
                "2️⃣ Choose an available cartela.\n"
                "3️⃣ Select a stake when games are enabled.\n"
                "4️⃣ Watch the called numbers.\n"
                "5️⃣ Claim Bingo when your card matches a valid pattern.\n\n"
                "🛡️ Never pay outside the official verified payment process."
                if language == "en"
                else
                "📖 ቢንጎ እንዴት ይጫወታሉ?\n\n"
                "1️⃣ አሁን ይጫወቱን ይክፈቱ።\n"
                "2️⃣ ያልተያዘ ካርቴላ ይምረጡ።\n"
                "3️⃣ ጨዋታዎች ሲጀምሩ የውርርድ መጠን ይምረጡ።\n"
                "4️⃣ የሚጠሩትን ቁጥሮች ይከታተሉ።\n"
                "5️⃣ ትክክለኛ የማሸነፊያ ቅጥ ሲሞላ ቢንጎ ይጠይቁ።\n\n"
                "🛡️ ከተረጋገጠው የክፍያ ሂደት ውጭ ገንዘብ አይክፈሉ።"
            ),
            reply_markup=make_keyboard(language),
        )

    # --------------------------------------------------
    # WIN PATTERNS
    # --------------------------------------------------
    elif action == "patterns":
        await message.reply_text(
            (
                "🏆 WINNING PATTERNS\n\n"
                "The official winning-pattern guide will appear here "
                "when it is configured. Claims must be validated "
                "by the server before a prize is awarded."
                if language == "en"
                else
                "🏆 የማሸነፊያ ቅጦች\n\n"
                "ይፋዊው የማሸነፊያ ቅጦች መመሪያ ሲዘጋጅ እዚህ "
                "ይታያል። ሽልማት ከመሰጠቱ በፊት ጥያቄው "
                "በሰርቨሩ መረጋገጥ አለበት።"
            ),
            reply_markup=make_keyboard(language),
        )

    # --------------------------------------------------
    # CHANGE DISPLAY NAME
    # --------------------------------------------------
    elif action == "username":
        context.user_data["waiting_for_username"] = True

        await message.reply_text(
            (
                "✏️ Send the display name you want to use "
                "(maximum 100 characters)."
                if language == "en"
                else
                "✏️ መጠቀም የሚፈልጉትን የሚታይ ስም ይላኩ "
                "(ከፍተኛው 100 ቁምፊ)።"
            ),
            reply_markup=make_keyboard(language),
        )

    # --------------------------------------------------
    # INVITE FRIENDS
    # --------------------------------------------------
    elif action == "invite":
        bot_username = context.bot.username

        if bot_username:
            referral_link = (
                f"https://t.me/{bot_username}?start=ref_{user.id}"
            )

            await message.reply_text(
                (
                    "👥 INVITE FRIENDS\n\n"
                    "Share your personal referral link:\n"
                    f"{referral_link}\n\n"
                    "Referral rewards are not enabled yet."
                    if language == "en"
                    else
                    "👥 ጓደኞችን ይጋብዙ\n\n"
                    "የግል የግብዣ ሊንክዎን ያጋሩ:\n"
                    f"{referral_link}\n\n"
                    "የግብዣ ሽልማቶች ገና አልተጀመሩም።"
                ),
                reply_markup=make_keyboard(language),
            )
        else:
            await message.reply_text(
                "The bot username is not available yet.",
                reply_markup=make_keyboard(language),
            )

    # --------------------------------------------------
    # LEADERBOARD
    # --------------------------------------------------
    elif action == "leaderboard":
        await message.reply_text(
            (
                "🥇 LEADERBOARD\n\n"
                "The leaderboard will be available when the game "
                "and verified scoring system are enabled."
                if language == "en"
                else
                "🥇 የደረጃ ሰንጠረዥ\n\n"
                "ጨዋታውና የተረጋገጠው የነጥብ ስርዓት ሲጀምሩ "
                "የደረጃ ሰንጠረዡ ይታያል።"
            ),
            reply_markup=make_keyboard(language),
        )

    # --------------------------------------------------
    # TRANSACTION HISTORY
    # --------------------------------------------------
    elif action == "history":
        async with engine.connect() as conn:
            result = await conn.execute(
                text("""
                    SELECT transaction_type, amount, status, created_at
                    FROM bingo_transactions
                    WHERE telegram_id = :tid
                    ORDER BY created_at DESC
                    LIMIT 10
                """),
                {"tid": user.id},
            )
            rows = result.fetchall()

        if not rows:
            history_text = (
                "📜 You have no transactions yet."
                if language == "en"
                else "📜 እስካሁን ምንም ግብይት የለዎትም።"
            )
        else:
            history_text = (
                "📜 RECENT TRANSACTIONS\n\n"
                if language == "en"
                else "📜 የቅርብ ጊዜ ግብይቶች\n\n"
            )

            for transaction_type, amount, status, created_at in rows:
                history_text += (
                    f"• {transaction_type} | "
                    f"{float(amount):,.2f} Birr | "
                    f"{status} | {created_at:%Y-%m-%d %H:%M}\n"
                )

        await message.reply_text(
            history_text,
            reply_markup=make_keyboard(language),
        )

    # --------------------------------------------------
    # CHANGE LANGUAGE
    # --------------------------------------------------
    elif action == "language":
        new_language = "am" if language == "en" else "en"
        await set_language(user.id, new_language)

        await message.reply_text(
            (
                "🌐 Language changed to English."
                if new_language == "en"
                else "🌐 ቋንቋው ወደ አማርኛ ተቀይሯል።"
            ),
            reply_markup=make_keyboard(new_language),
        )

        await message.reply_text(
            welcome_message(new_language),
            reply_markup=make_keyboard(new_language),
        )


# ==================================================
# BOT STARTUP AND SHUTDOWN
# ==================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global telegram_app

    await setup_database()

    if TELEGRAM_BOT_TOKEN:
        telegram_app = (
            Application.builder()
            .token(TELEGRAM_BOT_TOKEN)
            .build()
        )

        telegram_app.add_handler(
            CommandHandler("start", start_command)
        )

        telegram_app.add_handler(
            MessageHandler(
                filters.TEXT & ~filters.COMMAND,
                menu_handler,
            )
        )

        await telegram_app.initialize()
        await telegram_app.start()

        if telegram_app.updater is not None:
            await telegram_app.updater.start_polling(
                drop_pending_updates=False
            )

        logger.info("Telegram bot polling has started.")
    else:
        logger.warning(
            "TELEGRAM_BOT_TOKEN is missing. Bot polling is disabled."
        )

    yield

    if telegram_app is not None:
        if telegram_app.updater is not None:
            await telegram_app.updater.stop()

        await telegram_app.stop()
        await telegram_app.shutdown()

    if engine is not None:
        await engine.dispose()


# ==================================================
# FASTAPI WEB SERVICE
# ==================================================

app = FastAPI(
    title="Telegram Bingo",
    description="Telegram Bingo bot and backend",
    lifespan=lifespan,
)


@app.get("/", response_class=HTMLResponse)
async def home():
    return """
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>Telegram Bingo</title>
      <style>
        body {
          margin: 0;
          min-height: 100vh;
          display: flex;
          align-items: center;
          justify-content: center;
          background: linear-gradient(135deg, #101a43, #54228c);
          color: white;
          font-family: Arial, sans-serif;
          text-align: center;
        }
        main {
          padding: 32px;
          max-width: 480px;
        }
        h1 {
          font-size: 38px;
        }
        p {
          line-height: 1.7;
          color: #e5e7eb;
        }
      </style>
    </head>
    <body>
      <main>
        <h1>🎱 Telegram Bingo</h1>
        <p>Welcome to Bingo! Open your Telegram bot to use the
        dashboard and its keyboard.</p>
        <p>🎮 Play · 💰 Balance · 🏆 Patterns · 👥 Invite</p>
      </main>
    </body>
    </html>
    """


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/health/database")
async def database_health():
    if engine is None:
        return {"status": "disconnected"}

    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))

        return {"status": "connected"}

    except Exception:
        logger.exception("Database health check failed.")
        return {"status": "disconnected"}
