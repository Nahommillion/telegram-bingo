
import os
import logging
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from telegram import (
    Update,
    KeyboardButton,
    ReplyKeyboardMarkup,
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

# =========================================================
# CONFIGURATION
# =========================================================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
WEB_APP_URL = os.getenv(
    "WEB_APP_URL",
    "https://telegram-bingo-zq2q.onrender.com/app",
).strip()
SUPPORT_URL = os.getenv("SUPPORT_URL", "").strip()

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
LOGO_PATH = STATIC_DIR / "bingo.PNG"

STATIC_DIR.mkdir(parents=True, exist_ok=True)

engine = None
telegram_app = None


# =========================================================
# KEYBOARDS
# =========================================================

MENUS = {
    "en": [
        ["🎮 Play Now", "💰 Check Balance"],
        ["➕ Make Deposit", "🆘 Support"],
        ["📖 Instructions", "🏆 Win Patterns"],
        ["✏️ Change Username", "👥 Invite"],
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
    "🎮 Play Now": "play",
    "💰 Check Balance": "balance",
    "➕ Make Deposit": "deposit",
    "🆘 Support": "support",
    "📖 Instructions": "instructions",
    "🏆 Win Patterns": "patterns",
    "✏️ Change Username": "username",
    "👥 Invite": "invite",
    "👥 Invite Friends": "invite",
    "🥇 Leaderboard": "leaderboard",
    "📜 Transaction History": "history",
    "🌐 Change Language": "language",

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


def make_keyboard(language):
    rows = []

    for row in MENUS[language]:
        buttons = []

        for label in row:
            if label in ("🎮 Play Now", "🎮 አሁን ይጫወቱ"):
                buttons.append(
                    KeyboardButton(
                        text=label,
                        web_app=WebAppInfo(url=WEB_APP_URL),
                    )
                )
            else:
                buttons.append(KeyboardButton(text=label))

        rows.append(buttons)

    return ReplyKeyboardMarkup(
        rows,
        resize_keyboard=True,
        one_time_keyboard=False,
        is_persistent=False,
        input_field_placeholder=(
            "Choose a Bingo option 🎱"
            if language == "en"
            else "የቢንጎ አማራጭ ይምረጡ 🎱"
        ),
    )


# =========================================================
# WELCOME MESSAGES
# =========================================================

def welcome_message(language):
    if language == "am":
        return (
            "🎉🎱 እንኳን ወደ Cartela Bingo በደህና መጡ!\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "👋 ሰላም! ከእኛ ጋር ለመጫወት ዝግጁ ነዎት?\n\n"
            "🎮 አሁን ይጫወቱ — ወደ ጨዋታው ይግቡ\n"
            "💰 ቀሪ ሂሳብዎን ይመልከቱ\n"
            "🏆 የማሸነፊያ ቅጦችን ይመልከቱ\n"
            "👥 ጓደኞችዎን ይጋብዙ\n\n"
            "👇 ከታች ካሉት አዝራሮች አንዱን ይጫኑ!\n\n"
            "🎊 መልካም ጨዋታ!"
        )

    return (
        "🎉🎱 Welcome to Cartela Bingo!\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "👋 Hello, Bingo player! Ready to join the fun?\n\n"
        "🎮 Play Now — enter the Bingo app\n"
        "💰 Check your balance\n"
        "🏆 Explore winning patterns\n"
        "👥 Invite your friends\n\n"
        "👇 Tap any button on the keyboard below "
        "to open the section you want!\n\n"
        "🎊 Good luck and enjoy the game!"
    )


async def send_welcome(update: Update, language=None):
    if language is None:
        language = await get_language(update.effective_user.id)

    message = update.effective_message
    keyboard = make_keyboard(language)
    caption = welcome_message(language)

    if LOGO_PATH.is_file():
        await message.reply_photo(
            photo=str(LOGO_PATH),
            caption=caption,
            reply_markup=keyboard,
        )
    else:
        logger.warning("Logo not found at %s", LOGO_PATH)
        await message.reply_text(
            caption,
            reply_markup=keyboard,
        )


# =========================================================
# DATABASE
# =========================================================

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
                "language": (
                    "am"
                    if (user.language_code or "").lower().startswith("am")
                    else "en"
                ),
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
                SELECT language FROM bingo_users
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
            {"language": language, "tid": telegram_id},
        )


async def get_balance(telegram_id):
    async with engine.connect() as conn:
        result = await conn.execute(
            text("""
                SELECT balance FROM bingo_wallets
                WHERE telegram_id = :tid
            """),
            {"tid": telegram_id},
        )
        row = result.first()

    return float(row[0]) if row else 0.0


# =========================================================
# START COMMAND AND REFERRALS
# =========================================================

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

    await send_welcome(update)


# =========================================================
# BUTTON HANDLER
# =========================================================

async def menu_handler(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    message = update.effective_message
    user = update.effective_user
    selected = (message.text or "").strip()

    await ensure_user(user)
    language = await get_language(user.id)

    # Username entry
    if context.user_data.get("waiting_for_username"):
        if selected.startswith("/"):
            context.user_data["waiting_for_username"] = False
            await message.reply_text(
                "Name change cancelled.",
                reply_markup=make_keyboard(language),
            )
            return

        new_name = selected.strip()

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
            (
                f"✅ Your display name is now: {new_name}"
                if language == "en"
                else f"✅ አዲሱ ስምዎ: {new_name}"
            ),
            reply_markup=make_keyboard(language),
        )
        return

    action = ACTIONS.get(selected)

    if not action:
        await message.reply_text(
            "Please choose a button from the keyboard below. 👇"
            if language == "en"
            else "እባክዎ ከታች ካለው የቁልፍ ሰሌዳ አዝራር ይምረጡ። 👇",
            reply_markup=make_keyboard(language),
        )
        return

    # PLAY NOW
    if action == "play":
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
                "🎱 Welcome to the Bingo game!\n"
                "Tap below to open the Mini App."
                if language == "en"
                else
                "🎱 ወደ ቢንጎ ጨዋታ እንኳን በደህና መጡ!\n"
                "Mini App ለመክፈት ከታች ይጫኑ።"
            ),
            reply_markup=keyboard,
        )

    # BALANCE
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

    # DEPOSIT
    elif action == "deposit":
        await message.reply_text(
            (
                "➕ Deposits are not enabled yet.\n\n"
                "🛡️ Do not send money until official payment "
                "verification has been configured."
                if language == "en"
                else
                "➕ የገንዘብ ማስገባት ገና አልተጀመረም።\n\n"
                "🛡️ ይፋዊ የክፍያ ማረጋገጫ እስኪዘጋጅ ድረስ "
                "ገንዘብ አይላኩ።"
            ),
            reply_markup=make_keyboard(language),
        )

    # SUPPORT
    elif action == "support":
        if SUPPORT_URL:
            await message.reply_text(
                (
                    "🆘 Tap below to contact support."
                    if language == "en"
                    else "🆘 ድጋፍን ለማግኘት ከታች ይጫኑ።"
                ),
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        text="🆘 Contact Support",
                        url=SUPPORT_URL,
                    )
                ]]),
            )
        else:
            await message.reply_text(
                "🆘 Support contact has not been configured yet."
                if language == "en"
                else "🆘 የድጋፍ አድራሻ ገና አልተዘጋጀም።",
                reply_markup=make_keyboard(language),
            )

    # INSTRUCTIONS
    elif action == "instructions":
        await message.reply_text(
            (
                "📖 HOW TO PLAY BINGO\n\n"
                "1️⃣ Press Play Now to open the Mini App.\n"
                "2️⃣ Choose an available cartela when the game is ready.\n"
                "3️⃣ Select a stake when betting is enabled.\n"
                "4️⃣ Watch the called numbers.\n"
                "5️⃣ Claim Bingo when your card matches a valid pattern.\n\n"
                "🛡️ Always use official, verified payment channels."
                if language == "en"
                else
                "📖 ቢንጎ እንዴት ይጫወታሉ?\n\n"
                "1️⃣ Mini App ለመክፈት አሁን ይጫወቱን ይጫኑ።\n"
                "2️⃣ ጨዋታው ሲዘጋጅ ያልተያዘ ካርቴላ ይምረጡ።\n"
                "3️⃣ ውርርድ ሲነቃ የውርርድ መጠን ይምረጡ።\n"
                "4️⃣ የሚጠሩትን ቁጥሮች ይከታተሉ።\n"
                "5️⃣ ትክክለኛ ቅጥ ሲሞላ ቢንጎ ይጠይቁ።\n\n"
                "🛡️ ይፋዊ የክፍያ መንገዶችን ብቻ ይጠቀሙ።"
            ),
            reply_markup=make_keyboard(language),
        )

    # WIN PATTERNS
    elif action == "patterns":
        await message.reply_text(
            (
                "🏆 WINNING PATTERNS\n\n"
                "The official winning-pattern guide will be added "
                "when the game patterns are configured."
                if language == "en"
                else
                "🏆 የማሸነፊያ ቅጦች\n\n"
                "የጨዋታው ቅጦች ሲዘጋጁ ይፋዊው መመሪያ እዚህ ይታያል።"
            ),
            reply_markup=make_keyboard(language),
        )

    # CHANGE USERNAME
    elif action == "username":
        context.user_data["waiting_for_username"] = True
        await message.reply_text(
            (
                "✏️ Send your preferred display name "
                "(maximum 100 characters)."
                if language == "en"
                else "✏️ መጠቀም የሚፈልጉትን ስም ይላኩ "
                     "(ከፍተኛው 100 ቁምፊ)።"
            ),
            reply_markup=make_keyboard(language),
        )

    # INVITE FRIENDS
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
                "Bot username is not available yet.",
                reply_markup=make_keyboard(language),
            )

    # LEADERBOARD
    elif action == "leaderboard":
        await message.reply_text(
            (
                "🥇 LEADERBOARD\n\n"
                "The leaderboard will be available when the game "
                "and scoring system are implemented."
                if language == "en"
                else
                "🥇 የደረጃ ሰንጠረዥ\n\n"
                "ጨዋታውና የነጥብ ስርዓቱ ሲዘጋጁ ይገኛል።"
            ),
            reply_markup=make_keyboard(language),
        )

    # TRANSACTION HISTORY
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

            for tx_type, amount, status, created_at in rows:
                history_text += (
                    f"• {tx_type} | {float(amount):,.2f} Birr "
                    f"| {status} | {created_at:%Y-%m-%d %H:%M}\n"
                )

        await message.reply_text(
            history_text,
            reply_markup=make_keyboard(language),
        )

    # CHANGE LANGUAGE
    elif action == "language":
        new_language = "am" if language == "en" else "en"
        await set_language(user.id, new_language)
        await send_welcome(update, new_language)


# =========================================================
# FASTAPI LIFESPAN / TELEGRAM POLLING
# =========================================================

@asynccontextmanager
async def lifespan(app):
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

        logger.info("Telegram bot started.")
    else:
        logger.warning("TELEGRAM_BOT_TOKEN is missing.")

    yield

    if telegram_app is not None:
        if telegram_app.updater is not None:
            await telegram_app.updater.stop()

        await telegram_app.stop()
        await telegram_app.shutdown()

    if engine is not None:
        await engine.dispose()


# =========================================================
# FASTAPI APPLICATION
# =========================================================

app = FastAPI(
    title="Cartela Bingo",
    description="Telegram Bingo bot and Mini App",
    lifespan=lifespan,
)

app.mount(
    "/static",
    StaticFiles(directory=str(STATIC_DIR)),
    name="static",
)


@app.get("/", response_class=HTMLResponse)
async def home():
    return """
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>Cartela Bingo</title>
      <style>
        body {
          margin: 0;
          min-height: 100vh;
          display: flex;
          align-items: center;
          justify-content: center;
          background: linear-gradient(135deg, #101923, #25204e);
          color: white;
          font-family: Arial, sans-serif;
          text-align: center;
        }
        main { padding: 24px; max-width: 480px; }
        img { width: 100%; max-width: 320px; border-radius: 16px; }
      </style>
    </head>
    <body>
      <main>
        <img src="/static/bingo.PNG" alt="Cartela Bingo logo">
        <h1>🎉 Welcome to Cartela Bingo!</h1>
        <p>Open your Telegram bot to access the Bingo dashboard.</p>
      </main>
    </body>
    </html>
    """


@app.get("/app", response_class=HTMLResponse)
async def mini_app():
    return """
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="UTF-8">
      <meta name="viewport"
            content="width=device-width, initial-scale=1, maximum-scale=1">
      <script src="https://telegram.org/js/telegram-web-app.js"></script>
      <title>Cartela Bingo</title>
      <style>
        * { box-sizing: border-box; }
        body {
          margin: 0;
          padding: 20px;
          background: #101923;
          color: #ffffff;
          font-family: Arial, sans-serif;
          text-align: center;
        }
        .logo {
          display: block;
          width: 100%;
          max-width: 340px;
          max-height: 220px;
          object-fit: contain;
          margin: 0 auto 20px;
          border-radius: 16px;
        }
        h1 { font-size: 26px; }
        p { color: #cbd5e1; line-height: 1.6; }
        .menu {
          display: grid;
          grid-template-columns: 1fr 1fr;
          gap: 12px;
          margin-top: 24px;
        }
        button {
          width: 100%;
          min-height: 56px;
          padding: 14px 8px;
          border: 1px solid #35475a;
          border-radius: 12px;
          background: #223244;
          color: white;
          font-size: 15px;
          font-weight: 600;
        }
        button.primary {
          grid-column: 1 / -1;
          background: #7c3aed;
          border: none;
        }
        .note { font-size: 12px; margin-top: 24px; }
      </style>
    </head>
    <body>
      <img class="logo" src="/static/bingo.PNG" alt="Cartela Bingo logo">
      <h1>🎉 Cartela Bingo</h1>
      <p>🎱 Welcome! Choose an option below.</p>

      <div class="menu">
        <button class="primary" onclick="showNotice()">
          🎮 Enter Bingo Lobby
        </button>
        <button onclick="showNotice()">
          💰 Check Balance
        </button>
        <button onclick="showNotice()">
          🏆 Win Patterns
        </button>
        <button onclick="showNotice()">
          👥 Invite Friends
        </button>
        <button onclick="showNotice()">
          📖 Instructions
        </button>
      </div>

      <p class="note">
        The live game, wallet and payment features still need to be connected.
      </p>

      <script>
        const tg = window.Telegram && window.Telegram.WebApp;

        if (tg) {
          tg.ready();
          tg.expand();
        }

        function showNotice() {
          const message =
            "This feature is not connected yet. The game backend " +
            "must be implemented before you can play.";
          if (tg && tg.showAlert) {
            tg.showAlert(message);
          } else {
            alert(message);
          }
        }
      </script>
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
