import os,json,asyncio,random,hmac,hashlib,urllib.parse,time,secrets,logging,math,re,urllib.request,urllib.error
from contextlib import asynccontextmanager
from pathlib import Path
from decimal import Decimal
from typing import Optional
from fastapi import FastAPI,HTTPException,Header,Request,WebSocket,WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel,Field
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine
from telegram import Update,KeyboardButton,ReplyKeyboardMarkup,WebAppInfo,InlineKeyboardMarkup,InlineKeyboardButton
from telegram.ext import Application,CommandHandler,MessageHandler,ContextTypes,filters
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger=logging.getLogger("cartela_bingo")
BOT=os.getenv("TELEGRAM_BOT_TOKEN","").strip(); DB=os.getenv("DATABASE_URL","").strip()
WEB=os.getenv("WEB_APP_URL","https://telegram-bingo-zq2q.onrender.com/app").strip()
SUPPORT=os.getenv("SUPPORT_URL","").strip(); PAYKEY=os.getenv("PAYMENT_INGEST_API_KEY","").strip(); OWNER_ID=os.getenv("PLATFORM_OWNER_TELEGRAM_ID","").strip()
CHECK_ET_API_KEY=os.getenv("CHECK_ET_API_KEY","").strip()
STATIC=Path(__file__).parent/"static"; STATIC.mkdir(exist_ok=True)
engine=bot=None; ticker=None; lock=asyncio.Lock()
g={"id":1,"status":"waiting","seconds":30,"called":[],"players":{},"winner":None,"seq":[],"last":0,"lobby_started_at":None,"finished_at_ts":0}
PROVIDERS={"telebirr","cbe_birr","ebirr","cbe_bank","mpesa"}
def dsn(x):
 if x.startswith("postgres://"): return x.replace("postgres://","postgresql+asyncpg://",1)
 if x.startswith("postgresql://") and not x.startswith("postgresql+asyncpg://"): return x.replace("postgresql://","postgresql+asyncpg://",1)
 return x
async def dbinit():
 global engine
 if not DB: raise RuntimeError("Set DATABASE_URL in Render.")
 engine=create_async_engine(dsn(DB),pool_pre_ping=True)
 qs=[
 "CREATE TABLE IF NOT EXISTS bingo_users(telegram_id BIGINT PRIMARY KEY,username TEXT,display_name TEXT,language VARCHAR(5) DEFAULT 'en',phone_number TEXT,phone_verified BOOLEAN DEFAULT FALSE,created_at TIMESTAMPTZ DEFAULT NOW())",
  "ALTER TABLE bingo_users ADD COLUMN IF NOT EXISTS phone_number TEXT",
  "ALTER TABLE bingo_users ADD COLUMN IF NOT EXISTS phone_verified BOOLEAN DEFAULT FALSE",
 "CREATE TABLE IF NOT EXISTS bingo_wallets(telegram_id BIGINT PRIMARY KEY REFERENCES bingo_users(telegram_id),balance NUMERIC(14,2) DEFAULT 0,updated_at TIMESTAMPTZ DEFAULT NOW())",
 "CREATE TABLE IF NOT EXISTS bingo_transactions(id BIGSERIAL PRIMARY KEY,telegram_id BIGINT,transaction_type TEXT,amount NUMERIC(14,2),status TEXT DEFAULT 'pending',description TEXT,external_reference TEXT UNIQUE,created_at TIMESTAMPTZ DEFAULT NOW())",
 # CREATE TABLE IF NOT EXISTS does not update tables created by older app versions.
 # Migrate the existing production table before wallet/history/deposit queries run.
 "ALTER TABLE bingo_transactions ADD COLUMN IF NOT EXISTS description TEXT",
 "ALTER TABLE bingo_transactions ADD COLUMN IF NOT EXISTS external_reference TEXT",
 "CREATE UNIQUE INDEX IF NOT EXISTS uq_bingo_transactions_external_reference ON bingo_transactions(external_reference) WHERE external_reference IS NOT NULL",
 "CREATE TABLE IF NOT EXISTS bingo_referrals(id BIGSERIAL PRIMARY KEY,referrer_telegram_id BIGINT,referred_telegram_id BIGINT UNIQUE,created_at TIMESTAMPTZ DEFAULT NOW())",
 "ALTER TABLE bingo_referrals ADD COLUMN IF NOT EXISTS referrer_telegram_id BIGINT",
 "ALTER TABLE bingo_referrals ADD COLUMN IF NOT EXISTS referred_telegram_id BIGINT",
 "CREATE UNIQUE INDEX IF NOT EXISTS uq_bingo_referrals_referred_telegram_id ON bingo_referrals(referred_telegram_id)",
 "CREATE TABLE IF NOT EXISTS deposit_requests(id BIGSERIAL PRIMARY KEY,telegram_id BIGINT,provider TEXT,amount NUMERIC(14,2),reference TEXT,status TEXT DEFAULT 'pending',created_at TIMESTAMPTZ DEFAULT NOW(),verified_at TIMESTAMPTZ)",
  "ALTER TABLE deposit_requests ADD COLUMN IF NOT EXISTS payment_message TEXT",
  "ALTER TABLE deposit_requests ADD COLUMN IF NOT EXISTS parsed_message_amount NUMERIC(14,2)",
  "CREATE TABLE IF NOT EXISTS withdrawal_requests(id BIGSERIAL PRIMARY KEY,telegram_id BIGINT NOT NULL,provider TEXT NOT NULL,amount NUMERIC(14,2) NOT NULL,destination TEXT NOT NULL,status TEXT DEFAULT 'pending',created_at TIMESTAMPTZ DEFAULT NOW(),processed_at TIMESTAMPTZ)",
 "CREATE TABLE IF NOT EXISTS payment_records(id BIGSERIAL PRIMARY KEY,provider TEXT,external_reference TEXT UNIQUE,amount NUMERIC(14,2),payer_reference TEXT,memo TEXT,matched_request_id BIGINT,created_at TIMESTAMPTZ DEFAULT NOW())",
 "CREATE TABLE IF NOT EXISTS bingo_games(id BIGSERIAL PRIMARY KEY,status TEXT,created_at TIMESTAMPTZ DEFAULT NOW(),started_at TIMESTAMPTZ,finished_at TIMESTAMPTZ,winner_telegram_id BIGINT,called_numbers JSONB DEFAULT '[]'::jsonb)",
 "CREATE TABLE IF NOT EXISTS bingo_game_players(game_id BIGINT,telegram_id BIGINT,cartela_number INT,cartela JSONB,stake NUMERIC(14,2) DEFAULT 0,PRIMARY KEY(game_id,telegram_id),UNIQUE(game_id,cartela_number))",
 "ALTER TABLE bingo_games ADD COLUMN IF NOT EXISTS lobby_started_at TIMESTAMPTZ",
 "CREATE TABLE IF NOT EXISTS bingo_game_entries(game_id BIGINT NOT NULL,telegram_id BIGINT NOT NULL,cartela_number INT NOT NULL,cartela JSONB NOT NULL,stake NUMERIC(14,2) NOT NULL DEFAULT 30,created_at TIMESTAMPTZ DEFAULT NOW(),PRIMARY KEY(game_id,cartela_number),UNIQUE(game_id,telegram_id,cartela_number))",
 "CREATE TABLE IF NOT EXISTS bingo_platform_wallet(id INT PRIMARY KEY DEFAULT 1 CHECK(id=1),balance NUMERIC(14,2) NOT NULL DEFAULT 0,updated_at TIMESTAMPTZ DEFAULT NOW())",
 "INSERT INTO bingo_platform_wallet(id,balance) VALUES(1,0) ON CONFLICT(id) DO NOTHING",
 "INSERT INTO bingo_game_entries(game_id,telegram_id,cartela_number,cartela,stake) SELECT game_id,telegram_id,cartela_number,cartela,stake FROM bingo_game_players WHERE TRUE ON CONFLICT(game_id,cartela_number) DO NOTHING"
 ]
 async with engine.begin() as c:
  for q in qs: await c.execute(text(q))
  r=await c.execute(text("SELECT id,status,called_numbers,lobby_started_at,started_at,finished_at,winner_telegram_id FROM bingo_games WHERE status IN ('waiting','running') ORDER BY id DESC LIMIT 1")); row=r.first()
  if row:
   g.update(id=row[0],status=row[1],called=row[2] or [],lobby_started_at=row[3].timestamp() if row[3] else None,finished_at_ts=0)
   if g["status"]=="waiting": g["seconds"]=max(0,30-int(time.time()-(g["lobby_started_at"] or time.time()))) if g["lobby_started_at"] else 30
   else: g["seconds"]=0
   if g["status"]=="running": g["seq"]=[n for n in range(1,76) if n not in set(g["called"])];random.shuffle(g["seq"]);g["last"]=time.time()
   pr=await c.execute(text("SELECT p.telegram_id,COALESCE(u.display_name,u.username,'Player'),p.cartela_number,p.cartela,p.stake FROM bingo_game_entries p LEFT JOIN bingo_users u ON u.telegram_id=p.telegram_id WHERE p.game_id=:g"),{"g":g["id"]})
   for x in pr.fetchall():
    card=x[3] if isinstance(x[3],list) else json.loads(x[3] or '[]')
    g["players"][f"{int(x[0])}:{int(x[2])}"] = {"user_id":int(x[0]),"name":x[1],"n":int(x[2]),"card":card,"stake":float(x[4] or 0)}
  else:
   g["id"]=(await c.execute(text("INSERT INTO bingo_games(status) VALUES('waiting') RETURNING id"))).scalar_one()
 logger.info("Database initialized; game #%s status=%s players=%s",g["id"],g["status"],len(g["players"]))
async def reg(tid,username=None,name=None,language="en"):
 async with engine.begin() as c:
  await c.execute(text("INSERT INTO bingo_users(telegram_id,username,display_name,language) VALUES(:i,:u,:n,:l) ON CONFLICT(telegram_id) DO UPDATE SET username=COALESCE(EXCLUDED.username,bingo_users.username),display_name=COALESCE(EXCLUDED.display_name,bingo_users.display_name)"),{"i":tid,"u":username,"n":name,"l":language})
  await c.execute(text("INSERT INTO bingo_wallets(telegram_id) VALUES(:i) ON CONFLICT DO NOTHING"),{"i":tid})
async def langof(i):
 async with engine.connect() as c:return (await c.execute(text("SELECT language FROM bingo_users WHERE telegram_id=:i"),{"i":i})).scalar_one_or_none() or "en"
async def setlang(i,l):
 async with engine.begin() as c:await c.execute(text("UPDATE bingo_users SET language=:l WHERE telegram_id=:i"),{"l":l,"i":i})
def kb(l):
 rows=[["🎮 Play Now","💰 Check Balance"],["➕ Make Deposit","🆘 Support"],["📖 Instructions","🏆 Win Patterns"],["✏️ Change Username","👥 Invite Friends"],["🥇 Leaderboard","📜 Transaction History"],["🌐 Change Language"]] if l=="en" else [["🎮 አሁን ይጫወቱ","💰 ቀሪ ሂሳብ"],["➕ ገንዘብ ያስገቡ","🆘 ድጋፍ"],["📖 መመሪያ","🏆 የማሸነፊያ ቅጦች"],["✏️ ስም ይቀይሩ","👥 ጓደኞችን ይጋብዙ"],["🥇 ደረጃ ሰንጠረዥ","📜 የግብይት ታሪክ"],["🌐 ቋንቋ ይቀይሩ"]]
 return ReplyKeyboardMarkup(
    [[KeyboardButton(x) for x in r] for r in rows],
    resize_keyboard=True,
    one_time_keyboard=False,
    is_persistent=False
)
def wel(l):return ("🎉🎱 እንኳን ወደ Cartela Bingo በደህና መጡ!\n🇪🇹 የኢትዮጵያ ቀለሞች • ቢንጎ • ደስታ\n👇 ከታች ያለውን አዝራር ይምረጡ!" if l=="am" else "🎉🎱 Welcome to Cartela Bingo!\n🇪🇹 Ethiopian colors • Bingo • Fun\n👇 Choose an option below!")
async def sendwel(up,l=None):
 l=l or await langof(up.effective_user.id); p=STATIC/"bingo.PNG"
 if p.exists():
  try: await up.effective_message.reply_photo(str(p),caption=wel(l),reply_markup=kb(l));return
  except Exception: logger.exception("Welcome image failed; sending text fallback")
 await up.effective_message.reply_text(wel(l),reply_markup=kb(l))
async def request_contact(up):
 await up.effective_message.reply_text(
  "📱 Welcome! Please share your own phone number to register and use Cartela Bingo. Tap the button below.",
  reply_markup=ReplyKeyboardMarkup([[KeyboardButton("📱 Share my phone number",request_contact=True)]],resize_keyboard=True,one_time_keyboard=True)
 )
async def contact(up,ctx):
 u=up.effective_user;m=up.effective_message;ct=m.contact
 await reg(u.id,u.username,u.first_name,"am" if (u.language_code or "").startswith("am") else "en")
 if not ct or ct.user_id != u.id:
  await m.reply_text("For security, please share your own contact using the button below.")
  await request_contact(up);return
 phone=re.sub(r"[^0-9+]", "", ct.phone_number or "")
 if len(re.sub(r"\D","",phone))<9:
  await m.reply_text("That phone number does not look valid. Please share your contact again.");await request_contact(up);return
 async with engine.begin() as c:
  other=(await c.execute(text("SELECT telegram_id FROM bingo_users WHERE RIGHT(COALESCE(phone_number,''),9)=RIGHT(:p,9) AND telegram_id<>:i LIMIT 1"),{"p":phone,"i":u.id})).scalar_one_or_none()
  if other:
   await m.reply_text("That phone number is already registered to another Telegram account. Please contact support if this is a mistake.",reply_markup=kb(await langof(u.id)));return
  await c.execute(text("UPDATE bingo_users SET phone_number=:p,phone_verified=TRUE WHERE telegram_id=:i"),{"p":phone,"i":u.id})
 await m.reply_text("✅ Phone number registered successfully.",reply_markup=kb(await langof(u.id)))
 await sendwel(up)
async def start(up,ctx):
 logger.info("Received /start update")
 u=up.effective_user; await reg(u.id,u.username,u.first_name,"am" if (u.language_code or "").startswith("am") else "en")
 if ctx.args and ctx.args[0].startswith("ref_"):
  try:
   r=int(ctx.args[0][4:])
   if r!=u.id:
    try:
     async with engine.begin() as c:await c.execute(text("INSERT INTO bingo_referrals(referrer_telegram_id,referred_telegram_id) VALUES(:r,:u) ON CONFLICT(referred_telegram_id) DO NOTHING"),{"r":r,"u":u.id})
    except Exception:
     # Referral tracking must not prevent a user from starting the bot.
     logger.exception("Referral tracking failed; continuing /start")
  except ValueError:pass
 async with engine.connect() as c:
  phone=(await c.execute(text("SELECT phone_number FROM bingo_users WHERE telegram_id=:i"),{"i":u.id})).scalar_one_or_none()
 if not phone:
  await request_contact(up);return
 await sendwel(up)
async def bottext(up,ctx):
 logger.info("Received non-command text update")
 u=up.effective_user;m=up.effective_message;s=(m.text or "").strip();await reg(u.id,u.username,u.first_name);l=await langof(u.id)
 async with engine.connect() as c:
  phone=(await c.execute(text("SELECT phone_number FROM bingo_users WHERE telegram_id=:i"),{"i":u.id})).scalar_one_or_none()
 if not phone:
  await request_contact(up);return
 if ctx.user_data.get("rename"):
  async with engine.begin() as c:await c.execute(text("UPDATE bingo_users SET display_name=:n WHERE telegram_id=:i"),{"n":s[:100],"i":u.id})
  ctx.user_data["rename"]=False;await m.reply_text("✅ Name updated.",reply_markup=kb(l));return
 M={"🎮 Play Now":"play","🎮 አሁን ይጫወቱ":"play","💰 Check Balance":"balance","💰 ቀሪ ሂሳብ":"balance","➕ Make Deposit":"deposit","➕ ገንዘብ ያስገቡ":"deposit","🆘 Support":"support","🆘 ድጋፍ":"support","📖 Instructions":"help","📖 መመሪያ":"help","🏆 Win Patterns":"patterns","🏆 የማሸነፊያ ቅጦች":"patterns","✏️ Change Username":"rename","✏️ ስም ይቀይሩ":"rename","👥 Invite Friends":"invite","👥 ጓደኞችን ይጋብዙ":"invite","🥇 Leaderboard":"leaders","🥇 ደረጃ ሰንጠረዥ":"leaders","📜 Transaction History":"history","📜 የግብይት ታሪክ":"history","🌐 Change Language":"language","🌐 ቋንቋ ይቀይሩ":"language"}
 a=M.get(s)
 if a=="play":await m.reply_text("🎮 Open Mini App",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Open Cartela Bingo",web_app=WebAppInfo(url=WEB))]]))
 elif a=="balance":
  async with engine.connect() as c:b=(await c.execute(text("SELECT balance FROM bingo_wallets WHERE telegram_id=:i"),{"i":u.id})).scalar_one_or_none() or 0
  await m.reply_text(f"💰 {b} Birr",reply_markup=kb(l))
 elif a=="deposit":await m.reply_text("Create a request in Wallet. Balance is credited only after a trusted payment record matches.",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💳 Open Wallet",web_app=WebAppInfo(url=WEB+"?tab=wallet"))]]))
 elif a=="support":await m.reply_text(SUPPORT or "Support contact not configured.",reply_markup=kb(l))
 elif a=="help":await m.reply_text("Choose a cartela, join the lobby, watch numbers marked automatically, then press BINGO. The server verifies the claim.",reply_markup=kb(l))
 elif a=="patterns":await m.reply_text("🏆 Horizontal, vertical, or diagonal line. Center is free.",reply_markup=kb(l))
 elif a=="rename":ctx.user_data["rename"]=True;await m.reply_text("Send your display name.")
 elif a=="invite":
  me=await ctx.bot.get_me();await m.reply_text(f"👥 Your personal invite link:\nhttps://t.me/{me.username}?start=ref_{u.id}",reply_markup=kb(l))
 elif a=="language":await setlang(u.id,"am" if l=="en" else "en");await sendwel(up,"am" if l=="en" else "en")
 elif a in ("leaders","history"):
  async with engine.connect() as c:
   if a=="leaders":r=await c.execute(text("SELECT COALESCE(NULLIF(MAX(u.display_name),''),NULLIF(MAX(u.username),''),'Player'),COUNT(DISTINCT g.id) FROM bingo_games g JOIN bingo_users u ON u.telegram_id=g.winner_telegram_id WHERE g.winner_telegram_id IS NOT NULL GROUP BY u.telegram_id ORDER BY COUNT(DISTINCT g.id) DESC LIMIT 10"));rows=r.fetchall();out="🏆 Leaderboard\n"+("\n".join(f"{i+1}. {x[0]} — {x[1]}" for i,x in enumerate(rows)) if rows else "No winners yet.")
   else:r=await c.execute(text("SELECT transaction_type,amount,status FROM bingo_transactions WHERE telegram_id=:i ORDER BY id DESC LIMIT 10"),{"i":u.id});rows=r.fetchall();out="📜 Transactions\n"+("\n".join(f"{x[0]} · {x[1]} · {x[2]}" for x in rows) if rows else "No transactions yet.")
  await m.reply_text(out,reply_markup=kb(l))
def auth(init):
 if not BOT or not init:raise HTTPException(401,"Open Mini App inside Telegram.")
 try:
  d=dict(urllib.parse.parse_qsl(init,keep_blank_values=True));h=d.pop("hash");check="\n".join(f"{k}={v}" for k,v in sorted(d.items()));secret=hmac.new(b"WebAppData",BOT.encode(),hashlib.sha256).digest()
  if not hmac.compare_digest(hmac.new(secret,check.encode(),hashlib.sha256).hexdigest(),h) or abs(time.time()-int(d.get("auth_date","0")))>86400:raise ValueError()
  return json.loads(d["user"])
 except Exception:raise HTTPException(401,"Telegram authentication failed. Reopen the app.")
async def who(req):
 u=auth(req.headers.get("X-Telegram-Init-Data",""));await reg(int(u["id"]),u.get("username"),u.get("first_name"))
 async with engine.connect() as c:
  phone=(await c.execute(text("SELECT phone_number FROM bingo_users WHERE telegram_id=:i"),{"i":int(u["id"])})).scalar_one_or_none()
 if not phone:raise HTTPException(403,"Please register your phone number with the bot first: send /start and share your own contact.")
 return u
def newcard():
 cols=[random.sample(range(a,b+1),5) for a,b in [(1,15),(16,30),(31,45),(46,60),(61,75)]];g=[[cols[c][r] for c in range(5)] for r in range(5)];g[2][2]=0;return g
def winline(card,called):
 m=set(called)|{0};lines=[[(r,c) for c in range(5)] for r in range(5)]+[[(r,c) for r in range(5)] for c in range(5)]+[[(i,i) for i in range(5)],[(i,4-i) for i in range(5)]]
 return next(([[r,c] for r,c in ln] for ln in lines if all(card[r][c] in m for r,c in ln)),None)
async def loop():
 while True:
  await asyncio.sleep(1)
  try:
   async with lock:
    now=time.time()
    if g["status"]=="waiting" and g["players"]:
     if g["lobby_started_at"] is None: g["lobby_started_at"]=now
     g["seconds"]=max(0,math.ceil(30-(now-g["lobby_started_at"])))
     if now-g["lobby_started_at"]>=30:
      g["status"]="running";g["seconds"]=0;g["seq"]=[n for n in range(1,76) if n not in set(g["called"])];random.shuffle(g["seq"]);g["last"]=now
      async with engine.begin() as c: await c.execute(text("UPDATE bingo_games SET status='running',started_at=NOW() WHERE id=:i AND status='waiting'"),{"i":g["id"]})
      logger.info("Game #%s started with %s players",g["id"],len({p.get("user_id") for p in g["players"].values()}))
    elif g["status"]=="running" and now-g["last"]>=1.25:
     if g["seq"]:
      g["called"].append(g["seq"].pop(0));g["last"]=now
      async with engine.begin() as c: await c.execute(text("UPDATE bingo_games SET called_numbers=CAST(:n AS jsonb) WHERE id=:i AND status='running'"),{"n":json.dumps(g["called"]),"i":g["id"]})
     else:
      g["status"]="finished";g["finished_at_ts"]=now
      async with engine.begin() as c: await c.execute(text("UPDATE bingo_games SET status='finished',finished_at=NOW(),called_numbers=CAST(:n AS jsonb) WHERE id=:i"),{"n":json.dumps(g["called"]),"i":g["id"]})
    elif g["status"]=="finished" and now-(g.get("finished_at_ts") or now)>=5:
     async with engine.begin() as c: gid=(await c.execute(text("INSERT INTO bingo_games(status) VALUES('waiting') RETURNING id"))).scalar_one()
     g.update(id=gid,status="waiting",seconds=30,called=[],players={},winner=None,seq=[],last=0,lobby_started_at=None,finished_at_ts=0)
     logger.info("New lobby opened: game #%s",gid)
  except asyncio.CancelledError: raise
  except Exception: logger.exception("Game loop error")
@asynccontextmanager
async def lifespan(app):
 global bot,ticker
 await dbinit();ticker=asyncio.create_task(loop())
 if BOT:
  bot=Application.builder().token(BOT).build();bot.add_handler(CommandHandler("start",start));bot.add_handler(MessageHandler(filters.CONTACT,contact));bot.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND,bottext));bot.add_error_handler(telegram_error_handler);await bot.initialize();await bot.bot.delete_webhook(drop_pending_updates=False);await bot.start()
  if bot.updater: await bot.updater.start_polling(drop_pending_updates=False)
  logger.info("Telegram polling started successfully")
 else: logger.error("TELEGRAM_BOT_TOKEN is missing")
 yield
 if ticker:ticker.cancel()
 if bot:
  if bot.updater:await bot.updater.stop()
  await bot.stop();await bot.shutdown()
 if engine:await engine.dispose()
async def telegram_error_handler(update,context): logger.error("Telegram update processing failed",exc_info=context.error)
app=FastAPI(title="Cartela Bingo",lifespan=lifespan);app.mount("/static",StaticFiles(directory=str(STATIC)),name="static")
@app.get("/")
async def root():return {"app":"Cartela Bingo","mini_app":"/app","health":"/health"}
@app.get("/app",response_class=HTMLResponse)
async def mini():return HTMLResponse((STATIC/"index.html").read_text(encoding="utf-8"))
@app.get("/health")
async def health():return {"status":"ok"}
@app.get("/health/database")
async def healthdb():
 async with engine.connect() as c:await c.execute(text("SELECT 1"))
 return {"status":"connected"}
@app.get("/api/me")
async def me(req:Request):
 u=await who(req);i=int(u["id"])
 async with engine.connect() as c:
  row=(await c.execute(text("SELECT display_name,language,phone_number FROM bingo_users WHERE telegram_id=:i"),{"i":i})).first();bal=(await c.execute(text("SELECT balance FROM bingo_wallets WHERE telegram_id=:i"),{"i":i})).scalar_one_or_none() or 0;refs=(await c.execute(text("SELECT COUNT(*) FROM bingo_referrals WHERE referrer_telegram_id=:i"),{"i":i})).scalar_one()
 uname=(await bot.bot.get_me()).username if bot else "YOUR_BOT_USERNAME"
 return {"id":i,"name":(row[0] if row else None) or u.get("first_name","Player"),"username":u.get("username"),"language":row[1] if row else "en","phone_number":row[2] if row else None,"balance":float(bal),"referrals":refs,"referral_link":f"https://t.me/{uname}?start=ref_{i}"}
@app.post("/api/language")
async def language(req:Request):
 u=await who(req);l=(await req.json()).get("language")
 if l not in ("en","am"):raise HTTPException(400,"Invalid language")
 await setlang(int(u["id"]),l);return {"ok":True}
@app.get("/api/game")
async def getgame(req:Request):
 u=await who(req);uid=int(u["id"])
 async with lock:
  # PostgreSQL is the shared source of truth for cartela reservations. Refresh on
  # every poll so all players see reservations made by other users immediately.
  async with engine.connect() as c:
   state=(await c.execute(text("SELECT status,called_numbers,lobby_started_at,winner_telegram_id FROM bingo_games WHERE id=:g"),{"g":g["id"]})).first()
   if state:
    g["status"]=state[0]
    g["called"]=state[1] or []
    if state[2] and g["lobby_started_at"] is None:g["lobby_started_at"]=state[2].timestamp()
    pr=await c.execute(text("SELECT p.telegram_id,COALESCE(NULLIF(u.display_name,''),NULLIF(u.username,''),'Player'),p.cartela_number,p.cartela,p.stake FROM bingo_game_entries p LEFT JOIN bingo_users u ON u.telegram_id=p.telegram_id WHERE p.game_id=:g ORDER BY p.cartela_number"),{"g":g["id"]})
    fresh={}
    for x in pr.fetchall():
     card=x[3] if isinstance(x[3],list) else json.loads(x[3] or '[]')
     fresh[f"{int(x[0])}:{int(x[2])}"]={"user_id":int(x[0]),"name":x[1],"n":int(x[2]),"card":card,"stake":float(x[4] or 0)}
    g["players"]=fresh
  if g["status"]=="waiting" and g["lobby_started_at"] is not None:
   g["seconds"]=max(0,math.ceil(30-(time.time()-g["lobby_started_at"])))
  entries=list(g["players"].values())
  mine=[{"cartela":p["card"],"cartela_number":p["n"],"stake":p["stake"]} for p in entries if p.get("user_id")==uid]
  users={p.get("user_id") for p in entries}
  total_bets=sum((Decimal(str(p["stake"])) for p in entries),Decimal("0.00"))
  prize=(total_bets*Decimal("0.80")).quantize(Decimal("0.01"))
  return {"id":g["id"],"status":g["status"],"lobby_seconds":g["seconds"],"called":g["called"],"players":[{"telegram_id":p.get("user_id"),"name":p["name"],"cartela_number":p["n"],"stake":p["stake"]} for p in entries],"player_count":len(users),"total_bets":float(total_bets),"prize_amount":float(prize),"owner_share":float((total_bets-prize).quantize(Decimal("0.01"))),"my_cartelas":mine,"my_cartela":mine[0]["cartela"] if mine else None,"my_cartela_number":mine[0]["cartela_number"] if mine else None,"winner":g["winner"]}
class Join(BaseModel):
 cartela_numbers:list[int]=Field(default_factory=list)
 cartela_number:Optional[int]=Field(default=None,ge=1,le=200)
 stake:Decimal=Field(ge=Decimal("30"),le=Decimal("1000"))
@app.post("/api/game/join")
async def join(b:Join,req:Request):
 u=await who(req);uid=int(u["id"])
 nums=list(dict.fromkeys(b.cartela_numbers or ([b.cartela_number] if b.cartela_number else [])))
 if not nums: raise HTTPException(400,"Select at least one cartela.")
 if len(nums)>200 or any(n<1 or n>200 for n in nums): raise HTTPException(400,"Cartela numbers must be between 1 and 200.")
 total=(b.stake*len(nums)).quantize(Decimal("0.01"))
 async with lock:
  if g["status"]!="waiting": raise HTTPException(409,"Lobby closed; wait for the next game.")
  if g["lobby_started_at"] is not None and time.time()-g["lobby_started_at"]>=30: raise HTTPException(409,"The 30-second lobby has closed. Please join the next game.")
  already={p["n"] for p in g["players"].values() if p.get("user_id")==uid}
  nums=[n for n in nums if n not in already]
  if not nums: return {"ok":True,"cartelas":[{"cartela_number":p["n"],"cartela":p["card"]} for p in g["players"].values() if p.get("user_id")==uid],"lobby_seconds":g["seconds"]}
  total=(b.stake*len(nums)).quantize(Decimal("0.01"))
  cards={n:newcard() for n in nums}
  try:
   async with engine.begin() as c:
    game_row=(await c.execute(text("SELECT status,lobby_started_at FROM bingo_games WHERE id=:g FOR UPDATE"),{"g":g["id"]})).first()
    if not game_row or game_row[0]!="waiting": raise HTTPException(409,"Lobby closed; wait for the next game.")
    params={"g":g["id"]}; placeholders=[]
    for ix,n in enumerate(nums):params[f"n{ix}"]=n;placeholders.append(f":n{ix}")
    taken=(await c.execute(text("SELECT cartela_number FROM bingo_game_entries WHERE game_id=:g AND cartela_number IN ("+",".join(placeholders)+")"),params)).scalars().all()
    if taken: raise HTTPException(409,"Cartela(s) just taken: "+", ".join(map(str,taken))+". Refresh and choose available numbers.")
    bal=(await c.execute(text("SELECT balance FROM bingo_wallets WHERE telegram_id=:i FOR UPDATE"),{"i":uid})).scalar_one_or_none() or 0
    pending=(await c.execute(text("SELECT COALESCE(SUM(amount),0) FROM withdrawal_requests WHERE telegram_id=:i AND status='pending'"),{"i":uid})).scalar_one() or 0
    available=Decimal(str(bal))-Decimal(str(pending))
    if available<total: raise HTTPException(402,f"Insufficient available balance. {len(nums)} cartela(s) at {b.stake} Birr each cost {total} Birr.")
    await c.execute(text("UPDATE bingo_wallets SET balance=balance-:a,updated_at=NOW() WHERE telegram_id=:i"),{"a":total,"i":uid})
    await c.execute(text("INSERT INTO bingo_transactions(telegram_id,transaction_type,amount,status,description) VALUES(:i,'bet',:a,'verified',:d)"),{"i":uid,"a":-total,"d":f"Bingo bet: {len(nums)} cartela(s) at {b.stake} Birr each"})
    for n,card in cards.items():
     await c.execute(text("INSERT INTO bingo_game_entries(game_id,telegram_id,cartela_number,cartela,stake) VALUES(:g,:i,:n,CAST(:c AS jsonb),:s)"),{"g":g["id"],"i":uid,"n":n,"c":json.dumps(card),"s":b.stake})
    await c.execute(text("UPDATE bingo_games SET lobby_started_at=COALESCE(lobby_started_at,NOW()) WHERE id=:g"),{"g":g["id"]})
  except IntegrityError:
   raise HTTPException(409,"A selected cartela was just taken. Refresh and choose again.")
  for n,card in cards.items():g["players"][f"{uid}:{n}"]={"user_id":uid,"name":u.get("first_name","Player"),"n":n,"card":card,"stake":float(b.stake)}
  if g["lobby_started_at"] is None:g["lobby_started_at"]=time.time()
  g["seconds"]=max(0,math.ceil(30-(time.time()-g["lobby_started_at"])))
  return {"ok":True,"cartelas":[{"cartela_number":n,"cartela":cards[n]} for n in nums],"lobby_seconds":g["seconds"],"total_stake":float(total)}
@app.post("/api/game/claim")
async def claim(req:Request):
 u=await who(req);uid=int(u["id"])
 try:
  body=await req.json();number=int(body.get("cartela_number"));marked={int(x) for x in body.get("marked",[]) if str(x).isdigit()}
 except Exception:raise HTTPException(400,"Choose the cartela you want to claim with.")
 async with lock:
  if g["status"]!="running":raise HTTPException(409,"Game is not running.")
  p=g["players"].get(f"{uid}:{number}")
  if not p:raise HTTPException(400,"That cartela is not yours in this game.")
  line=winline(p["card"],g["called"])
  if not line:raise HTTPException(400,"No valid line on this cartela yet.")
  valid_marked=(marked & set(g["called"])) | {0}
  lines=[[(r,c) for c in range(5)] for r in range(5)]+[[(r,c) for r in range(5)] for c in range(5)]+[[(i,i) for i in range(5)],[(i,4-i) for i in range(5)]]
  marked_line=next(([[r,c] for r,c in ln] for ln in lines if all(p["card"][r][c] in valid_marked for r,c in ln)),None)
  if not marked_line:raise HTTPException(400,"Mark every called number in a complete horizontal, vertical, or diagonal line first.")
  if g["winner"]:raise HTTPException(409,"Another player already won.")
  entries=list(g["players"].values());pot=sum((Decimal(str(x["stake"])) for x in entries),Decimal("0.00"))
  prize=(pot*Decimal("0.80")).quantize(Decimal("0.01"));commission=(pot-prize).quantize(Decimal("0.01"))
  g["winner"]={"telegram_id":uid,"name":p["name"],"cartela_number":p["n"],"pattern":marked_line,"prize":float(prize),"total_bets":float(pot),"commission":float(commission)};g["status"]="finished";g["finished_at_ts"]=time.time()
  async with engine.begin() as c:
   # Atomic conditional update prevents two simultaneous BINGO claims from
   # paying the same round twice, including across app workers/restarts.
   won=(await c.execute(text("UPDATE bingo_games SET status='finished',finished_at=NOW(),winner_telegram_id=:u,called_numbers=CAST(:n AS jsonb) WHERE id=:g AND status='running' AND winner_telegram_id IS NULL RETURNING id"),{"u":uid,"n":json.dumps(g["called"]),"g":g["id"]})).scalar_one_or_none()
   if won is None:
    raise HTTPException(409,"This game has already been won. Refresh for the result.")
   if prize:
    txid=(await c.execute(text("INSERT INTO bingo_transactions(telegram_id,transaction_type,amount,status,description,external_reference) VALUES(:u,'win',:a,'verified',:d,:r) ON CONFLICT(external_reference) DO NOTHING RETURNING id"),{"u":uid,"a":prize,"d":f"Bingo prize (80% of {pot} Birr total bets)","r":f"bingo:game:{g['id']}:winner"})).scalar_one_or_none()
    if txid is not None:
     await c.execute(text("UPDATE bingo_wallets SET balance=balance+:a,updated_at=NOW() WHERE telegram_id=:u"),{"a":prize,"u":uid})
   if commission and OWNER_ID.isdigit():
    owner=int(OWNER_ID)
    await c.execute(text("INSERT INTO bingo_users(telegram_id,display_name) VALUES(:i,'Platform Owner') ON CONFLICT(telegram_id) DO NOTHING"),{"i":owner})
    await c.execute(text("INSERT INTO bingo_wallets(telegram_id) VALUES(:i) ON CONFLICT(telegram_id) DO NOTHING"),{"i":owner})
    await c.execute(text("UPDATE bingo_wallets SET balance=balance+:a,updated_at=NOW() WHERE telegram_id=:i"),{"a":commission,"i":owner})
    await c.execute(text("INSERT INTO bingo_transactions(telegram_id,transaction_type,amount,status,description) VALUES(:i,'platform_commission',:a,'verified',:d)"),{"i":owner,"a":commission,"d":f"20% platform commission from game #{g['id']}"})
   elif commission:
    await c.execute(text("UPDATE bingo_platform_wallet SET balance=balance+:a,updated_at=NOW() WHERE id=1"),{"a":commission})
  return {"ok":True,"pattern":marked_line,"prize":float(prize),"total_bets":float(pot),"platform_commission":float(commission)}
@app.get("/api/wallet")
async def wallet(req:Request):
 u=await who(req);i=int(u["id"])
 async with engine.connect() as c:
  bal=(await c.execute(text("SELECT balance FROM bingo_wallets WHERE telegram_id=:i"),{"i":i})).scalar_one_or_none() or 0
  ds=(await c.execute(text("SELECT id,provider,amount,reference,status,created_at FROM deposit_requests WHERE telegram_id=:i ORDER BY id DESC LIMIT 20"),{"i":i})).fetchall()
  ws=(await c.execute(text("SELECT id,provider,amount,destination,status,created_at FROM withdrawal_requests WHERE telegram_id=:i ORDER BY id DESC LIMIT 20"),{"i":i})).fetchall()
  pending=(await c.execute(text("SELECT COALESCE(SUM(amount),0) FROM withdrawal_requests WHERE telegram_id=:i AND status='pending'"),{"i":i})).scalar_one()
  ts=(await c.execute(text("SELECT transaction_type,amount,status,description,created_at FROM bingo_transactions WHERE telegram_id=:i ORDER BY id DESC LIMIT 20"),{"i":i})).fetchall()
 return {"balance":float(bal),"available_balance":float(Decimal(str(bal))-Decimal(str(pending or 0))),"deposits":[{"id":x[0],"provider":x[1],"amount":float(x[2]),"reference":x[3],"status":x[4],"created_at":x[5].isoformat()} for x in ds],"withdrawals":[{"id":x[0],"provider":x[1],"amount":float(x[2]),"destination":x[3],"status":x[4],"created_at":x[5].isoformat()} for x in ws],"transactions":[{"type":x[0],"amount":float(x[1]),"status":x[2],"description":x[3],"created_at":x[4].isoformat()} for x in ts]}
class Dep(BaseModel):
 provider:str
 amount:Decimal=Field(gt=0,le=Decimal("1000000"))
 message:str=Field(min_length=8,max_length=5000)
 reference:Optional[str]=Field(default=None,max_length=200)

def _checket_verify_sync(payload):
 data=json.dumps(payload).encode("utf-8")
 req=urllib.request.Request(
  "https://api.check.et/api/v1/verify",
  data=data,
  headers={"Content-Type":"application/json","Accept":"application/json",
           "Authorization":f"Bearer {CHECK_ET_API_KEY}","User-Agent":"CartelaBingo/1.0"},
  method="POST"
 )
 with urllib.request.urlopen(req,timeout=45) as resp:
  return json.loads(resp.read(200000).decode("utf-8"))

async def _checket_verify(payload):
 if not CHECK_ET_API_KEY:
  logger.warning("CHECK_ET_API_KEY is not configured; receipt will not be auto-credited")
  return None
 try:
  result=await asyncio.to_thread(_checket_verify_sync,payload)
  return result if isinstance(result,dict) else None
 except urllib.error.HTTPError as exc:
  logger.warning("Check.et verification returned HTTP %s",exc.code)
  return None
 except Exception as exc:
  logger.warning("Check.et verification unavailable: %s",type(exc).__name__)
  return None

def _checket_parts(result):
 data=result.get("data") if result and isinstance(result.get("data"),dict) else {}
 receipt=data.get("receipt") if isinstance(data.get("receipt"),dict) else {}
 return data,receipt,{**data,**receipt}

def _receipt_link(message):
 m=re.search(r"https?://[^\s<>]+",message or "",re.I)
 if not m:return None
 url=m.group(0).rstrip(".,)")
 try:
  parsed=urllib.parse.urlparse(url)
  if parsed.scheme.lower()!="https" or not parsed.hostname:return None
  host=parsed.hostname.lower()
  if host in ("localhost","127.0.0.1","0.0.0.0") or host.endswith(".local"):return None
  return url
 except Exception:return None

def _verification_amount(data):
 receipt=data.get("receipt") if isinstance(data.get("receipt"),dict) else {}
 value=_field(receipt,"amount","transactionAmount","transaction_amount")
 if value is None:value=_field(data,"amount","transactionAmount","transaction_amount")
 try:return Decimal(str(value).replace(",","")).quantize(Decimal("0.01"))
 except Exception:return None

def parse_sms_amount(message:str):
 # Used for friendly diagnostics only; the trusted Check.et amount is authoritative.
 patterns=[
  r"(?i)(?:amount|paid|payment|received|credited|deposit(?:ed)?|transfer(?:red)?)\s*(?:is|of|:|=)?\s*(?:ETB|Birr|ብር)?\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)\s*(?:ETB|Birr|ብር)",
  r"(?i)(?:ETB|Birr|ብር)\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)",
  r"(?i)([0-9][0-9,]*(?:\.[0-9]{1,2})?)\s*(?:ETB|Birr|ብር)"
 ]
 for pattern in patterns:
  m=re.search(pattern,message)
  if m:
   try:return Decimal(m.group(1).replace(',','')).quantize(Decimal('0.01'))
   except Exception:pass
 return None

def parse_sms_reference(message:str):
 patterns=[
  r"(?i)(?:transaction|txn|trans(?:action)?\s*id|reference|ref(?:erence)?\s*(?:no|number|#)?|receipt(?:\s*(?:no|number|#))?)\s*[:#= -]+([A-Z0-9-]{5,})",
  r"ቁጥርዎ\s*[:#= -]*([A-Z0-9-]{5,})",
  r"(?i)receipt/([A-Z0-9-]{5,})"
 ]
 for pattern in patterns:
  m=re.search(pattern,message)
  if m:return m.group(1).strip()[:200]
 return None

ALLOWED_RECIPIENTS={
 "0943912235":"Nahom Million",
 "0937189718":"Tewdros Getu",
 "0934417096":"Tinsae Merga",
}
CBE_RECIPIENTS={
 "1000261740528":"Nahom Million",
 "1000216646472":"Tewodros Getu",
}
MPESA_RECEIVER_NUMBERS={re.sub(r"\D","",x) for x in os.getenv("MPESA_RECEIVER_NUMBERS","").split(",") if re.sub(r"\D","",x)}

def _field(data,*names):
 for name in names:
  value=data.get(name)
  if value not in (None,""):return value
 return None

def _field_digits(data,*names):
 value=_field(data,*names)
 return re.sub(r"\D","",str(value or ""))

def _phone_receiver_matches(data, allowed=None):
 allowed=allowed if allowed is not None else set(ALLOWED_RECIPIENTS)
 digits=_field_digits(data,"receiverAccount","receiver_account","receiverPhone","receiver_phone","recipientAccount","recipientPhone","receiver")
 if digits and any(digits.endswith(re.sub(r"\D","",phone)[-9:]) for phone in allowed):return True
 # Some Check.et provider responses return only the receiver's display name.
 name=str(_field(data,"receiverName","receiver_name","recipientName","recipient_name") or "").strip().casefold()
 return bool(name and any(name==person.casefold() for person in ALLOWED_RECIPIENTS.values())) if allowed is None or set(allowed)==set(ALLOWED_RECIPIENTS) else False

def _cbe_receiver_matches(data):
 account=re.sub(r"\D","",str(data.get("_verified_receiver_account") or ""))
 if account in CBE_RECIPIENTS:return True
 digits=_field_digits(data,"receiverAccount","receiver_account","recipientAccount","receiver")
 if digits and any(digits.endswith(acc[-4:]) for acc in CBE_RECIPIENTS):return True
 name=str(_field(data,"receiverName","receiver_name","recipientName","recipient_name") or "").strip().casefold()
 return bool(name and any(name==person.casefold() for person in CBE_RECIPIENTS.values()))

def _receiver_matches(data):
 name=str(_field(data,"receiverName","receiver_name","recipientName","recipient_name") or "").strip().casefold()
 return _phone_receiver_matches(data) or bool(name and any(name==recipient.casefold() for recipient in ALLOWED_RECIPIENTS.values()))

def _verification_reference(result, entered_reference, message):
 data,receipt,combined=_checket_parts(result)
 value=_field(data,"transaction_number","reference","transaction_reference","external_reference","id")
 if value is None:value=_field(receipt,"transaction_number","reference","transaction_reference","transaction_id")
 if value is None:value=entered_reference or parse_sms_reference(message)
 if value:
  return re.sub(r"[^A-Za-z0-9._:-]","",str(value))[:200]
 # Stable fallback lets retries of the same URL/text refer to the same pending request.
 link=_receipt_link(message)
 seed=link or (message or "").strip()
 return "checket-"+hashlib.sha256(seed.encode("utf-8")).hexdigest()[:40]

async def _credit_verified_deposit(uid,provider,amount,reference,payment_message):
 """Atomically mark one deposit verified and credit the wallet exactly once."""
 ledger_ref=f"deposit:{provider}:{reference}"
 async with engine.begin() as c:
  prior=(await c.execute(text("SELECT id FROM bingo_transactions WHERE external_reference=:r"),{"r":ledger_ref})).scalar_one_or_none()
  if prior is not None:return False
  existing=(await c.execute(text("SELECT id,telegram_id,amount,status FROM deposit_requests WHERE provider=:p AND reference=:r ORDER BY id LIMIT 1 FOR UPDATE"),{"p":provider,"r":reference})).first()
  if existing is None:
   existing=(await c.execute(text("SELECT id,telegram_id,amount,status FROM deposit_requests WHERE provider=:p AND telegram_id=:u AND amount=:a AND status='pending' AND payment_message=:m ORDER BY id LIMIT 1 FOR UPDATE"),{"p":provider,"u":uid,"a":amount,"m":payment_message[:5000]})).first()
   if existing:
    await c.execute(text("UPDATE deposit_requests SET reference=:r WHERE id=:i AND status='pending'"),{"r":reference,"i":existing[0]})
  if existing and int(existing[1])!=uid:
   raise HTTPException(409,"This payment reference is already associated with another account. No wallet credit was made.")
  if existing and Decimal(str(existing[2])).quantize(Decimal("0.01"))!=amount:
   raise HTTPException(409,"This payment reference was submitted with a different amount. No wallet credit was made.")
  if existing and str(existing[3]).lower()=="verified":
   raise HTTPException(409,"This payment reference is already marked verified. No duplicate wallet credit was made.")
  if existing:
   rid=int(existing[0])
   await c.execute(text("UPDATE deposit_requests SET status='verified',verified_at=NOW(),payment_message=:m,parsed_message_amount=:a WHERE id=:i AND status='pending'"),{"m":payment_message[:5000],"a":amount,"i":rid})
  else:
   rid=(await c.execute(text("INSERT INTO deposit_requests(telegram_id,provider,amount,reference,status,payment_message,parsed_message_amount,verified_at) VALUES(:u,:p,:a,:r,'verified',:m,:a,NOW()) RETURNING id"),{"u":uid,"p":provider,"a":amount,"r":reference,"m":payment_message[:5000]})).scalar_one()
  txid=(await c.execute(text("INSERT INTO bingo_transactions(telegram_id,transaction_type,amount,status,description,external_reference) VALUES(:u,'deposit',:a,'verified',:d,:r) ON CONFLICT(external_reference) DO NOTHING RETURNING id"),{"u":uid,"a":amount,"d":f"Automatically verified {provider} deposit #{rid} via Check.et","r":ledger_ref})).scalar_one_or_none()
  if txid is None:
   raise HTTPException(409,"This payment reference was already credited or is being processed. No duplicate credit was made.")
  await c.execute(text("UPDATE bingo_wallets SET balance=balance+:a,updated_at=NOW() WHERE telegram_id=:u"),{"a":amount,"u":uid})
 return True

async def _verify_deposit_receipt(provider, reference, message):
 """Ask Check.et to verify the receipt using the selected payment provider."""
 provider=provider.lower()
 receipt_url=_receipt_link(message)
 if provider=="ebirr":
  return None,{},"Check.et's documented API does not currently list eBirr. The request remains pending; no wallet credit was made."
 if provider not in ("telebirr","cbe_bank","cbe_birr","mpesa"):
  return None,{},"This payment type is not supported by Check.et. The request remains pending; no wallet credit was made."
 if not CHECK_ET_API_KEY:
  return None,{},"CHECK_ET_API_KEY is missing on the server. The request remains pending; no wallet credit was made."

 bank={"telebirr":"telebirr","cbe_bank":"cbe","cbe_birr":"cbebirr","mpesa":"mpesa"}[provider]
 if provider=="cbe_bank":
  # CBE requires the full receiving account; test only the two configured accounts.
  last=None
  for account in CBE_RECIPIENTS:
   payload={"bank":bank,"account_number":account}
   if receipt_url:payload["receipt_url"]=receipt_url
   elif reference:payload["transaction_number"]=reference
   else:payload["receipt_text"]=message
   candidate=await _checket_verify(payload)
   last=candidate
   if candidate and candidate.get("success") is True and candidate.get("exists") is True:
    data,receipt,combined=_checket_parts(candidate)
    combined["_verified_receiver_account"]=account
    return candidate,combined,None
  data,receipt,combined=_checket_parts(last)
  return last,combined,None

 payload={"bank":bank}
 if receipt_url:payload["receipt_url"]=receipt_url
 elif reference:payload["transaction_number"]=reference
 else:payload["receipt_text"]=message
 result=await _checket_verify(payload)
 data,receipt,combined=_checket_parts(result)
 return result,combined,None

@app.post("/api/deposits")
async def deposit(b:Dep,req:Request):
 u=await who(req);p=b.provider.lower()
 if p not in PROVIDERS:raise HTTPException(400,"Unknown provider.")
 message=b.message.strip()
 if not message:raise HTTPException(400,"Paste the official receipt link or full payment confirmation SMS.")
 entered=b.amount.quantize(Decimal("0.01"))
 lookup_ref=(b.reference or parse_sms_reference(message) or "").strip()[:200]
 ref=lookup_ref or _verification_reference(None,None,message)
 uid=int(u["id"])
 # A URL/SMS/reference is only input to Check.et; it is never proof on its own.
 verified,data,unsupported_reason=await _verify_deposit_receipt(p,lookup_ref,message)
 data=data or {}
 receipt=data.get("receipt") if isinstance(data.get("receipt"),dict) else {}
 combined={**data,**receipt}
 if verified and verified.get("success") is True and verified.get("exists") is True and verified.get("duplicate") is not True:
  verified_amount=_verification_amount(data)
  currency=str(_field(receipt,"currency","currencyCode","currency_code") or _field(data,"currency","currencyCode","currency_code") or "").upper()
  receipt_status=str(_field(receipt,"status","payment_status","state") or "").strip().lower()
  if verified_amount != entered or currency not in ("ETB","BIRR"):
   raise HTTPException(400,f"Check.et verified {verified_amount} {currency or 'unknown currency'}, but your entered amount was {entered} Birr. No wallet credit was made; correct the amount and submit again.")
  elif receipt_status and receipt_status not in ("completed","success","successful","paid","confirmed"):
   status_message="Check.et found the transaction, but its status is not completed. No wallet credit was made."
  else:
   receiver_ok=False
   if p=="cbe_bank":
    receiver_ok=_cbe_receiver_matches(data)
   elif p=="telebirr":
    receiver_ok=_receiver_matches(data)
   elif p=="cbe_birr":
    receiver_ok=_phone_receiver_matches(data)
   elif p=="mpesa":
    receiver_ok=bool(MPESA_RECEIVER_NUMBERS and _phone_receiver_matches(data,MPESA_RECEIVER_NUMBERS))
   if not receiver_ok:
    if p=="mpesa" and not MPESA_RECEIVER_NUMBERS:
     status_message="Check.et verified a transaction, but the approved M-Pesa receiving number is not configured. No wallet credit was made."
    else:
     status_message="Check.et verified a transaction, but the receipt does not confirm an approved receiving account. No wallet credit was made."
   else:
    ref=_verification_reference(verified,lookup_ref,message)
    try:
     credited=await _credit_verified_deposit(uid,p,entered,ref,message)
    except HTTPException:raise
    if not credited:raise HTTPException(409,"This transaction reference was already processed. No duplicate credit was made.")
    return {"ok":True,"status":"verified","reference":ref,"parsed_amount":float(verified_amount),"message":"Check.et independently verified the payment, the amount, and the approved recipient. Your Cartela Bingo wallet has been credited."}
 elif verified and verified.get("duplicate") is True:
  status_message="Check.et reports that this receipt was already verified before. No wallet credit was made."
 elif not verified:
  status_message=unsupported_reason or "Check.et could not be reached or returned no verification. The request remains pending; no wallet credit was made."
 else:
  status_message=unsupported_reason or "Check.et could not confirm this transaction. The request remains pending; no wallet credit was made."
 async with engine.begin() as c:
  exists=(await c.execute(text("SELECT id,telegram_id,amount,status FROM deposit_requests WHERE provider=:p AND reference=:r ORDER BY id LIMIT 1"),{"p":p,"r":ref})).first()
  if exists is not None:
   if int(exists[1]) != uid:
    raise HTTPException(409,"This reference is already associated with another account. No wallet credit was made.")
   return {"ok":True,"id":exists[0],"status":exists[3],"reference":ref,"message":status_message}
  rid=(await c.execute(text("INSERT INTO deposit_requests(telegram_id,provider,amount,reference,payment_message,parsed_message_amount) VALUES(:i,:p,:a,:r,:m,:pa) RETURNING id"),{"i":uid,"p":p,"a":entered,"r":ref,"m":message,"pa":entered})).scalar_one()
 return {"ok":True,"id":rid,"status":"pending","parsed_amount":float(entered),"reference":ref,"message":status_message}

class Withdrawal(BaseModel):
 provider:str
 amount:Decimal=Field(gt=0,le=Decimal("1000000"))
 destination:str=Field(min_length=5,max_length=100)
@app.post("/api/withdrawals")
async def withdrawal(b:Withdrawal,req:Request):
 u=await who(req);i=int(u["id"]);p=b.provider.lower()
 if p not in ("telebirr","cbe_bank","ebirr","mpesa"):raise HTTPException(400,"Choose Telebirr, CBE Bank, eBirr, or M-Pesa.")
 dest=re.sub(r"[^A-Za-z0-9+@._ -]","",b.destination).strip()
 if p=="cbe_bank":
  if len(re.sub(r"\D","",dest))<8:raise HTTPException(400,"Enter a valid CBE account number.")
 else:
  if len(re.sub(r"\D","",dest))<9:raise HTTPException(400,"Enter a valid phone number.")
 async with engine.begin() as c:
  bal=(await c.execute(text("SELECT balance FROM bingo_wallets WHERE telegram_id=:i FOR UPDATE"),{"i":i})).scalar_one_or_none() or 0
  pending=(await c.execute(text("SELECT COALESCE(SUM(amount),0) FROM withdrawal_requests WHERE telegram_id=:i AND status='pending'"),{"i":i})).scalar_one() or 0
  if Decimal(str(bal))-Decimal(str(pending))<b.amount:raise HTTPException(402,"Insufficient available balance.")
  rid=(await c.execute(text("INSERT INTO withdrawal_requests(telegram_id,provider,amount,destination) VALUES(:i,:p,:a,:d) RETURNING id"),{"i":i,"p":p,"a":b.amount,"d":dest})).scalar_one()
  await c.execute(text("INSERT INTO bingo_transactions(telegram_id,transaction_type,amount,status,description) VALUES(:i,'withdrawal',:a,'pending',:d)"),{"i":i,"a":-b.amount,"d":f"Withdrawal request #{rid} via {p}"})
 return {"ok":True,"id":rid,"status":"pending","message":"Withdrawal request submitted. Funds are reserved while it is pending."}
class Transfer(BaseModel):
 method:str
 recipient:str=Field(min_length=2,max_length=120)
 amount:Decimal=Field(gt=0,le=Decimal("1000000"))
@app.post("/api/transfers")
async def transfer(b:Transfer,req:Request):
 u=await who(req);sender=int(u["id"]);method=b.method.lower();target=b.recipient.strip()
 if method not in ("phone","username"):raise HTTPException(400,"Choose phone or username.")
 async with engine.begin() as c:
  if method=="phone":
   phone=re.sub(r"\D","",target)
   recipient=(await c.execute(text("SELECT telegram_id,COALESCE(display_name,username,'Player'),username FROM bingo_users WHERE RIGHT(COALESCE(phone_number,''),9)=RIGHT(:p,9) AND phone_verified=TRUE LIMIT 1"),{"p":phone})).first()
  else:
   username=target.lstrip("@").lower()
   recipient=(await c.execute(text("SELECT telegram_id,COALESCE(display_name,username,'Player'),username FROM bingo_users WHERE LOWER(username)=:u LIMIT 1"),{"u":username})).first()
  if not recipient:raise HTTPException(404,"No registered player was found with those details.")
  rid=int(recipient[0])
  if rid==sender:raise HTTPException(400,"You cannot transfer to yourself.")
  ids=sorted([sender,rid])
  balances=(await c.execute(text("SELECT telegram_id,balance FROM bingo_wallets WHERE telegram_id IN (:a,:b) ORDER BY telegram_id FOR UPDATE"),{"a":ids[0],"b":ids[1]})).fetchall()
  bm={int(x[0]):Decimal(str(x[1] or 0)) for x in balances}
  pending=(await c.execute(text("SELECT COALESCE(SUM(amount),0) FROM withdrawal_requests WHERE telegram_id=:i AND status='pending'"),{"i":sender})).scalar_one() or 0
  if bm.get(sender,Decimal("0"))-Decimal(str(pending))<b.amount:raise HTTPException(402,"Insufficient available balance.")
  await c.execute(text("UPDATE bingo_wallets SET balance=balance-:a,updated_at=NOW() WHERE telegram_id=:i"),{"a":b.amount,"i":sender})
  await c.execute(text("UPDATE bingo_wallets SET balance=balance+:a,updated_at=NOW() WHERE telegram_id=:i"),{"a":b.amount,"i":rid})
  await c.execute(text("INSERT INTO bingo_transactions(telegram_id,transaction_type,amount,status,description) VALUES(:i,'transfer_sent',:a,'verified',:d),(:r,'transfer_received',:a,'verified',:rd)"),{"i":sender,"r":rid,"a":b.amount,"d":f"Transfer to {recipient[1]}","rd":f"Transfer from Telegram user {sender}"})
 return {"ok":True,"recipient":{"name":recipient[1],"username":recipient[2]},"amount":float(b.amount)}
class Payment(BaseModel):
 provider:str
 external_reference:str=Field(min_length=2,max_length=200)
 amount:Decimal=Field(gt=0)
 payer_reference:Optional[str]=None
 memo:Optional[str]=None
 receiver_account:Optional[str]=None
@app.post("/api/payments/records")
async def payment(b:Payment,x_payment_api_key:str=Header(default="")):
 if not PAYKEY or not secrets.compare_digest(x_payment_api_key,PAYKEY):raise HTTPException(401,"Invalid payment integration key.")
 p=b.provider.lower()
 if p not in PROVIDERS:raise HTTPException(400,"Unknown provider.")
 # Fail closed: only accept trusted payment records whose receiving destination
 # is one of the configured accounts. A pasted SMS is not a trusted record.
 receiver_digits=re.sub(r"\D","",b.receiver_account or "")
 allowed_phone_suffixes={"943912235","937189718","934417096"}
 if p in ("telebirr","ebirr"):
  if not receiver_digits or receiver_digits[-9:] not in allowed_phone_suffixes:
   raise HTTPException(400,"Receiver must be one of the configured Telebirr/eBirr phone recipients; no wallet credit was made.")
 elif p in ("cbe_bank","cbe_birr"):
  if not receiver_digits or not any(receiver_digits.endswith(suffix) for suffix in ("0528", "6472")):
   raise HTTPException(400,"Receiver must be one of the configured CBE accounts ending 0528 or 6472; no wallet credit was made.")
 elif p=="mpesa":
  raise HTTPException(400,"M-Pesa receiving details are not configured; no wallet credit was made.")
 async with engine.begin() as c:
  inserted=(await c.execute(text("INSERT INTO payment_records(provider,external_reference,amount,payer_reference,memo) VALUES(:p,:r,:a,:pr,:m) ON CONFLICT(external_reference) DO NOTHING RETURNING id"),{"p":p,"r":b.external_reference,"a":b.amount,"pr":b.payer_reference,"m":((b.memo or "") + " | receiver=" + (b.receiver_account or ""))[:1000]})).scalar_one_or_none()
  existing=(await c.execute(text("SELECT matched_request_id FROM payment_records WHERE external_reference=:r FOR UPDATE"),{"r":b.external_reference})).first()
  if existing and existing[0] is not None:
   return {"ok":True,"matched":True,"duplicate":True,"request_id":existing[0],"message":"This payment reference was already processed; no second wallet credit was made."}
  x=(await c.execute(text("SELECT id,telegram_id,amount,reference FROM deposit_requests WHERE provider=:p AND status='pending' AND amount=:a AND (reference=:r OR reference=:pr OR POSITION(reference IN COALESCE(:m,''))>0) ORDER BY id LIMIT 1 FOR UPDATE"),{"p":p,"a":b.amount,"r":b.external_reference,"pr":b.payer_reference,"m":b.memo or ""})).first()
  if x:
   matched=(await c.execute(text("UPDATE payment_records SET matched_request_id=:i WHERE external_reference=:r AND matched_request_id IS NULL RETURNING id"),{"i":x[0],"r":b.external_reference})).scalar_one_or_none()
   if matched is None:
    return {"ok":True,"matched":False,"duplicate":True,"message":"This payment reference was already handled; no wallet credit was made."}
   await c.execute(text("UPDATE deposit_requests SET status='verified',verified_at=NOW() WHERE id=:i AND status='pending'"),{"i":x[0]})
   await c.execute(text("UPDATE bingo_wallets SET balance=balance+:a WHERE telegram_id=:u"),{"a":x[2],"u":x[1]})
   await c.execute(text("INSERT INTO bingo_transactions(telegram_id,transaction_type,amount,status,description,external_reference) VALUES(:u,'deposit',:a,'verified',:d,:r) ON CONFLICT(external_reference) DO NOTHING"),{"u":x[1],"a":x[2],"d":f"Verified {p} deposit #{x[0]}","r":f"payment:{b.external_reference}"})
   return {"ok":True,"matched":True,"request_id":x[0]}
 return {"ok":True,"matched":False,"message":"Stored but no exact pending request matched."}
@app.get("/api/leaderboard")
async def leaderboard(req:Request):
 await who(req)
 async with engine.connect() as c:
  r=await c.execute(text("SELECT u.telegram_id,COALESCE(NULLIF(MAX(u.display_name),''),NULLIF(MAX(u.username),''),'Player'),COUNT(DISTINCT g.id) AS wins FROM bingo_games g JOIN bingo_users u ON u.telegram_id=g.winner_telegram_id WHERE g.winner_telegram_id IS NOT NULL GROUP BY u.telegram_id ORDER BY wins DESC,MIN(g.finished_at) ASC NULLS LAST LIMIT 50"));rows=r.fetchall()
 return {"players":[{"telegram_id":int(x[0]),"name":x[1],"wins":int(x[2])} for x in rows]}
@app.websocket("/ws/game")
async def ws(ws:WebSocket):
 await ws.accept()
 try:
  while True:
   async with lock:
    if g["status"]=="waiting" and g["lobby_started_at"] is not None:g["seconds"]=max(0,math.ceil(30-(time.time()-g["lobby_started_at"])))
    d={"id":g["id"],"status":g["status"],"lobby_seconds":g["seconds"],"called":g["called"],"player_count":len({p.get("user_id") for p in g["players"].values()}),"winner":g["winner"]}
   await ws.send_json(d);await asyncio.sleep(1)
 except (WebSocketDisconnect,RuntimeError):pass
