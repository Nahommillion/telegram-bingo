import os,json,asyncio,random,hmac,hashlib,urllib.parse,time,secrets,logging,math,re,urllib.request,urllib.error,html
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
 "CREATE TABLE IF NOT EXISTS bingo_referrals(id BIGSERIAL PRIMARY KEY,referrer_telegram_id BIGINT,referred_telegram_id BIGINT UNIQUE,created_at TIMESTAMPTZ DEFAULT NOW())",
 "ALTER TABLE bingo_referrals ADD COLUMN IF NOT EXISTS referrer_telegram_id BIGINT",
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
    async with engine.begin() as c:await c.execute(text("INSERT INTO bingo_referrals(referrer_telegram_id,referred_telegram_id) VALUES(:r,:u) ON CONFLICT(referred_telegram_id) DO NOTHING"),{"r":r,"u":u.id})
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
 message:Optional[str]=Field(default=None,max_length=5000)
 reference:Optional[str]=Field(default=None,max_length=300)

def normalize_digits(value:str)->str:
 # Normalize Ethiopic/Arabic-Indic numerals if a provider uses them.
 trans={}
 for base in (0x0660,0x06F0):
  for i in range(10): trans[chr(base+i)]=str(i)
 return (value or "").translate(str.maketrans(trans))

def parse_sms_amount(message:str):
 message=normalize_digits(message or "")
 patterns=[
  r"(?i)(?:amount|paid|payment|received|credited|deposit(?:ed)?|transfer(?:red)?)\s*(?:is|of|:|=)?\s*(?:ETB|Birr|ብር)?\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)\s*(?:ETB|Birr|ብር)",
  r"([0-9][0-9,]*(?:\.[0-9]{1,2})?)\s*ብር\s*በቀን",
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
 message=normalize_digits(message or "")
 patterns=[r"(?i)(?:transaction|txn|trans(?:action)?\s*id|reference|ref(?:erence)?\s*(?:no|number|#)?|receipt(?:\s*(?:no|number|#))?)\s*[:#= -]+([A-Z0-9-]{5,})", r"ቁጥርዎ\s*[:：#= -]*([A-Z0-9]{8,14})"]
 for pattern in patterns:
  m=re.search(pattern,message)
  if m:return m.group(1).strip()[:200]
 return None

def extract_telebirr_reference(value:str)->Optional[str]:
 """Accept a raw receipt reference or a URL on the fixed official receipt host only."""
 value=(value or '').strip()
 if not value:return None
 if value.lower().startswith(('https://','http://')):
  try:
   u=urllib.parse.urlsplit(value)
   if u.scheme!='https' or u.hostname!='transactioninfo.ethiotelecom.et' or u.port not in (None,443):return None
   m=re.fullmatch(r'/receipt/([A-Za-z0-9]{5,40})/?',u.path)
   return m.group(1) if m else None
  except Exception:return None
 return value if re.fullmatch(r'[A-Za-z0-9]{5,40}',value) else None

class _ReceiptTextParser(__import__('html.parser',fromlist=['HTMLParser']).HTMLParser):
 def __init__(self):
  super().__init__();self.parts=[];self.hidden=0
 def handle_starttag(self,tag,attrs):
  if tag in ('script','style','noscript'):self.hidden+=1
 def handle_endtag(self,tag):
  if tag in ('script','style','noscript') and self.hidden:self.hidden-=1
 def handle_data(self,data):
  if not self.hidden:self.parts.append(data)

def _fetch_telebirr_receipt(ref:str):
 """Conservative public receipt parser. It returns data only when fields are explicit."""
 url='https://transactioninfo.ethiotelecom.et/receipt/'+urllib.parse.quote(ref,safe='')
 class SafeRedirect(urllib.request.HTTPRedirectHandler):
  def redirect_request(self,req,fp,code,msg,headers,newurl):
   u=urllib.parse.urlsplit(newurl)
   if u.scheme!='https' or u.hostname!='transactioninfo.ethiotelecom.et':return None
   return super().redirect_request(req,fp,code,msg,headers,newurl)
 opener=urllib.request.build_opener(SafeRedirect)
 req=urllib.request.Request(url,headers={'User-Agent':'CartelaBingo-ReceiptCheck/1.0','Accept':'text/html'})
 try:
  with opener.open(req,timeout=8) as response:
   if response.geturl().split('/')[2].lower()!='transactioninfo.ethiotelecom.et':return None
   raw=response.read(1_000_001)
   if len(raw)>1_000_000:return None
   charset=response.headers.get_content_charset() or 'utf-8'
   page=raw.decode(charset,errors='replace')
 except Exception as exc:
  logger.info('Telebirr receipt could not be fetched (%s)',type(exc).__name__)
  return None
 parser=_ReceiptTextParser()
 try:parser.feed(page)
 except Exception:return None
 visible=' '.join(html.unescape(x) for x in parser.parts)
 visible=normalize_digits(re.sub(r'\s+',' ',visible)).strip()
 if not visible or ref.lower() not in visible.lower():return None
 low=visible.lower()
 success=('successful' in low or 'successfully' in low or 'completed' in low or 'በተሳካ ሁኔታ' in visible)
 failure=any(x in low for x in ('transaction failed','status: failed','unsuccessful','pending'))
 if not success or failure:return None
 # Extract only an explicitly labelled transaction amount; never use fees or balance as fallback.
 amount_patterns=[
  r'(?i)(?:transaction\s+amount|transfer\s+amount|payment\s+amount|amount\s+transferred|amount)\s*[:：-]?\s*(?:ETB|Birr|ብር)?\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)\s*(?:ETB|Birr|ብር)?',
  r'(?:የተላለፈው\s*መጠን|የክፍያ\s*መጠን)\s*[:：-]?\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)\s*(?:ETB|Birr|ብር)?'
 ]
 amount=None
 for pat in amount_patterns:
  m=re.search(pat,visible)
  if m:
   try:amount=Decimal(m.group(1).replace(',','')).quantize(Decimal('0.01'));break
   except Exception:pass
 if amount is None:return None
 digits=re.sub(r'\D','',visible)
 # Require an exact authorized recipient to be present in the receipt text.
 cbe='1000261740528' in digits
 telebirr_phone='0943912235' in digits or '251943912235' in digits
 if not (cbe or telebirr_phone):return None
 return {'amount':amount,'destination':'cbe_bank' if cbe else 'telebirr','receipt_text':visible[:5000]}

def extract_cbe_receipt_url(value:str)->Optional[str]:
 """Accept only CBE's public receipt host, never arbitrary URLs."""
 value=(value or "").strip().rstrip(".,;)")
 if not value:return None
 try:
  u=urllib.parse.urlsplit(value)
  if u.scheme!="https" or u.hostname!="mbreciept.cbe.com.et" or u.port not in (None,443) or u.username or u.password:return None
  if not re.fullmatch(r"/[A-Za-z0-9_-]{8,100}\.?",u.path):return None
  path=u.path.rstrip(".")
  return urllib.parse.urlunsplit(("https","mbreciept.cbe.com.et",path,u.query,""))
 except Exception:return None

def _cheki_verify(payload:dict)->dict:
 """Call the public Cheki verifier; this is a third-party scraper, not a bank API."""
 data=json.dumps(payload).encode("utf-8")
 request=urllib.request.Request("https://cheki.et/api/verify",data=data,headers={"Content-Type":"application/json","Accept":"application/json","User-Agent":"CartelaBingo/1.0"},method="POST")
 try:
  with urllib.request.urlopen(request,timeout=12) as response:
   raw=response.read(250001)
   if len(raw)>250000:return {"success":False,"error":"Verifier response was too large."}
   parsed=json.loads(raw.decode("utf-8"))
   return parsed if isinstance(parsed,dict) else {"success":False,"error":"Invalid verifier response."}
 except Exception as exc:
  logger.warning("Cheki receipt verification failed (%s)",type(exc).__name__)
  return {"success":False,"error":"The free receipt verifier is temporarily unreachable. No balance was changed."}

def _field(data:dict,*names):
 for name in names:
  value=data.get(name)
  if value not in (None,""):
   return str(value)
 return ""

def _destination_matches(provider:str,data:dict)->bool:
 """Require the verified receipt's receiver field to show an allowed destination suffix."""
 if provider=="telebirr":
  receiver=_field(data,"creditedPartyAccountNumber","creditedPartyAccount","receiverAccount","receiverPhone","creditedPartyPhone","receiverAccountNumber","receiverMsisdn","receiverNumber")
  allowed=("0528","2235") # CBE account ending 0528 or Telebirr number ending 2235
 elif provider=="cbe_bank":
  receiver=_field(data,"receiverAccount","receiverAccountNumber","creditedPartyAccountNumber","creditedPartyAccount")
  allowed=("0528",)
 else:return False
 digits=re.sub(r"\D","",receiver)
 return bool(digits and any(digits.endswith(suffix) for suffix in allowed))

async def verify_deposit_reference(provider:str,reference:str,amount:Decimal,request_id:int,telegram_id:int)->dict:
 """Verify via Cheki and credit once only when success, amount and destination all match."""
 if provider=="telebirr":
  ref=extract_telebirr_reference(reference)
  if not ref:return {"verified":False,"reason":"Enter a Telebirr transaction reference or official receipt URL."}
  payload={"bank":"telebirr","reference":ref}
  provider_label="telebirr"
  request_ref=ref
 elif provider=="cbe_bank":
  url=extract_cbe_receipt_url(reference)
  if not url:return {"verified":False,"reason":"For CBE, paste the new receipt link from the CBE app (mbreciept.cbe.com.et). An old FT reference alone cannot be verified by this free checker."}
  payload={"reference":url}
  provider_label="cbe_bank"
  request_ref=url
 else:
  return {"verified":False,"reason":"This provider is not supported by the free verifier yet. No wallet credit was made."}
 data=await asyncio.to_thread(_cheki_verify,payload)
 # success means the verifier request worked; verified must independently be true.
 if data.get("success") is not True or data.get("verified") is not True:
  reason=str(data.get("error") or data.get("message") or "The verifier could not confirm this receipt.")
  return {"verified":False,"reason":reason+" No wallet credit was made."}
 try:
  verified_amount=Decimal(str(data.get("amount"))).quantize(Decimal("0.01"))
 except Exception:
  return {"verified":False,"reason":"The verifier did not return a valid transaction amount. No wallet credit was made."}
 if str(data.get("currency","ETB")).upper() not in ("ETB","BIRR"):
  return {"verified":False,"reason":"The receipt currency is not ETB. No wallet credit was made."}
 if verified_amount!=amount.quantize(Decimal("0.01")):
  return {"verified":False,"reason":f"Verified receipt amount is {verified_amount} Birr, not {amount} Birr. No wallet credit was made."}
 if not _destination_matches(provider,data):
  logger.warning("Verified %s receipt did not expose a matching authorized receiver; keys=%s",provider,sorted(data.keys()))
  return {"verified":False,"reason":"The receipt was found, but the verifier did not show a receiver account matching your authorized destination ending 0528 or Telebirr phone ending 2235. It remains pending; no wallet credit was made."}
 external_ref=_field(data,"reference","transactionNumber","transactionId") or (extract_telebirr_reference(reference) if provider=="telebirr" else request_ref)
 if not re.fullmatch(r"[A-Za-z0-9._:-]{5,200}",external_ref):
  return {"verified":False,"reason":"The verifier returned an invalid transaction reference. No wallet credit was made."}
 # CBE links can change shape; use the bank's transaction reference for duplicate protection.
 unique_ref=external_ref if provider=="telebirr" else "cbe:"+external_ref
 async with engine.begin() as c:
  # A verified reference must be globally unique in our ledger. Insert is the concurrency gate.
  inserted=(await c.execute(text("INSERT INTO payment_records(provider,external_reference,amount,payer_reference,memo) VALUES(:p,:r,:a,NULL,:m) ON CONFLICT(external_reference) DO NOTHING RETURNING id"),{"p":provider_label,"r":unique_ref,"a":verified_amount,"m":"Verified by free Cheki public receipt checker; receiver suffix matched"})).scalar_one_or_none()
  if inserted is None:
   return {"verified":False,"reason":"This transaction reference has already been submitted or credited. No second wallet credit was made."}
  already=(await c.execute(text("SELECT id FROM bingo_transactions WHERE external_reference=:r"),{"r":"payment:"+unique_ref})).scalar_one_or_none()
  if already is not None:
   await c.execute(text("DELETE FROM payment_records WHERE external_reference=:r"),{"r":unique_ref})
   return {"verified":False,"reason":"This reference already exists in the wallet ledger. No second credit was made."}
  claimed=(await c.execute(text("UPDATE deposit_requests SET status='verified',verified_at=NOW() WHERE id=:id AND telegram_id=:u AND status='pending' AND amount=:a RETURNING id"),{"id":request_id,"u":telegram_id,"a":amount})).scalar_one_or_none()
  if claimed is None:
   await c.execute(text("DELETE FROM payment_records WHERE external_reference=:r"),{"r":unique_ref})
   return {"verified":False,"reason":"The deposit request could not be safely matched. No wallet credit was made."}
  matched=(await c.execute(text("UPDATE payment_records SET matched_request_id=:i WHERE external_reference=:r AND matched_request_id IS NULL RETURNING id"),{"i":request_id,"r":unique_ref})).scalar_one_or_none()
  if matched is None:
   raise RuntimeError("Payment reference could not be claimed atomically")
  await c.execute(text("INSERT INTO bingo_transactions(telegram_id,transaction_type,amount,status,description,external_reference) VALUES(:u,'deposit',:a,'verified',:d,:r)"),{"u":telegram_id,"a":amount,"d":f"Cheki-verified {provider_label} deposit #{request_id}","r":"payment:"+unique_ref})
  await c.execute(text("UPDATE bingo_wallets SET balance=balance+:a,updated_at=NOW() WHERE telegram_id=:u"),{"a":amount,"u":telegram_id})
 return {"verified":True,"reason":"Receipt verified, amount and authorized receiver matched, and wallet credited once."}

@app.post("/api/deposits")
async def deposit(b:Dep,req:Request):
 u=await who(req);p=b.provider.lower()
 if p not in PROVIDERS:raise HTTPException(400,"Unknown provider.")
 message=(b.message or '').strip()
 raw=(b.reference or '').strip()
 if not raw and message:
  sms_ref=parse_sms_reference(message)
  if sms_ref:raw=sms_ref
 if p=="telebirr":
  ref=extract_telebirr_reference(raw)
  if not ref and raw and not raw.lower().startswith(("http://","https://")) and re.fullmatch(r"[A-Za-z0-9-]{5,40}",raw):ref=raw
 elif p=="cbe_bank":
  ref=extract_cbe_receipt_url(raw) or (raw if not raw.lower().startswith(("http://","https://")) and re.fullmatch(r"[A-Za-z0-9-]{5,200}",raw) else None)
 else:
  ref=raw if raw and not raw.lower().startswith(("http://","https://")) and re.fullmatch(r"[A-Za-z0-9-]{5,200}",raw) else None
 if not message and not ref:raise HTTPException(400,"Enter a transaction reference or the provider's receipt link.")
 parsed=parse_sms_amount(message) if message else None
 entered=b.amount.quantize(Decimal('0.01'))
 if message and parsed is None:raise HTTPException(400,"I could not find a clear Birr amount in that SMS. Paste the full confirmation SMS or submit the receipt reference alone.")
 if parsed is not None and parsed != entered:raise HTTPException(400,f"Amount mismatch: the SMS appears to show {parsed} Birr, but you entered {entered} Birr.")
 if not ref:ref='SMS-'+hashlib.sha256(message.encode()).hexdigest()[:16]
 if p in ("telebirr","cbe_bank") and not ref.startswith("SMS-"):
  # Fast duplicate pre-check for a friendly message; the unique DB constraint remains authoritative.
  unique_hint=ref if p=="telebirr" else None
  if unique_hint:
   async with engine.connect() as c:
    old=(await c.execute(text("SELECT matched_request_id FROM payment_records WHERE external_reference=:r"),{"r":unique_hint})).first()
   if old:
    raise HTTPException(409,"This payment reference has already been submitted or credited. It cannot be used again.")
 async with engine.begin() as c:
  rid=(await c.execute(text("INSERT INTO deposit_requests(telegram_id,provider,amount,reference,payment_message,parsed_message_amount) VALUES(:i,:p,:a,:r,:m,:pa) RETURNING id"),{'i':int(u['id']),'p':p,'a':entered,'r':ref,'m':message or None,'pa':parsed})).scalar_one()
 if p in ("telebirr","cbe_bank") and not ref.startswith("SMS-"):
  result=await verify_deposit_reference(p,ref,entered,int(rid),int(u['id']))
  if result['verified']:
   return {'ok':True,'id':rid,'status':'verified','parsed_amount':float(entered),'reference':ref,'message':result['reason']}
  return {'ok':True,'id':rid,'status':'pending','parsed_amount':float(parsed) if parsed is not None else None,'reference':ref,'message':result['reason']}
 return {'ok':True,'id':rid,'status':'pending','parsed_amount':float(parsed) if parsed is not None else None,'reference':ref,'message':'Request saved. This provider cannot be reliably checked by the free verifier yet, so the wallet remains unchanged until trusted confirmation.'}

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
 provider:str;external_reference:str=Field(min_length=2,max_length=200);amount:Decimal=Field(gt=0);payer_reference:Optional[str]=None;memo:Optional[str]=None
@app.post("/api/payments/records")
async def payment(b:Payment,x_payment_api_key:str=Header(default="")):
 if not PAYKEY or not secrets.compare_digest(x_payment_api_key,PAYKEY):raise HTTPException(401,"Invalid payment integration key.")
 p=b.provider.lower()
 if p not in PROVIDERS:raise HTTPException(400,"Unknown provider.")
 async with engine.begin() as c:
  inserted=(await c.execute(text("INSERT INTO payment_records(provider,external_reference,amount,payer_reference,memo) VALUES(:p,:r,:a,:pr,:m) ON CONFLICT(external_reference) DO NOTHING RETURNING id"),{"p":p,"r":b.external_reference,"a":b.amount,"pr":b.payer_reference,"m":b.memo})).scalar_one_or_none()
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
