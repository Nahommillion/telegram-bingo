import os,json,asyncio,random,hmac,hashlib,urllib.parse,time,secrets
from contextlib import asynccontextmanager
from pathlib import Path
from decimal import Decimal
from typing import Optional
from fastapi import FastAPI,HTTPException,Header,Request,WebSocket,WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel,Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from telegram import Update,KeyboardButton,ReplyKeyboardMarkup,WebAppInfo,InlineKeyboardMarkup,InlineKeyboardButton
from telegram.ext import Application,CommandHandler,MessageHandler,ContextTypes,filters
BOT=os.getenv("TELEGRAM_BOT_TOKEN","").strip(); DB=os.getenv("DATABASE_URL","").strip()
WEB=os.getenv("WEB_APP_URL","https://telegram-bingo-zq2q.onrender.com/app").strip()
SUPPORT=os.getenv("SUPPORT_URL","").strip(); PAYKEY=os.getenv("PAYMENT_INGEST_API_KEY","").strip()
STATIC=Path(__file__).parent/"static"; STATIC.mkdir(exist_ok=True)
engine=bot=None; ticker=None; lock=asyncio.Lock()
g={"id":1,"status":"waiting","seconds":30,"called":[],"players":{},"winner":None,"seq":[],"last":0}
PROVIDERS={"telebirr","cbe_birr","ebirr","cbe_bank"}
def dsn(x):
 if x.startswith("postgres://"): return x.replace("postgres://","postgresql+asyncpg://",1)
 if x.startswith("postgresql://") and not x.startswith("postgresql+asyncpg://"): return x.replace("postgresql://","postgresql+asyncpg://",1)
 return x
async def dbinit():
 global engine
 if not DB: raise RuntimeError("Set DATABASE_URL in Render.")
 engine=create_async_engine(dsn(DB),pool_pre_ping=True)
 qs=[
 "CREATE TABLE IF NOT EXISTS bingo_users(telegram_id BIGINT PRIMARY KEY,username TEXT,display_name TEXT,language VARCHAR(5) DEFAULT 'en',created_at TIMESTAMPTZ DEFAULT NOW())",
 "CREATE TABLE IF NOT EXISTS bingo_wallets(telegram_id BIGINT PRIMARY KEY REFERENCES bingo_users(telegram_id),balance NUMERIC(14,2) DEFAULT 0,updated_at TIMESTAMPTZ DEFAULT NOW())",
 "CREATE TABLE IF NOT EXISTS bingo_transactions(id BIGSERIAL PRIMARY KEY,telegram_id BIGINT,transaction_type TEXT,amount NUMERIC(14,2),status TEXT DEFAULT 'pending',description TEXT,external_reference TEXT UNIQUE,created_at TIMESTAMPTZ DEFAULT NOW())",
 "CREATE TABLE IF NOT EXISTS bingo_referrals(id BIGSERIAL PRIMARY KEY,referrer_telegram_id BIGINT,referred_telegram_id BIGINT UNIQUE,created_at TIMESTAMPTZ DEFAULT NOW())",
 "CREATE TABLE IF NOT EXISTS deposit_requests(id BIGSERIAL PRIMARY KEY,telegram_id BIGINT,provider TEXT,amount NUMERIC(14,2),reference TEXT,status TEXT DEFAULT 'pending',created_at TIMESTAMPTZ DEFAULT NOW(),verified_at TIMESTAMPTZ)",
 "CREATE TABLE IF NOT EXISTS payment_records(id BIGSERIAL PRIMARY KEY,provider TEXT,external_reference TEXT UNIQUE,amount NUMERIC(14,2),payer_reference TEXT,memo TEXT,matched_request_id BIGINT,created_at TIMESTAMPTZ DEFAULT NOW())",
 "CREATE TABLE IF NOT EXISTS bingo_games(id BIGSERIAL PRIMARY KEY,status TEXT,created_at TIMESTAMPTZ DEFAULT NOW(),started_at TIMESTAMPTZ,finished_at TIMESTAMPTZ,winner_telegram_id BIGINT,called_numbers JSONB DEFAULT '[]'::jsonb)",
 "CREATE TABLE IF NOT EXISTS bingo_game_players(game_id BIGINT,telegram_id BIGINT,cartela_number INT,cartela JSONB,stake NUMERIC(14,2) DEFAULT 0,PRIMARY KEY(game_id,telegram_id),UNIQUE(game_id,cartela_number))"]
 async with engine.begin() as c:
  for q in qs: await c.execute(text(q))
  r=await c.execute(text("SELECT id,status,called_numbers FROM bingo_games WHERE status IN ('waiting','running') ORDER BY id DESC LIMIT 1")); row=r.first()
  if row:g.update(id=row[0],status=row[1],called=row[2] or [])
  else:g["id"]=(await c.execute(text("INSERT INTO bingo_games(status) VALUES('waiting') RETURNING id"))).scalar_one()
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
 return ReplyKeyboardMarkup([[KeyboardButton(x,web_app=WebAppInfo(url=WEB)) if x in ("🎮 Play Now","🎮 አሁን ይጫወቱ") else KeyboardButton(x) for x in r] for r in rows],resize_keyboard=True,one_time_keyboard=False,is_persistent=False)
def wel(l):return ("🎉🎱 እንኳን ወደ Cartela Bingo በደህና መጡ!\n🇪🇹 የኢትዮጵያ ቀለሞች • ቢንጎ • ደስታ\n👇 ከታች ያለውን አዝራር ይምረጡ!" if l=="am" else "🎉🎱 Welcome to Cartela Bingo!\n🇪🇹 Ethiopian colors • Bingo • Fun\n👇 Choose an option below!")
async def sendwel(up,l=None):
 l=l or await langof(up.effective_user.id); p=STATIC/"bingo.PNG"
 if p.exists():await up.effective_message.reply_photo(str(p),caption=wel(l),reply_markup=kb(l))
 else:await up.effective_message.reply_text(wel(l),reply_markup=kb(l))
async def start(up,ctx):
 u=up.effective_user; await reg(u.id,u.username,u.first_name,"am" if (u.language_code or "").startswith("am") else "en")
 if ctx.args and ctx.args[0].startswith("ref_"):
  try:
   r=int(ctx.args[0][4:])
   if r!=u.id:
    async with engine.begin() as c:await c.execute(text("INSERT INTO bingo_referrals(referrer_telegram_id,referred_telegram_id) VALUES(:r,:u) ON CONFLICT(referred_telegram_id) DO NOTHING"),{"r":r,"u":u.id})
  except ValueError:pass
 await sendwel(up)
async def bottext(up,ctx):
 u=up.effective_user;m=up.effective_message;s=(m.text or "").strip();await reg(u.id,u.username,u.first_name);l=await langof(u.id)
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
   if a=="leaders":r=await c.execute(text("SELECT COALESCE(u.display_name,'Player'),COUNT(*) FROM bingo_games g JOIN bingo_users u ON u.telegram_id=g.winner_telegram_id GROUP BY u.display_name ORDER BY COUNT(*) DESC LIMIT 10"));rows=r.fetchall();out="🏆 Leaderboard\n"+("\n".join(f"{i+1}. {x[0]} — {x[1]}" for i,x in enumerate(rows)) if rows else "No winners yet.")
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
 u=auth(req.headers.get("X-Telegram-Init-Data",""));await reg(int(u["id"]),u.get("username"),u.get("first_name"));return u
def newcard():
 cols=[random.sample(range(a,b+1),5) for a,b in [(1,15),(16,30),(31,45),(46,60),(61,75)]];g=[[cols[c][r] for c in range(5)] for r in range(5)];g[2][2]=0;return g
def winline(card,called):
 m=set(called)|{0};lines=[[(r,c) for c in range(5)] for r in range(5)]+[[(r,c) for r in range(5)] for c in range(5)]+[[(i,i) for i in range(5)],[(i,4-i) for i in range(5)]]
 return next(([[r,c] for r,c in ln] for ln in lines if all(card[r][c] in m for r,c in ln)),None)
async def loop():
 while True:
  await asyncio.sleep(1)
  async with lock:
   if g["status"]=="waiting" and g["players"]:
    g["seconds"]-=1
    if g["seconds"]<=0:
     g["status"]="running";g["seq"]=random.sample(range(1,76),75);g["last"]=time.time()
     async with engine.begin() as c:await c.execute(text("UPDATE bingo_games SET status='running',started_at=NOW() WHERE id=:i"),{"i":g["id"]})
   elif g["status"]=="running" and time.time()-g["last"]>=5:
    if g["seq"]:
     g["called"].append(g["seq"].pop(0));g["last"]=time.time()
     async with engine.begin() as c:await c.execute(text("UPDATE bingo_games SET called_numbers=CAST(:n AS jsonb) WHERE id=:i"),{"n":json.dumps(g["called"]),"i":g["id"]})
    else:g["status"]="finished"
   elif g["status"]=="finished":
    async with engine.begin() as c:gid=(await c.execute(text("INSERT INTO bingo_games(status) VALUES('waiting') RETURNING id"))).scalar_one()
    g.update(id=gid,status="waiting",seconds=30,called=[],players={},winner=None,seq=[],last=0)
@asynccontextmanager
async def lifespan(app):
 global bot,ticker
 await dbinit();ticker=asyncio.create_task(loop())
 if BOT:
  bot=Application.builder().token(BOT).build();bot.add_handler(CommandHandler("start",start));bot.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND,bottext));await bot.initialize();await bot.start()
  if bot.updater:await bot.updater.start_polling(drop_pending_updates=False)
 yield
 if ticker:ticker.cancel()
 if bot:
  if bot.updater:await bot.updater.stop()
  await bot.stop();await bot.shutdown()
 if engine:await engine.dispose()
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
  row=(await c.execute(text("SELECT display_name,language FROM bingo_users WHERE telegram_id=:i"),{"i":i})).first();bal=(await c.execute(text("SELECT balance FROM bingo_wallets WHERE telegram_id=:i"),{"i":i})).scalar_one_or_none() or 0;refs=(await c.execute(text("SELECT COUNT(*) FROM bingo_referrals WHERE referrer_telegram_id=:i"),{"i":i})).scalar_one()
 uname=(await bot.bot.get_me()).username if bot else "YOUR_BOT_USERNAME"
 return {"id":i,"name":(row[0] if row else None) or u.get("first_name","Player"),"username":u.get("username"),"language":row[1] if row else "en","balance":float(bal),"referrals":refs,"referral_link":f"https://t.me/{uname}?start=ref_{i}"}
@app.post("/api/language")
async def language(req:Request):
 u=await who(req);l=(await req.json()).get("language")
 if l not in ("en","am"):raise HTTPException(400,"Invalid language")
 await setlang(int(u["id"]),l);return {"ok":True}
@app.get("/api/game")
async def getgame(req:Request):
 u=await who(req)
 async with lock:
  p=g["players"].get(int(u["id"]));return {"id":g["id"],"status":g["status"],"lobby_seconds":g["seconds"],"called":g["called"],"players":[{"telegram_id":i,"name":x["name"],"cartela_number":x["n"]} for i,x in g["players"].items()],"player_count":len(g["players"]),"my_cartela":p["card"] if p else None,"my_cartela_number":p["n"] if p else None,"winner":g["winner"]}
class Join(BaseModel):
 cartela_number:int=Field(ge=1,le=200);stake:Decimal=Field(default=Decimal("0"),ge=0,le=Decimal("1000"))
@app.post("/api/game/join")
async def join(b:Join,req:Request):
 u=await who(req);i=int(u["id"])
 async with lock:
  if g["status"]!="waiting":raise HTTPException(409,"Lobby closed; wait for next game.")
  if i in g["players"]:return {"ok":True}
  if any(p["n"]==b.cartela_number for p in g["players"].values()):raise HTTPException(409,"Cartela already taken.")
  if b.stake:
   async with engine.begin() as c:
    bal=(await c.execute(text("SELECT balance FROM bingo_wallets WHERE telegram_id=:i FOR UPDATE"),{"i":i})).scalar_one()
    if Decimal(str(bal))<b.stake:raise HTTPException(402,"Insufficient balance.")
    await c.execute(text("UPDATE bingo_wallets SET balance=balance-:a WHERE telegram_id=:i"),{"a":b.stake,"i":i})
    await c.execute(text("INSERT INTO bingo_transactions(telegram_id,transaction_type,amount,status,description) VALUES(:i,'bet',:a,'verified','Bingo stake')"),{"i":i,"a":-b.stake})
  card=newcard();g["players"][i]={"name":u.get("first_name","Player"),"n":b.cartela_number,"card":card,"stake":float(b.stake)}
  async with engine.begin() as c:await c.execute(text("INSERT INTO bingo_game_players(game_id,telegram_id,cartela_number,cartela,stake) VALUES(:g,:i,:n,CAST(:c AS jsonb),:s)"),{"g":g["id"],"i":i,"n":b.cartela_number,"c":json.dumps(card),"s":b.stake})
  return {"ok":True,"cartela":card,"cartela_number":b.cartela_number}
@app.post("/api/game/claim")
async def claim(req:Request):
 u=await who(req);i=int(u["id"])
 async with lock:
  if g["status"]!="running":raise HTTPException(409,"Game is not running.")
  p=g["players"].get(i)
  if not p:raise HTTPException(400,"Join first.")
  line=winline(p["card"],g["called"])
  if not line:raise HTTPException(400,"No valid line yet.")
  if g["winner"]:raise HTTPException(409,"Another player already won.")
  g["winner"]={"telegram_id":i,"name":p["name"],"cartela_number":p["n"],"pattern":line};g["status"]="finished";prize=sum(Decimal(str(x["stake"])) for x in g["players"].values())
  async with engine.begin() as c:
   await c.execute(text("UPDATE bingo_games SET status='finished',finished_at=NOW(),winner_telegram_id=:u,called_numbers=CAST(:n AS jsonb) WHERE id=:g"),{"u":i,"n":json.dumps(g["called"]),"g":g["id"]})
   if prize:
    await c.execute(text("UPDATE bingo_wallets SET balance=balance+:a WHERE telegram_id=:u"),{"a":prize,"u":i})
    await c.execute(text("INSERT INTO bingo_transactions(telegram_id,transaction_type,amount,status,description) VALUES(:u,'win',:a,'verified','Bingo prize')"),{"u":i,"a":prize})
  return {"ok":True,"pattern":line,"prize":float(prize)}
@app.get("/api/wallet")
async def wallet(req:Request):
 u=await who(req);i=int(u["id"])
 async with engine.connect() as c:
  bal=(await c.execute(text("SELECT balance FROM bingo_wallets WHERE telegram_id=:i"),{"i":i})).scalar_one_or_none() or 0
  ds=(await c.execute(text("SELECT id,provider,amount,reference,status,created_at FROM deposit_requests WHERE telegram_id=:i ORDER BY id DESC LIMIT 20"),{"i":i})).fetchall()
  ts=(await c.execute(text("SELECT transaction_type,amount,status,description,created_at FROM bingo_transactions WHERE telegram_id=:i ORDER BY id DESC LIMIT 20"),{"i":i})).fetchall()
 return {"balance":float(bal),"deposits":[{"id":x[0],"provider":x[1],"amount":float(x[2]),"reference":x[3],"status":x[4],"created_at":x[5].isoformat()} for x in ds],"transactions":[{"type":x[0],"amount":float(x[1]),"status":x[2],"description":x[3],"created_at":x[4].isoformat()} for x in ts]}
class Dep(BaseModel):
 provider:str;amount:Decimal=Field(gt=0,le=Decimal("1000000"));reference:str=Field(min_length=3,max_length=200)
@app.post("/api/deposits")
async def deposit(b:Dep,req:Request):
 u=await who(req);p=b.provider.lower()
 if p not in PROVIDERS:raise HTTPException(400,"Unknown provider.")
 async with engine.begin() as c:r=await c.execute(text("INSERT INTO deposit_requests(telegram_id,provider,amount,reference) VALUES(:i,:p,:a,:r) RETURNING id"),{"i":int(u["id"]),"p":p,"a":b.amount,"r":b.reference})
 return {"ok":True,"id":r.scalar_one(),"status":"pending"}
class Payment(BaseModel):
 provider:str;external_reference:str=Field(min_length=2,max_length=200);amount:Decimal=Field(gt=0);payer_reference:Optional[str]=None;memo:Optional[str]=None
@app.post("/api/payments/records")
async def payment(b:Payment,x_payment_api_key:str=Header(default="")):
 if not PAYKEY or not secrets.compare_digest(x_payment_api_key,PAYKEY):raise HTTPException(401,"Invalid payment integration key.")
 p=b.provider.lower()
 if p not in PROVIDERS:raise HTTPException(400,"Unknown provider.")
 async with engine.begin() as c:
  await c.execute(text("INSERT INTO payment_records(provider,external_reference,amount,payer_reference,memo) VALUES(:p,:r,:a,:pr,:m) ON CONFLICT(external_reference) DO NOTHING"),{"p":p,"r":b.external_reference,"a":b.amount,"pr":b.payer_reference,"m":b.memo})
  x=(await c.execute(text("SELECT id,telegram_id,amount,reference FROM deposit_requests WHERE provider=:p AND status='pending' AND amount=:a AND (reference=:r OR reference=:pr OR POSITION(reference IN COALESCE(:m,''))>0) ORDER BY id LIMIT 1 FOR UPDATE"),{"p":p,"a":b.amount,"r":b.external_reference,"pr":b.payer_reference,"m":b.memo or ""})).first()
  if x:
   await c.execute(text("UPDATE deposit_requests SET status='verified',verified_at=NOW() WHERE id=:i"),{"i":x[0]});await c.execute(text("UPDATE payment_records SET matched_request_id=:i WHERE external_reference=:r"),{"i":x[0],"r":b.external_reference});await c.execute(text("UPDATE bingo_wallets SET balance=balance+:a WHERE telegram_id=:u"),{"a":x[2],"u":x[1]});await c.execute(text("INSERT INTO bingo_transactions(telegram_id,transaction_type,amount,status,description,external_reference) VALUES(:u,'deposit',:a,'verified',:d,:r) ON CONFLICT(external_reference) DO NOTHING"),{"u":x[1],"a":x[2],"d":f"Verified {p} deposit #{x[0]}","r":b.external_reference});return {"ok":True,"matched":True,"request_id":x[0]}
 return {"ok":True,"matched":False,"message":"Stored but no exact pending request matched."}
@app.get("/api/leaderboard")
async def leaderboard(req:Request):
 await who(req)
 async with engine.connect() as c:r=await c.execute(text("SELECT COALESCE(u.display_name,'Player'),COUNT(*) FROM bingo_games g JOIN bingo_users u ON u.telegram_id=g.winner_telegram_id GROUP BY u.display_name ORDER BY COUNT(*) DESC LIMIT 50"));rows=r.fetchall()
 return {"players":[{"name":x[0],"wins":x[1]} for x in rows]}
@app.websocket("/ws/game")
async def ws(ws:WebSocket):
 await ws.accept()
 try:
  while True:
   async with lock:d={"id":g["id"],"status":g["status"],"lobby_seconds":g["seconds"],"called":g["called"],"player_count":len(g["players"]),"winner":g["winner"]}
   await ws.send_json(d);await asyncio.sleep(1)
 except (WebSocketDisconnect,RuntimeError):pass
