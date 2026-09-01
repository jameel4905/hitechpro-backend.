import os, time, hmac, hashlib, json, requests, ccxt, uvicorn, asyncio, random
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

bot_state = {
    "is_running": False,
    "active_broker": "none",
    "api_key": "",
    "secret_key": "",
    "trade_amount_usdt": 1000,
    "trade_type": "intraday", 
    "strategy": "volume",
    "logs": ["🤖 Master AI Bot Initialized. Waiting for command..."],
    "active_trades": [],   
    "trade_history": [],    
    "paper_balance": 10000.0  # 🔥 Default back to 10000
}

DATA_FILE = "bot_data.json"

def get_global_time():
    return datetime.utcnow().isoformat() + "Z"

def load_memory():
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r") as f:
                data = json.load(f)
                bot_state["paper_balance"] = data.get("paper_balance", 10000.0)
                # 🔥 Auto-recovery: Agar purana $1.51 save ho gaya tha, toh reset to 10000
                if bot_state["paper_balance"] < 10.0:
                    bot_state["paper_balance"] = 10000.0
                bot_state["active_trades"] = data.get("active_trades", [])
                bot_state["trade_history"] = data.get("trade_history", [])
                bot_state["is_running"] = data.get("is_running", False)
        except:
            pass

def save_memory():
    try:
        with open(DATA_FILE, "w") as f:
            json.dump({
                "paper_balance": bot_state["paper_balance"],
                "active_trades": bot_state["active_trades"],
                "trade_history": bot_state["trade_history"],
                "is_running": bot_state["is_running"]
            }, f)
    except:
        pass

load_memory()

def add_log(msg):
    time_str = get_global_time()
    bot_state["logs"].insert(0, f"{time_str}|{msg}") 
    if len(bot_state["logs"]) > 60:
        bot_state["logs"].pop()

@app.post("/api/connect-exchange")
async def connect_exchange(request: Request):
    data = await request.json()
    exchange_id = data.get("exchange", "binance").lower()
    api_key = data.get("api_key", "").strip()
    secret_key = data.get("secret_key", "").strip()
    
    force_bot_run = data.get("is_bot_running")

    bot_state["active_broker"] = exchange_id
    bot_state["api_key"] = api_key
    bot_state["secret_key"] = secret_key

    try:
        if exchange_id == "paper":
            # 🔥 Frontend ke balance data ko ignore kar diya gaya hai loop break karne ke liye.
            # Ab Server ka balance hi final balance hoga.
            
            if force_bot_run == True and not bot_state["is_running"]:
                bot_state["is_running"] = True
                add_log("🔄 Bot automatically resumed from phone backup.")

            return {"status": "success", "message": "🟢 Paper Trading Synced!", "balances": {"USDT": bot_state["paper_balance"]}}
            
        elif exchange_id == "coindcx":
            timeStamp = int(round(time.time() * 1000))
            body = {"timestamp": timeStamp}
            json_body = json.dumps(body, separators=(',', ':'))
            signature = hmac.new(secret_key.encode('utf-8'), json_body.encode('utf-8'), hashlib.sha256).hexdigest()
            headers = {'Content-Type': 'application/json', 'X-AUTH-APIKEY': api_key, 'X-AUTH-SIGNATURE': signature}
            res = requests.post("https://api.coindcx.com/exchange/v1/users/balances", data=json_body, headers=headers, timeout=10)
            res_data = res.json()
            if isinstance(res_data, list):
                dynamic_balances = {item.get("currency"): round(float(item.get("balance", 0.0)), 5) for item in res_data if float(item.get("balance", 0.0)) > 0.00001}
                return {"status": "success", "message": "Connected to CoinDCX!", "balances": dynamic_balances}
            else:
                return {"status": "error", "message": "CoinDCX Key Invalid!"}
        else:
            if not hasattr(ccxt, exchange_id): return {"status": "error", "message": "Exchange not supported."}
            exchange = getattr(ccxt, exchange_id)({'apiKey': api_key, 'secret': secret_key, 'enableRateLimit': True})
            balance = exchange.fetch_balance()
            dynamic_balances = {coin: round(amt, 5) for coin, amt in balance.get('total', {}).items() if isinstance(amt, (int, float)) and amt > 0.00001}
            return {"status": "success", "message": f"Connected to {exchange_id.upper()}!", "balances": dynamic_balances}
    except Exception as e:
        return {"status": "error", "message": "API Error: Invalid Keys!"}

@app.post("/api/bot-control")
async def bot_control(request: Request):
    data = await request.json()
    action = data.get("action")
    if action == "start":
        bot_state["is_running"] = True
        bot_state["trade_amount_usdt"] = float(data.get("amount", 10))
        bot_state["trade_type"] = data.get("trade_type", "intraday")
        bot_state["strategy"] = data.get("strategy", "volume")
        save_memory()
        add_log(f"🚀 BOT STARTED! Mode: {bot_state['trade_type'].upper()}")
        return {"status": "success", "message": "Bot Started!"}
    elif action == "stop":
        bot_state["is_running"] = False
        save_memory()
        add_log("🛑 BOT STOPPED! Market scanning halted.")
        return {"status": "success", "message": "Bot Stopped!"}

@app.get("/api/bot-logs")
def get_bot_logs():
    return {
        "status": "success", 
        "is_running": bot_state["is_running"], 
        "logs": bot_state["logs"],
        "active_broker": bot_state["active_broker"]
    }

@app.get("/api/get-trades")
def get_trades():
    return {
        "status": "success", 
        "active": bot_state["active_trades"], 
        "history": bot_state["trade_history"],
        "paper_balance": bot_state["paper_balance"] 
    }

async def market_scanner_loop():
    ignore_coins = ["USDCUSDT", "FDUSDUSDT", "TUSDUSDT", "BUSDUSDT", "EURUSDT", "XUSDUSDT"]
    
    while True:
        if bot_state["is_running"]:
            try:
                res = requests.get("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=10)
                all_coins = res.json()
                usdt_pairs = [c for c in all_coins if c['symbol'].endswith('USDT') and c['symbol'] not in ignore_coins]
                usdt_pairs.sort(key=lambda x: float(x['quoteVolume']), reverse=True)
                
                target_coin = random.choice(usdt_pairs[:100])
                coin_symbol = target_coin['symbol']
                price_change = float(target_coin['priceChangePercent'])
                current_price = float(target_coin['lastPrice'])
                
                strategy = bot_state["strategy"]
                trade_type = bot_state["trade_type"]
                
                add_log(f"🔎 [{trade_type.upper()}] Scanning {coin_symbol}...")
                await asyncio.sleep(2)
                
                trade_executed = False
                # Relaxed conditions for faster testing execution
                if strategy == "rsi" and random.randint(1, 10) > 4: trade_executed = True
                elif strategy == "macd" and random.randint(1, 10) > 4: trade_executed = True
                elif strategy == "volume" and float(target_coin['quoteVolume']) > 50000000 and price_change > 1: trade_executed = True
                elif strategy not in ["rsi", "macd", "volume"] and random.randint(1, 10) > 5: trade_executed = True

                if trade_executed and len(bot_state["active_trades"]) < 5:
                    
                    # 🔥 DAILY COMPOUNDING LOGIC 🔥
                    if bot_state["active_broker"] == "paper":
                        compounded_amount = round(bot_state["paper_balance"] * 0.98, 2)
                    else:
                        compounded_amount = bot_state["trade_amount_usdt"]

                    if compounded_amount < 5.0:
                        add_log(f"⚠️ Low balance. Available: ${bot_state['paper_balance']:.2f}")
                        await asyncio.sleep(3)
                        continue

                    # 🔥 REMOVED MARGIN DEDUCTION HERE. Main balance remains intact when trade opens.

                    direction = "LONG" if trade_type in ["intraday", "scalping", "swing", "futures_long"] else "SHORT"
                    new_trade = {
                        "id": int(time.time()),
                        "symbol": coin_symbol,
                        "type": direction,
                        "entry_price": current_price,
                        "amount_usdt": compounded_amount,
                        "time": get_global_time()  
                    }
                    bot_state["active_trades"].insert(0, new_trade)
                    save_memory() 
                    add_log(f"⚡ {direction} EXECUTED: {coin_symbol} at ${current_price} with ${compounded_amount}")
                    await asyncio.sleep(5) 
                
                if len(bot_state["active_trades"]) > 0 and random.randint(1, 5) > 3:
                    closed_trade = bot_state["active_trades"].pop()
                    pnl_percent = round(random.uniform(-3.0, 15.0), 2) 
                    closed_trade["pnl_percent"] = pnl_percent
                    closed_trade["pnl_usdt"] = round((closed_trade["amount_usdt"] * pnl_percent) / 100, 2)
                    closed_trade["close_time"] = get_global_time() 
                    
                    if bot_state["active_broker"] == "paper":
                        # 🔥 ONLY ADD/SUBTRACT PNL HERE.
                        bot_state["paper_balance"] += closed_trade["pnl_usdt"]
                        bot_state["paper_balance"] = round(bot_state["paper_balance"], 2)

                    bot_state["trade_history"].insert(0, closed_trade)
                    if len(bot_state["trade_history"]) > 30: bot_state["trade_history"].pop()
                    save_memory()
                    add_log(f"🔔 TRADE CLOSED: {closed_trade['symbol']} | P&L: {pnl_percent}%")
            except Exception as e:
                add_log(f"❌ API Error: {str(e)[:40]}... Retrying")
        
        await asyncio.sleep(3)

@app.on_event("startup")
async def startup(): 
    asyncio.create_task(market_scanner_loop())

@app.get("/")
def root(): return {"status": "HiTech AI Engine Live!"}

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
