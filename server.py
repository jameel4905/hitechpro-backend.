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
    "paper_balance": 10000.0,
    "today_pnl": 0.0, 
    "last_settlement_date": datetime.utcnow().strftime("%Y-%m-%d") 
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
                if bot_state["paper_balance"] < 10.0:
                    bot_state["paper_balance"] = 10000.0
                bot_state["active_trades"] = data.get("active_trades", [])
                bot_state["trade_history"] = data.get("trade_history", [])
                bot_state["is_running"] = data.get("is_running", False)
                bot_state["trade_type"] = data.get("trade_type", "intraday")
                bot_state["strategy"] = data.get("strategy", "volume")
                bot_state["today_pnl"] = data.get("today_pnl", 0.0)
                bot_state["last_settlement_date"] = data.get("last_settlement_date", datetime.utcnow().strftime("%Y-%m-%d"))
        except:
            pass

def save_memory():
    try:
        with open(DATA_FILE, "w") as f:
            json.dump({
                "paper_balance": bot_state["paper_balance"],
                "active_trades": bot_state["active_trades"],
                "trade_history": bot_state["trade_history"],
                "is_running": bot_state["is_running"],
                "trade_type": bot_state["trade_type"],
                "strategy": bot_state["strategy"],
                "today_pnl": bot_state["today_pnl"],
                "last_settlement_date": bot_state["last_settlement_date"]
            }, f)
    except:
        pass

load_memory()

def add_log(msg):
    time_str = get_global_time()
    bot_state["logs"].insert(0, f"{time_str}|{msg}") 
    if len(bot_state["logs"]) > 60:
        bot_state["logs"].pop()

def check_midnight_settlement():
    current_date = datetime.utcnow().strftime("%Y-%m-%d")
    if current_date != bot_state["last_settlement_date"]:
        bot_state["paper_balance"] += bot_state["today_pnl"]
        bot_state["paper_balance"] = round(bot_state["paper_balance"], 2)
        settled_amount = bot_state["today_pnl"]
        bot_state["today_pnl"] = 0.0
        bot_state["last_settlement_date"] = current_date
        add_log(f"🏦 Midnight Settlement: ${settled_amount} moved to Wallet.")
        save_memory()

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
            if force_bot_run == True and not bot_state["is_running"]:
                bot_state["is_running"] = True
                add_log("🔄 Bot automatically resumed from backup.")
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

# 🔥 NAYA API ENDPOINT: MANUAL TRADE CLOSE KARNE KE LIYE 🔥
@app.post("/api/close-trade")
async def close_trade(request: Request):
    data = await request.json()
    trade_id = data.get("id")
    
    trade_to_close = None
    for t in bot_state["active_trades"]:
        if t["id"] == trade_id:
            trade_to_close = t
            break
            
    if not trade_to_close:
        return {"status": "error", "message": "Trade already closed or not found!"}
        
    try:
        # Binance se ekdum latest live price mangwana manual exit ke waqt
        res = requests.get(f"https://api.binance.com/api/v3/ticker/price?symbol={trade_to_close['symbol']}", timeout=5)
        exit_price = float(res.json()['price'])
        
        if trade_to_close["type"] == "LONG":
            pnl_percent = ((exit_price - trade_to_close["entry_price"]) / trade_to_close["entry_price"]) * 100
        else:
            pnl_percent = ((trade_to_close["entry_price"] - exit_price) / trade_to_close["entry_price"]) * 100
            
        pnl_usdt = (trade_to_close["amount_usdt"] * pnl_percent) / 100
        
        trade_to_close["pnl_percent"] = round(pnl_percent, 2)
        trade_to_close["pnl_usdt"] = round(pnl_usdt, 2)
        trade_to_close["exit_price"] = exit_price
        trade_to_close["close_time"] = get_global_time()
        
        if bot_state["active_broker"] == "paper":
            bot_state["today_pnl"] += trade_to_close["pnl_usdt"]
            bot_state["today_pnl"] = round(bot_state["today_pnl"], 2)

        bot_state["active_trades"].remove(trade_to_close)
        bot_state["trade_history"].insert(0, trade_to_close)
        if len(bot_state["trade_history"]) > 30: bot_state["trade_history"].pop()
        
        add_log(f"🛑 MANUAL EXIT: {trade_to_close['symbol']} | P&L: {trade_to_close['pnl_percent']}%")
        save_memory()
        
        return {"status": "success", "message": f"Manual Exit Successful! PNL: {trade_to_close['pnl_percent']}%"}
    except Exception as e:
        return {"status": "error", "message": "API Error: Could not close trade."}

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
        "paper_balance": bot_state["paper_balance"],
        "today_pnl": bot_state["today_pnl"] 
    }

async def market_scanner_loop():
    ignore_coins = ["USDCUSDT", "FDUSDUSDT", "TUSDUSDT", "BUSDUSDT", "EURUSDT", "XUSDUSDT"]
    
    while True:
        check_midnight_settlement() 
        
        if bot_state["is_running"]:
            try:
                res = requests.get("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=10)
                all_coins = res.json()
                
                live_prices = {c['symbol']: float(c['lastPrice']) for c in all_coins}
                
                trades_to_close = []
                for trade in bot_state["active_trades"]:
                    sym = trade["symbol"]
                    if sym in live_prices:
                        curr_p = live_prices[sym]
                        
                        if trade["type"] == "LONG":
                            if curr_p > trade["highest_price"]:
                                trade["highest_price"] = curr_p
                                trade["sl_price"] = max(trade["sl_price"], curr_p * 0.98)
                            if curr_p <= trade["sl_price"]:
                                trades_to_close.append(trade)
                                
                        elif trade["type"] == "SHORT":
                            if curr_p < trade["lowest_price"]:
                                trade["lowest_price"] = curr_p
                                trade["sl_price"] = min(trade["sl_price"], curr_p * 1.02)
                            if curr_p >= trade["sl_price"]:
                                trades_to_close.append(trade)
                
                for trade in trades_to_close:
                    exit_price = live_prices[trade["symbol"]]
                    
                    if trade["type"] == "LONG":
                        pnl_percent = ((exit_price - trade["entry_price"]) / trade["entry_price"]) * 100
                    else:
                        pnl_percent = ((trade["entry_price"] - exit_price) / trade["entry_price"]) * 100
                        
                    pnl_usdt = (trade["amount_usdt"] * pnl_percent) / 100
                    
                    trade["pnl_percent"] = round(pnl_percent, 2)
                    trade["pnl_usdt"] = round(pnl_usdt, 2)
                    trade["exit_price"] = exit_price
                    trade["close_time"] = get_global_time()
                    
                    if bot_state["active_broker"] == "paper":
                        bot_state["today_pnl"] += trade["pnl_usdt"]
                        bot_state["today_pnl"] = round(bot_state["today_pnl"], 2)

                    bot_state["active_trades"].remove(trade)
                    bot_state["trade_history"].insert(0, trade)
                    if len(bot_state["trade_history"]) > 30: bot_state["trade_history"].pop()
                    
                    add_log(f"🔔 TSL HIT: {trade['symbol']} | P&L: {trade['pnl_percent']}%")
                
                save_memory()

                usdt_pairs = [c for c in all_coins if c['symbol'].endswith('USDT') and c['symbol'] not in ignore_coins]
                usdt_pairs.sort(key=lambda x: float(x['quoteVolume']), reverse=True)
                
                target_coin = random.choice(usdt_pairs[:100])
                coin_symbol = target_coin['symbol']
                price_change = float(target_coin['priceChangePercent'])
                current_price = float(target_coin['lastPrice'])
                
                strategy = bot_state["strategy"]
                trade_type = bot_state["trade_type"]
                
                trade_executed = False
                if strategy == "rsi" and random.randint(1, 10) > 4: trade_executed = True
                elif strategy == "macd" and random.randint(1, 10) > 4: trade_executed = True
                elif strategy == "volume" and float(target_coin['quoteVolume']) > 50000000 and abs(price_change) > 1: trade_executed = True
                elif strategy not in ["rsi", "macd", "volume"] and random.randint(1, 10) > 5: trade_executed = True

                if trade_executed and len(bot_state["active_trades"]) < 5:
                    
                    if bot_state["active_broker"] == "paper":
                        compounded_amount = round(bot_state["paper_balance"] * 0.98, 2)
                    else:
                        compounded_amount = bot_state["trade_amount_usdt"]

                    if compounded_amount < 5.0:
                        add_log(f"⚠️ Low balance. Available: ${bot_state['paper_balance']:.2f}")
                        await asyncio.sleep(3)
                        continue

                    direction = "LONG" if price_change > 0 else "SHORT"
                    initial_sl = current_price * 0.98 if direction == "LONG" else current_price * 1.02

                    new_trade = {
                        "id": int(time.time()),
                        "symbol": coin_symbol,
                        "type": direction,
                        "entry_price": current_price,
                        "amount_usdt": compounded_amount,
                        "highest_price": current_price,
                        "lowest_price": current_price,
                        "sl_price": initial_sl,
                        "time": get_global_time()  
                    }
                    bot_state["active_trades"].insert(0, new_trade)
                    save_memory() 
                    add_log(f"⚡ {direction} EXECUTED: {coin_symbol} at ${current_price}")
                    await asyncio.sleep(5) 
                    
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
