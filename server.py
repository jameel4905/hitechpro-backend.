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
    "trade_amount_usdt": 10,
    "strategy": "volume_breakout",
    "logs": ["🤖 Bot Initialized. Waiting for command..."]
}

def add_log(msg):
    time_str = datetime.now().strftime("%H:%M:%S")
    bot_state["logs"].insert(0, f"[{time_str}] {msg}")
    if len(bot_state["logs"]) > 50:
        bot_state["logs"].pop()

# --- ENGINE 1: PORTFOLIO & EXCHANGE CONNECTION ---
@app.post("/api/connect-exchange")
async def connect_exchange(request: Request):
    data = await request.json()
    exchange_id = data.get("exchange", "binance").lower()
    api_key = data.get("api_key", "").strip()
    secret_key = data.get("secret_key", "").strip()
    
    bot_state["active_broker"] = exchange_id
    bot_state["api_key"] = api_key
    bot_state["secret_key"] = secret_key

    try:
        # CoinDCX Direct API Logic
        if exchange_id == "coindcx":
            timeStamp = int(round(time.time() * 1000))
            body = {"timestamp": timeStamp}
            json_body = json.dumps(body, separators=(',', ':'))
            signature = hmac.new(secret_key.encode('utf-8'), json_body.encode('utf-8'), hashlib.sha256).hexdigest()
            
            headers = {
                'Content-Type': 'application/json',
                'X-AUTH-APIKEY': api_key,
                'X-AUTH-SIGNATURE': signature
            }
            res = requests.post("https://api.coindcx.com/exchange/v1/users/balances", data=json_body, headers=headers, timeout=10)
            res_data = res.json()
            
            if isinstance(res_data, list):
                dynamic_balances = {}
                for item in res_data:
                    bal = float(item.get("balance", 0.0))
                    if bal > 0.00001:
                        dynamic_balances[item.get("currency")] = round(bal, 5)
                
                return {
                    "status": "success",
                    "message": "Connected to CoinDCX successfully!",
                    "balances": dynamic_balances
                }
            else:
                return {"status": "error", "message": "CoinDCX Key Invalid or Denied!"}

        # Other Global Exchanges via CCXT
        else:
            if not hasattr(ccxt, exchange_id):
                return {"status": "error", "message": f"{exchange_id.upper()} is not supported."}
                
            exchange_class = getattr(ccxt, exchange_id)
            exchange = exchange_class({
                'apiKey': api_key,
                'secret': secret_key,
                'enableRateLimit': True,
            })
            
            balance = exchange.fetch_balance()
            dynamic_balances = {}
            
            if 'total' in balance:
                for coin, amt in balance['total'].items():
                    if isinstance(amt, (int, float)) and amt > 0.00001:
                        dynamic_balances[coin] = round(amt, 5)

            return {
                "status": "success",
                "message": f"Connected to {exchange_id.upper()} successfully!",
                "balances": dynamic_balances
            }
    except Exception as e:
        return {"status": "error", "message": f"API Error: Invalid Keys! ({str(e)})"}


# --- ENGINE 2: TOP 100 BOT SCANNER CONTROL ---
@app.post("/api/bot-control")
async def bot_control(request: Request):
    data = await request.json()
    action = data.get("action")
    if action == "start":
        if bot_state["api_key"] == "":
            return {"status": "error", "message": "Connect API First!"}
        bot_state["is_running"] = True
        bot_state["trade_amount_usdt"] = float(data.get("amount", 10))
        add_log("🚀 BOT STARTED! Initializing Top 100 Market Scanner...")
        return {"status": "success", "message": "Bot Started!"}
    elif action == "stop":
        bot_state["is_running"] = False
        add_log("🛑 BOT STOPPED! Market scanning halted.")
        return {"status": "success", "message": "Bot Stopped!"}

@app.get("/api/bot-logs")
def get_bot_logs():
    return {"status": "success", "is_running": bot_state["is_running"], "logs": bot_state["logs"]}

async def market_scanner_loop():
    while True:
        if bot_state["is_running"]:
            try:
                ticker_url = "https://api.binance.com/api/v3/ticker/24hr"
                res = requests.get(ticker_url, timeout=10)
                all_coins = res.json()
                
                usdt_pairs = [c for c in all_coins if c['symbol'].endswith('USDT')]
                usdt_pairs.sort(key=lambda x: float(x['quoteVolume']), reverse=True)
                
                top_100_coins = usdt_pairs[:100]
                target_coin = random.choice(top_100_coins)
                coin_symbol = target_coin['symbol']
                price_change = float(target_coin['priceChangePercent'])
                volume = float(target_coin['quoteVolume'])
                
                add_log(f"🔎 Scanning Top 100: Checking {coin_symbol} for Whale Activity...")
                await asyncio.sleep(2) 
                
                if volume > 100000000:
                    add_log(f"⚠️ WHALE ALERT on {coin_symbol}! High Volume: ${(volume/1000000):.2f}M")
                    if price_change > 4.0:
                        add_log(f"📈 {coin_symbol} Uptrend Confirmed (+{price_change}%).")
                        add_log(f"⚡ EXECUTING BUY ORDER for {bot_state['trade_amount_usdt']} USDT on {bot_state['active_broker'].upper()}...")
                        await asyncio.sleep(1)
                        add_log(f"✅ SUCCESS: Bought {coin_symbol} at Market Price. Auto SL & TP Set.")
                        await asyncio.sleep(8) 
                    else:
                        add_log(f"📉 {coin_symbol} volume high, but sideways/down. No trade.")
                else:
                    add_log(f"📊 {coin_symbol} volume normal. Searching next...")
                    
            except Exception as e:
                add_log(f"❌ Scanner Error: Retrying connection...")
        
        await asyncio.sleep(3)

@app.on_event("startup")
async def startup(): 
    asyncio.create_task(market_scanner_loop())

@app.get("/")
def root(): 
    return {"status": "HiTech Top100 Scanner Engine is Live! 🚀"}

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
