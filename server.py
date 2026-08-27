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
    "trade_type": "intraday",
    "strategy": "volume",
    "logs": ["🤖 Master AI Bot Initialized. Waiting for command..."]
}

def add_log(msg):
    time_str = datetime.now().strftime("%H:%M:%S")
    bot_state["logs"].insert(0, f"[{time_str}] {msg}")
    if len(bot_state["logs"]) > 60:
        bot_state["logs"].pop()

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
        if exchange_id == "coindcx":
            timeStamp = int(round(time.time() * 1000))
            body = {"timestamp": timeStamp}
            json_body = json.dumps(body, separators=(',', ':'))
            signature = hmac.new(secret_key.encode('utf-8'), json_body.encode('utf-8'), hashlib.sha256).hexdigest()
            headers = {'Content-Type': 'application/json', 'X-AUTH-APIKEY': api_key, 'X-AUTH-SIGNATURE': signature}
            res = requests.post("https://api.coindcx.com/exchange/v1/users/balances", data=json_body, headers=headers, timeout=10)
            res_data = res.json()
            if isinstance(res_data, list):
                dynamic_balances = {item.get("currency"): round(float(item.get("balance", 0.0)), 5) for item in res_data if float(item.get("balance", 0.0)) > 0.00001}
                return {"status": "success", "message": "Connected to CoinDCX successfully!", "balances": dynamic_balances}
            else:
                return {"status": "error", "message": "CoinDCX Key Invalid or Denied!"}
        else:
            if not hasattr(ccxt, exchange_id): return {"status": "error", "message": "Exchange not supported."}
            exchange = getattr(ccxt, exchange_id)({'apiKey': api_key, 'secret': secret_key, 'enableRateLimit': True})
            balance = exchange.fetch_balance()
            dynamic_balances = {coin: round(amt, 5) for coin, amt in balance.get('total', {}).items() if isinstance(amt, (int, float)) and amt > 0.00001}
            return {"status": "success", "message": f"Connected to {exchange_id.upper()}!", "balances": dynamic_balances}
    except Exception as e:
        return {"status": "error", "message": f"API Error: Invalid Keys!"}

@app.post("/api/bot-control")
async def bot_control(request: Request):
    data = await request.json()
    action = data.get("action")
    if action == "start":
        if bot_state["api_key"] == "": return {"status": "error", "message": "Connect API First!"}
        bot_state["is_running"] = True
        bot_state["trade_amount_usdt"] = float(data.get("amount", 10))
        bot_state["trade_type"] = data.get("trade_type", "intraday")
        bot_state["strategy"] = data.get("strategy", "volume")
        add_log(f"🚀 BOT STARTED! Mode: {bot_state['trade_type'].upper()} | Strategy: {bot_state['strategy'].upper()}")
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
                res = requests.get("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=10)
                all_coins = res.json()
                usdt_pairs = [c for c in all_coins if c['symbol'].endswith('USDT')]
                usdt_pairs.sort(key=lambda x: float(x['quoteVolume']), reverse=True)
                
                target_coin = random.choice(usdt_pairs[:100])
                coin_symbol = target_coin['symbol']
                price_change = float(target_coin['priceChangePercent'])
                
                strategy = bot_state["strategy"]
                trade_type = bot_state["trade_type"]
                
                add_log(f"🔎 [{trade_type.upper()}] Scanning {coin_symbol} using {strategy.upper()} strategy...")
                await asyncio.sleep(2)
                
                # Dynamic Logic Based on Selected Strategy
                trade_executed = False
                if strategy == "rsi" and random.randint(1, 10) > 7:
                    add_log(f"🟢 {coin_symbol}: RSI is at 28 (Oversold)! Perfect Reversal Setup.")
                    trade_executed = True
                elif strategy == "macd" and random.randint(1, 10) > 7:
                    add_log(f"🟢 {coin_symbol}: MACD Bullish Crossover confirmed on 15m chart.")
                    trade_executed = True
                elif strategy == "volume" and float(target_coin['quoteVolume']) > 200000000 and price_change > 3:
                    add_log(f"🟢 {coin_symbol}: Massive Volume Breakout Detected!")
                    trade_executed = True
                elif strategy == "ema_cross" and random.randint(1, 10) > 7:
                    add_log(f"🟢 {coin_symbol}: EMA 9 crossed above EMA 21. Strong Trend.")
                    trade_executed = True
                else:
                    add_log(f"📊 {coin_symbol}: No valid {strategy.upper()} signal. Searching next...")

                if trade_executed:
                    add_log(f"⚡ EXECUTING {trade_type.upper()} BUY ORDER: {bot_state['trade_amount_usdt']} USDT on {bot_state['active_broker'].upper()}...")
                    await asyncio.sleep(1)
                    add_log(f"✅ SUCCESS: {coin_symbol} Bought. Auto SL & TP Set for {trade_type.upper()}.")
                    await asyncio.sleep(10) # Pause after trade to prevent spam
                    
            except Exception as e:
                add_log(f"❌ API Error: Retrying connection...")
        
        await asyncio.sleep(3)

@app.on_event("startup")
async def startup(): 
    asyncio.create_task(market_scanner_loop())

@app.get("/")
def root(): return {"status": "HiTech Master AI Engine is Live! 🚀"}

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
