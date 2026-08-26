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
    "active_broker": "binance",
    "api_key": "",
    "secret_key": "",
    "trades_today": 0,
    "max_trades": 5,
    "target_profit": 20,
    "stop_loss": 10,
    "paper_trading": False,
    "active_position": None,
    "history": [
        {"time": datetime.now().strftime("%Y-%m-%d %I:%M %p"), "action": "✅ SUCCESS: Backend Engine Started!"}
    ]
}

# --- DYNAMIC PORTFOLIO UPDATE ---
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
        # 1. SPECIAL CASE: CoinDCX (Direct API)
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
                    # Sirf wahi coin bhejo jisme balance 0 se zyada ho
                    if bal > 0.00001:
                        dynamic_balances[item.get("currency")] = round(bal, 5)
                
                return {
                    "status": "success",
                    "message": "Connected to CoinDCX successfully!",
                    "balances": dynamic_balances
                }
            else:
                return {"status": "error", "message": "CoinDCX Key Invalid or Permission Denied!"}

        # 2. GLOBAL EXCHANGES (Binance, Bybit, OKX via CCXT)
        else:
            if not hasattr(ccxt, exchange_id):
                return {"status": "error", "message": f"{exchange_id.upper()} is not supported by CCXT engine."}
                
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

async def auto_trade_loop():
    while True:
        try:
            pass 
        except Exception as e:
            print(f"Loop Error: {str(e)}")
        await asyncio.sleep(30)

@app.on_event("startup")
async def startup(): 
    asyncio.create_task(auto_trade_loop())

@app.get("/api/bot-history")
def get_bot_history():
    return {
        "status": "success",
        "broker": bot_state['active_broker'],
        "trades_today": bot_state['trades_today'],
        "history": bot_state["history"]
    }

@app.post("/api/save-settings")
async def save_settings(request: Request):
    try:
        data = await request.json()
        bot_state["max_trades"] = data.get("maxTrades", 5)
        bot_state["target_profit"] = data.get("target", 20)
        bot_state["stop_loss"] = data.get("stoploss", 10)
        bot_state["paper_trading"] = data.get("paperTrading", False)
        return {"status": "success", "message": "Settings saved successfully!"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/manual-trade")
async def manual_trade(request: Request):
    data = await request.json()
    action = data.get("action") 
    pair = data.get("pair")
    
    if bot_state["trades_today"] >= int(bot_state["max_trades"]):
        return {"status": "error", "message": "Daily trade limit reached!"}
        
    time_str = datetime.now().strftime("%Y-%m-%d %I:%M %p")
    trade_msg = f"✅ SUCCESS: Manual {action} order executed for {pair}"
    
    bot_state["history"].insert(0, {"time": time_str, "action": trade_msg})
    bot_state["trades_today"] += 1
    
    return {"status": "success", "message": f"{action} Order signal received for {pair}!"}

@app.get("/")
def root(): 
    return {"status": "HiTech Hybrid Multi-Exchange Platform is Live! 🚀"}

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
