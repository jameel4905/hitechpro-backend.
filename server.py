import os, time, ccxt, uvicorn, asyncio, random
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime

# 🏢 MULTI-USER & MULTI-BROKER COMMERCIAL STATE
bot_state = {
    "active_broker": "coindcx",  # Default broker, app se change ho jayega
    "api_key": None, 
    "secret_key": None, 
    "active_position": None, 
    "trades_today": 0, 
    "history": []
}

SCAN_COINS = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'ADA/USDT']

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

async def auto_trade_loop():
    print("🚀 Multi-Broker Commercial Engine Started...")
    while True:
        try:
            if bot_state["api_key"] and bot_state["secret_key"] and not bot_state["active_position"]:
                broker_name = bot_state["active_broker"].lower()
                sym = random.choice(SCAN_COINS)
                time_str = datetime.now().strftime("%H:%M:%S")
                
                try:
                    # 🔄 Dynamically connect to ANY exchange using CCXT!
                    exchange_class = getattr(ccxt, broker_name)
                    ex = exchange_class({
                        'apiKey': bot_state["api_key"],
                        'secret': bot_state["secret_key"],
                        'enableRateLimit': True
                    })
                    
                    # Fetch market price
                    ticker = ex.fetch_ticker(sym)
                    price = ticker['last']
                    
                    # Calculate safe ~$2 amount
                    amount = 2.0 / price
                    
                    print(f"🔥 Attempting trade on {broker_name.upper()} for {sym} at {price}...")
                    
                    # Place Market Buy Order
                    order = ex.create_market_buy_order(sym, amount)
                    
                    if order:
                        bot_state["active_position"] = {"symbol": sym, "entry": price}
                        bot_state["trades_today"] += 1
                        bot_state["history"].insert(0, {"time": time_str, "action": f"✅ SUCCESS ({broker_name.upper()}) Bought {sym} at {price}"})
                        print(f"✅ Trade Successful on {broker_name.upper()}!")
                        
                except Exception as trade_err:
                    err_msg = str(trade_err)
                    print(f"❌ Trade Rejected on {broker_name.upper()}: {err_msg}")
                    bot_state["history"].insert(0, {"time": time_str, "action": f"❌ REJECTED ({broker_name.upper()}): {err_msg[:40]}"})
                    
        except Exception as e:
            print(f"Loop Error: {str(e)}")
            
        await asyncio.sleep(30)

@app.on_event("startup")
async def startup(): asyncio.create_task(auto_trade_loop())

@app.get("/api/bot-history")
def get_bot_history():
    return {
        "status": "success", 
        "broker": bot_state['active_broker'],
        "trades_today": bot_state['trades_today'], 
        "history": bot_state["history"]
    }

# 📥 Universal Save Keys Endpoint (Handles any Broker: Binance, CoinDCX, etc.)
@app.api_route("/api/save-keys", methods=["GET", "POST"])
async def save_keys(request: Request):
    try:
        if request.method == "POST":
            data = await request.json()
        else:
            data = dict(request.query_params)
            
        print(f"📥 Commercial App Request ({request.method}): {data}")
        
        api_key = data.get("api_key") or data.get("apiKey")
        secret_key = data.get("secret_key") or data.get("secretKey")
        broker = data.get("broker") or data.get("active_broker") or "coindcx"
        
        if api_key and secret_key:
            bot_state.update({
                "api_key": api_key, 
                "secret_key": secret_key, 
                "active_broker": broker.lower()
            })
            return {"status": "success", "message": f"Connected successfully to {broker.upper()}!"}
        else:
            return {"status": "error", "message": "API Key or Secret Key is missing!"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/")
def root(): return {"status": "HiTech Multi-Broker Commercial Platform is Live! 🚀"}

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
