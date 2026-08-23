import os, time, ccxt, uvicorn, asyncio, random
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime

# 🏢 MULTI-USER & 23+ EXCHANGES COMMERCIAL STATE
bot_state = {
    "active_broker": "binance",  # Default, app se jo user dega wo set ho jayega
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
    print("🚀 Universal Multi-Exchange Commercial Engine Started...")
    while True:
        try:
            if bot_state["api_key"] and bot_state["secret_key"] and not bot_state["active_position"]:
                broker_name = bot_state["active_broker"].lower().strip()
time_str = datetime.now().strftime("%H:%M:%S")

# Exchange se saare active USDT coins automatic load honge
markets = ex.load_markets()
usdt_coins = [s for s in markets if s.endswith('/USDT') and markets[s]['active']]
sym = random.choice(usdt_coins)
                
                try:
                    # 🔄 Check if exchange exists in CCXT's 23+ supported exchanges list
                    if broker_name not in ccxt.exchanges:
                        raise Exception(f"Exchange '{broker_name}' is not supported by CCXT!")
                    
                    # Dynamically initialize ANY exchange safely
                    exchange_class = getattr(ccxt, broker_name)
                    ex = exchange_class({
                        'apiKey': bot_state["api_key"],
                        'secret': bot_state["secret_key"],
                        'enableRateLimit': True
                    })
                    
                    # Fetch market ticker price
                    ticker = ex.fetch_ticker(sym)
                    price = ticker['last']
                    
                    # Safe ~$2 amount calculation
                    amount = 2.0 / price
                    
                    print(f"🔥 Placing trade on {broker_name.upper()} for {sym} at {price}...")
                    
                    # Universal Market Buy Order for all exchanges
                    order = ex.create_market_buy_order(sym, amount)
                    
                    if order:
                        bot_state["active_position"] = {"symbol": sym, "entry": price}
                        bot_state["trades_today"] += 1
                        bot_state["history"].insert(0, {"time": time_str, "action": f"✅ SUCCESS ({broker_name.upper()}): Bought {sym} at {price}"})
                        print(f"✅ Trade Successful on {broker_name.upper()}!")
                        
                except Exception as trade_err:
                    err_msg = str(trade_err)[:50]
                    print(f"❌ Trade Error on {broker_name.upper()}: {err_msg}")
                    bot_state["history"].insert(0, {"time": time_str, "action": f"❌ REJECTED ({broker_name.upper()}): {err_msg}"})
                    
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

# 📥 Universal Endpoint for ALL 23+ Exchanges
@app.api_route("/api/save-keys", methods=["GET", "POST"])
async def save_keys(request: Request):
    try:
        if request.method == "POST":
            data = await request.json()
        else:
            data = dict(request.query_params)
            
        print(f"📥 Commercial Multi-Broker Request ({request.method}): {data}")
        
        api_key = data.get("api_key") or data.get("apiKey")
        secret_key = data.get("secret_key") or data.get("secretKey")
        broker = data.get("broker") or data.get("active_broker") or "binance"
        
        if api_key and secret_key:
            bot_state.update({
                "api_key": api_key, 
                "secret_key": secret_key, 
                "active_broker": broker.lower().strip()
            })
            return {"status": "success", "message": f"Successfully connected to {broker.upper()}! (Multi-Exchange Ready)"}
        else:
            return {"status": "error", "message": "API Key or Secret Key missing!"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/")
def root(): return {"status": "HiTech Multi-Exchange SaaS Platform is Live! 🚀"}

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
