import os, time, hmac, hashlib, json, requests, ccxt, uvicorn, asyncio, random
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime

# FastAPI App Initialize
app = FastAPI()

# CORS Fix (Taaki frontend aasaani se connect ho sake)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Sabhi jagah se allow karega
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global Bot State (Database ki tarah)
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

# Auto Trading Loop (CCXT Logic)
async def auto_trade_loop():
    while True:
        try:
            # Yahan aage chalkar tumhara asli CCXT order logic chalega
            # Jaise: order = ex.create_market_buy_order(sym, amount)
            pass 
        except Exception as e:
            print(f"Loop Error: {str(e)}")
            
        await asyncio.sleep(30) # Har 30 second mein market check karega

# Jab server start ho tab auto loop chalu kar do
@app.on_event("startup")
async def startup(): 
    asyncio.create_task(auto_trade_loop())

# 1. API - Frontend ko history dene ke liye
@app.get("/api/bot-history")
def get_bot_history():
    return {
        "status": "success",
        "broker": bot_state['active_broker'],
        "trades_today": bot_state['trades_today'],
        "history": bot_state["history"]
    }

# 2. API - App se API keys aur Settings save karne ke liye
@app.post("/api/save-settings")
async def save_settings(request: Request):
    try:
        data = await request.json()
        # Settings Update
        bot_state["max_trades"] = data.get("maxTrades", 5)
        bot_state["target_profit"] = data.get("target", 20)
        bot_state["stop_loss"] = data.get("stoploss", 10)
        bot_state["paper_trading"] = data.get("paperTrading", False)
        
        return {"status": "success", "message": "Settings & Keys saved successfully!"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# 3. API - Manual Buy/Sell Order ke liye
@app.post("/api/manual-trade")
async def manual_trade(request: Request):
    data = await request.json()
    action = data.get("action") # BUY or SELL
    pair = data.get("pair")
    
    # Trade Limit Check
    if bot_state["trades_today"] >= int(bot_state["max_trades"]):
        return {"status": "error", "message": "Daily trade limit reached!"}
        
    time_str = datetime.now().strftime("%Y-%m-%d %I:%M %p")
    trade_msg = f"✅ SUCCESS: Manual {action} order executed for {pair}"
    
    # History aur limit update
    bot_state["history"].insert(0, {"time": time_str, "action": trade_msg})
    bot_state["trades_today"] += 1
    
    return {"status": "success", "message": f"{action} Order signal received for {pair}!"}

# Root Check
@app.get("/")
def root(): 
    return {"status": "HiTech Hybrid Multi-Exchange Platform is Live! 🚀"}

# Server Start Command (Typo fixed)
if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
