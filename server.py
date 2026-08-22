import os, time, ccxt, uvicorn, asyncio, random
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime

bot_state = {"active_broker": None, "api_key": None, "secret_key": None, "active_position": None, "trades_today": 0, "history": []}
SCAN_COINS = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'NEAR/USDT', 'DOGE/USDT', 'ADA/USDT', 'AVAX/USDT']

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/api/get-price/{symbol}")
def get_price(symbol: str):
    try:
        ex = ccxt.binance({'enableRateLimit': True})
        ticker = ex.fetch_ticker(symbol.replace("-", "/"))
        return {"price": ticker['last']}
    except: return {"price": 0.0}

async def auto_trade_loop():
    print("🚀 Error-Tracking Trade Engine Started...")
    while True:
        try:
            if bot_state["api_key"] and not bot_state["active_position"]:
                ex = ccxt.binance({'apiKey': bot_state["api_key"], 'secret': bot_state["secret_key"], 'enableRateLimit': True})
                
                sym = random.choice(SCAN_COINS)
                ticker = ex.fetch_ticker(sym)
                price = ticker['last']
                amount = 6.0 / price
                
                time_str = datetime.now().strftime("%H:%M:%S")
                
                try:
                    print(f"🔥 Attempting trade on {sym}...")
                    order = ex.create_market_buy_order(sym, amount)
                    if order:
                        bot_state["active_position"] = {"symbol": sym, "entry": price}
                        bot_state["trades_today"] += 1
                        bot_state["history"].insert(0, {"time": time_str, "action": f"✅ SUCCESS: Bought {sym} at {price}"})
                        print(f"✅ Trade Successfully Taken on {sym}!")
                except Exception as trade_err:
                    # Yahan reject hua hua trade error ke sath history mein save ho jayega!
                    err_msg = str(trade_err)
                    print(f"❌ Trade Rejected on {sym}: {err_msg}")
                    bot_state["history"].insert(0, {"time": time_str, "action": f"❌ REJECTED ({sym}): {err_msg[:40]}..."})
                    
        except Exception as e:
            print(f"Loop Error: {str(e)}")
            
        await asyncio.sleep(30)

@app.on_event("startup")
async def startup(): asyncio.create_task(auto_trade_loop())

@app.get("/api/bot-history")
def get_bot_history():
    return {
        "status": "success",
        "trades_today": f"{bot_state['trades_today']}/5",
        "active_trade": bot_state["active_position"],
        "history": bot_state["history"]
    }

@app.post("/api/save-keys")
def save_keys(data: dict):
    bot_state.update({"api_key": data.get("api_key"), "secret_key": data.get("secret_key"), "active_broker": "binance"})
    return {"status": "success"}

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
