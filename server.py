import os, time, ccxt, uvicorn, asyncio, random
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime

bot_state = {"active_broker": None, "api_key": None, "secret_key": None, "active_position": None, "trades_today": 0}
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
    print("🚀 Force-Execute Engine Started...")
    while True:
        try:
            if bot_state["api_key"] and not bot_state["active_position"]:
                ex = ccxt.binance({'apiKey': bot_state["api_key"], 'secret': bot_state["secret_key"], 'enableRateLimit': True})
                
                # Pick a random coin and force-buy instantly to test connection & execution
                sym = random.choice(SCAN_COINS)
                ticker = ex.fetch_ticker(sym)
                price = ticker['last']
                
                # Minimum viable amount
                amount = 6.0 / price
                
                print(f"🔥 Force executing test trade on {sym} at {price}...")
                order = ex.create_market_buy_order(sym, amount)
                
                if order:
                    bot_state["active_position"] = {"symbol": sym, "entry": price}
                    bot_state["trades_today"] += 1
                    print(f"✅ Trade Successfully Taken on {sym}!")
                    
        except Exception as e:
            print(f"❌ Execution Error: {str(e)}")
            
        await asyncio.sleep(30)

@app.on_event("startup")
async def startup(): asyncio.create_task(auto_trade_loop())

@app.post("/api/save-keys")
def save_keys(data: dict):
    bot_state.update({"api_key": data.get("api_key"), "secret_key": data.get("secret_key"), "active_broker": "binance"})
    return {"status": "success"}

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
