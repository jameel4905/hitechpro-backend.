import os, time, ccxt, uvicorn, asyncio, random
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime

bot_state = {"active_broker": None, "api_key": None, "secret_key": None, "active_position": None, "trades_today": 0}
SCAN_COINS = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'NEAR/USDT', 'DOGE/USDT', 'ADA/USDT', 'AVAX/USDT']

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
exchange = ccxt.binance({'enableRateLimit': True})

# 1. PRICE API FOR YOUR APP
@app.get("/api/get-price/{symbol}")
def get_price(symbol: str):
    try:
        ticker = exchange.fetch_ticker(symbol.replace("-", "/"))
        return {"price": ticker['last']}
    except: return {"price": 0.0}

# 2. AUTO TRADING LOOP
async def auto_trade_loop():
    while True:
        try:
            if bot_state["api_key"] and not bot_state["active_position"]:
                random.shuffle(SCAN_COINS)
                for sym in SCAN_COINS:
                    ticker = exchange.fetch_ticker(sym)
                    price = ticker['last']
                    # Smart Qty: ₹500 / Price = Amount (Safe for any coin)
                    amount = 6.0 / price 
                    
                    # Sentiment logic
                    ohlcv = exchange.fetch_ohlcv(sym, '15m', limit=2)
                    signal = "BUY" if ohlcv[-1][4] > ohlcv[-2][4] else "SELL"
                    
                    # Execution
                    ex = ccxt.binance({'apiKey': bot_state["api_key"], 'secret': bot_state["secret_key"]})
                    if signal == "BUY": ex.create_market_buy_order(sym, amount)
                    else: ex.create_market_sell_order(sym, amount)
                    
                    bot_state["active_position"] = {"symbol": sym, "entry": price}
                    break
        except Exception as e: print(e)
        await asyncio.sleep(60)

@app.on_event("startup")
async def startup(): asyncio.create_task(auto_trade_loop())

@app.post("/api/save-keys")
def save_keys(data: dict):
    bot_state.update({"api_key": data["api_key"], "secret_key": data["secret_key"], "active_broker": "binance"})
    return {"status": "success"}

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
