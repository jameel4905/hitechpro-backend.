import os
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import requests
import ccxt
import time
import hmac
import hashlib
import json
import uvicorn
import asyncio
from datetime import datetime

# ==========================================
# 1. BOT MEMORY & STATE
# ==========================================
bot_state = {
    "active_broker": None,
    "api_key": None,
    "secret_key": None,
    "trades_today": 0,
    "total_pnl": 0.0,
    "last_trade_date": datetime.now().date().isoformat(),
    "active_position": None,
    "history": []
}

# 🎯 MANUAL SL/TP SETTINGS (Aap yahan se change kar sakte hain)
TAKE_PROFIT = 3.0   # 3% Profit Target
STOP_LOSS = -1.5    # 1.5% Stop Loss

# ==========================================
# 2. HITECH AI BOT CLASS
# ==========================================
class HitechAIBot:
    def __init__(self):
        self.exchange = ccxt.binance({'enableRateLimit': True})

    def get_market_sentiment(self, symbol='BTC/USDT'):
        try:
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe='1d', limit=5)
            if not ohlcv: return "Neutral"
            close_today = ohlcv[-1][4]
            close_yesterday = ohlcv[-2][4]
            if close_today > close_yesterday * 1.02: return "Highly Bullish 🚀"
            elif close_today > close_yesterday: return "Bullish 🟢"
            elif close_today < close_yesterday * 0.98: return "Highly Bearish 🩸"
            else: return "Bearish 🔴"
        except Exception:
            return "Neutral ⚖️"

# ==========================================
# 3. FASTAPI SERVER INITIALIZATION
# ==========================================
app = FastAPI(title="Hitech Crypto Trading Engine PRO")
ai_bot = HitechAIBot()  
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# ==========================================
# 4. BACKGROUND AUTO-TRADING LOOP (Mood Based + Manual SL/TP)
# ==========================================
async def auto_trade_loop():
    print("🚀 Auto-Trading Engine Started (Mood Based Mode)...")
    while True:
        try:
            # Date Reset
            current_date = datetime.now().date().isoformat()
            if bot_state["last_trade_date"] != current_date:
                bot_state["trades_today"] = 0
                bot_state["last_trade_date"] = current_date
                
            if bot_state["api_key"] and bot_state["secret_key"] and bot_state["active_broker"]:
                target_symbol = "BTC/USDT"
                
                # Fetch Price
                ticker = ai_bot.exchange.fetch_ticker(target_symbol)
                current_price = ticker['last']
                
                # 🛡️ SMART EXIT (Manual SL/TP)
                if bot_state["active_position"]:
                    pos = bot_state["active_position"]
                    entry_price = pos["entry_price"]
                    pnl = ((current_price - entry_price) / entry_price * 100) if pos["side"] == "BUY" else ((entry_price - current_price) / entry_price * 100)
                    
                    if pnl >= TAKE_PROFIT or pnl <= STOP_LOSS:
                        exit_trade({"broker": bot_state["active_broker"], "symbol": target_symbol, "api_key": bot_state["api_key"], "secret_key": bot_state["secret_key"]})
                        bot_state["active_position"] = None
                        bot_state["history"].append({"time": datetime.now().strftime("%H:%M"), "action": f"Closed at {pnl:.2f}%"})
                        await asyncio.sleep(30)
                        continue

                # 🎯 MOOD BASED ENTRY (No Volume Filter)
                sentiment = ai_bot.get_market_sentiment(target_symbol)
                signal = "BUY" if "Bullish" in sentiment else "SELL" if "Bearish" in sentiment else "HOLD"
                
                if signal in ["BUY", "SELL"] and bot_state["trades_today"] < 5 and not bot_state["active_position"]:
                    trade_req = TradeRequest(user_id="auto_bot", broker=bot_state["active_broker"], symbol=target_symbol, side=signal.lower(), amount=0.001, api_key=bot_state["api_key"], secret_key=bot_state["secret_key"], is_futures=(signal == "SELL"))
                    res = execute_real_trade(trade_req)
                    if res.get("status") == "success":
                        bot_state["trades_today"] += 1
                        bot_state["active_position"] = {"symbol": target_symbol, "side": signal, "entry_price": current_price}
                        bot_state["history"].append({"time": datetime.now().strftime("%H:%M"), "action": f"Mood {signal} at {current_price}"})
                        print(f"🤖 Trade Entered: {signal}")
                        
        except Exception as e:
            print(f"Loop Error: {str(e)}")
        await asyncio.sleep(30) 

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(auto_trade_loop())

# ==========================================
# 5. MODELS & ROUTES (Rest of the structure remains same)
# ==========================================
class UserConfigRequest(BaseModel):
    exchange_name: str
    api_key: str
    secret_key: str

class TradeRequest(BaseModel):
    user_id: str
    broker: str
    symbol: str
    side: str
    amount: float
    api_key: str
    secret_key: str
    is_futures: bool

@app.get("/")
def root(): return {"status": "Hitech Crypto Bot PRO Running Online!"}

# [Note: Keep all existing API routes (execute_real_trade, get_real_portfolio, exit_trade, verify_key) exactly as they were in your previous code]

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("server:app", host="0.0.0.0", port=port)
