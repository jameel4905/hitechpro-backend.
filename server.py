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

TAKE_PROFIT = 3.0   
STOP_LOSS = -1.5    
SCAN_COINS = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'ADA/USDT', 'DOGE/USDT', 'BNB/USDT', 'AVAX/USDT']

# ==========================================
# 2. HITECH AI BOT CLASS
# ==========================================
class HitechAIBot:
    def __init__(self):
        self.exchange = ccxt.binance({'enableRateLimit': True})

    def get_market_sentiment(self, symbol='BTC/USDT'):
        try:
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe='1d', limit=3)
            if not ohlcv: return "Neutral"
            close_today = ohlcv[-1][4]
            close_yesterday = ohlcv[-2][4]
            return "Bullish" if close_today > close_yesterday else "Bearish"
        except Exception:
            return "Neutral"

# ==========================================
# 3. FASTAPI SERVER
# ==========================================
app = FastAPI(title="Hitech Crypto Multi-Coin Bot")
ai_bot = HitechAIBot()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# ==========================================
# 4. TRADING ENGINE
# ==========================================
async def auto_trade_loop():
    while True:
        try:
            if bot_state["api_key"] and bot_state["active_broker"]:
                # Exit Logic
                if bot_state["active_position"]:
                    pos = bot_state["active_position"]
                    ticker = ai_bot.exchange.fetch_ticker(pos["symbol"])
                    current_p = ticker['last']
                    entry_p = pos["entry_price"]
                    pnl = ((current_p - entry_p)/entry_p*100) if pos["side"]=="BUY" else ((entry_p - current_p)/entry_p*100)
                    if pnl >= TAKE_PROFIT or pnl <= STOP_LOSS:
                        exit_trade({"broker": bot_state["active_broker"], "symbol": pos["symbol"], "api_key": bot_state["api_key"], "secret_key": bot_state["secret_key"]})
                        bot_state["active_position"] = None
                
                # Multi-Coin Scan Logic
                if bot_state["trades_today"] < 5 and not bot_state["active_position"]:
                    for sym in SCAN_COINS:
                        sent = ai_bot.get_market_sentiment(sym)
                        if sent != "Neutral":
                            price = ai_bot.exchange.fetch_ticker(sym)['last']
                            amt = 5.0 / price # Small trade amount
                            req = TradeRequest(user_id="auto", broker=bot_state["active_broker"], symbol=sym, side=sent.lower(), amount=amt, api_key=bot_state["api_key"], secret_key=bot_state["secret_key"], is_futures=(sent=="Bearish"))
                            res = execute_real_trade(req)
                            if res.get("status") == "success":
                                bot_state["active_position"] = {"symbol": sym, "side": sent, "entry_price": price}
                                bot_state["trades_today"] += 1
                                break
        except Exception as e: print(f"Loop: {e}")
        await asyncio.sleep(30)

@app.on_event("startup")
async def startup_event(): asyncio.create_task(auto_trade_loop())

# ==========================================
# 5. API ROUTES
# ==========================================
class TradeRequest(BaseModel):
    user_id: str; broker: str; symbol: str; side: str; amount: float; api_key: str; secret_key: str; is_futures: bool

@app.post("/api/trade/execute")
def execute_real_trade(trade: TradeRequest):
    try:
        ex = getattr(ccxt, trade.broker.lower())({'apiKey': trade.api_key, 'secret': trade.secret_key, 'options': {'defaultType': 'future' if trade.is_futures else 'spot'}})
        if trade.side == 'buy': ex.create_market_buy_order(trade.symbol, trade.amount)
        else: ex.create_market_sell_order(trade.symbol, trade.amount)
        return {"status": "success"}
    except Exception as e: return {"status": "error", "message": str(e)}

@app.post("/api/trade/exit")
def exit_trade(data: dict):
    # Simplified Exit Logic
    return {"status": "success"}

@app.post("/api/save-keys")
def save_keys(config: dict):
    bot_state["active_broker"] = config["exchange_name"].lower()
    bot_state["api_key"] = config["api_key"]
    bot_state["secret_key"] = config["secret_key"]
    return {"status": "Success"}

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
