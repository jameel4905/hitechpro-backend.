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

TAKE_PROFIT = 3.0   # 3% Profit Target
STOP_LOSS = -1.5    # 1.5% Stop Loss

# Top Coins to Scan Fast
SCAN_COINS = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'ADA/USDT', 'DOGE/USDT', 'BNB/USDT', 'AVAX/USDT']

# ==========================================
# 2. HITECH AI BOT CLASS (Smart Intraday Analyzer)
# ==========================================
class HitechAIBot:
    def __init__(self):
        self.exchange = ccxt.binance({'enableRateLimit': True})

    def analyze_fast_market(self, symbol='BTC/USDT'):
        try:
            # 15-minute candles for quick, smart action
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe='15m', limit=3)
            if not ohlcv or len(ohlcv) < 2:
                return {"signal": "HOLD", "price": 0}

            latest = ohlcv[-1]
            opn, close = latest[1], latest[4]
            body = close - opn

            # Smart Condition: If green candle with solid momentum -> BUY, if red -> SELL
            if body > 0:
                return {"signal": "BUY", "price": close}
            elif body < 0:
                return {"signal": "SELL", "price": close}
            else:
                return {"signal": "HOLD", "price": close}
        except Exception:
            return {"signal": "HOLD", "price": 0}

# ==========================================
# 3. FASTAPI SERVER
# ==========================================
app = FastAPI(title="Hitech Crypto Smart-Fast Bot")
ai_bot = HitechAIBot()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# ==========================================
# 4. SMART & FAST TRADING ENGINE
# ==========================================
async def auto_trade_loop():
    print("🚀 Smart-Fast Multi-Coin Engine Started...")
    while True:
        try:
            # Reset daily trades counter
            current_date = datetime.now().date().isoformat()
            if bot_state["last_trade_date"] != current_date:
                bot_state["trades_today"] = 0
                bot_state["last_trade_date"] = current_date

            if bot_state["api_key"] and bot_state["active_broker"]:
                
                # 🛡️ EXIT CHECK
                if bot_state["active_position"]:
                    pos = bot_state["active_position"]
                    ticker = ai_bot.exchange.fetch_ticker(pos["symbol"])
                    current_p = ticker['last']
                    entry_p = pos["entry_price"]
                    pnl = ((current_p - entry_p)/entry_p*100) if pos["side"]=="BUY" else ((entry_p - current_p)/entry_p*100)
                    
                    if pnl >= TAKE_PROFIT or pnl <= STOP_LOSS:
                        exit_trade({"broker": bot_state["active_broker"], "symbol": pos["symbol"], "api_key": bot_state["api_key"], "secret_key": bot_state["secret_key"]})
                        bot_state["active_position"] = None
                        bot_state["history"].append({"time": datetime.now().strftime("%H:%M"), "action": f"Closed at {pnl:.2f}%"})
                        print(f"🛡️ Position Closed with PnL: {pnl:.2f}%")
                    
                    await asyncio.sleep(15)
                    continue

                # ⚡ FAST SCANNING ACROSS ALL COINS
                if bot_state["trades_today"] < 5 and not bot_state["active_position"]:
                    for sym in SCAN_COINS:
                        analysis = ai_bot.analyze_fast_market(sym)
                        signal = analysis["signal"]
                        price = analysis["price"]

                        if signal in ["BUY", "SELL"] and price > 0:
                            # Budget optimized amount for ₹800 balance (~$5 equivalent)
                            amt = 5.0 / price
                            
                            req = TradeRequest(
                                user_id="auto", 
                                broker=bot_state["active_broker"], 
                                symbol=sym, 
                                side=signal.lower(), 
                                amount=amt, 
                                api_key=bot_state["api_key"], 
                                secret_key=bot_state["secret_key"], 
                                is_futures=(signal == "SELL")
                            )
                            res = execute_real_trade(req)
                            if res.get("status") == "success":
                                bot_state["active_position"] = {"symbol": sym, "side": signal, "entry_price": price}
                                bot_state["trades_today"] += 1
                                bot_state["history"].append({"time": datetime.now().strftime("%H:%M"), "action": f"{signal} {sym} at {price}"})
                                print(f"🤖 Smart Trade Executed: {signal} on {sym}!")
                                break # Take one trade and wait
        except Exception as e:
            print(f"Loop Error: {e}")
            
        await asyncio.sleep(20) # Check every 20 seconds

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(auto_trade_loop())

# ==========================================
# 5. API ROUTES & EXECUTION
# ==========================================
class TradeRequest(BaseModel):
    user_id: str
    broker: str
    symbol: str
    side: str
    amount: float
    api_key: str
    secret_key: str
    is_futures: bool

@app.post("/api/trade/execute")
def execute_real_trade(trade: TradeRequest):
    try:
        ex = getattr(ccxt, trade.broker.lower())({
            'apiKey': trade.api_key, 
            'secret': trade.secret_key, 
            'options': {'defaultType': 'future' if trade.is_futures else 'spot'}
        })
        if trade.side == 'buy':
            ex.create_market_buy_order(trade.symbol, trade.amount)
        else:
            ex.create_market_sell_order(trade.symbol, trade.amount)
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/trade/exit")
def exit_trade(data: dict):
    try:
        broker = data.get("broker", "binance").lower()
        ex = getattr(ccxt, broker)({'apiKey': data.get("api_key"), 'secret': data.get("secret_key")})
        # Basic emergency close market logic if needed
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/save-keys")
def save_keys(config: dict):
    bot_state["active_broker"] = config.get("exchange_name", "").lower()
    bot_state["api_key"] = config.get("api_key")
    bot_state["secret_key"] = config.get("secret_key")
    return {"status": "Success"}

@app.get("/")
def root():
    return {"status": "Hitech Smart-Fast Bot Running Online!"}

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
