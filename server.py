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
# 1. BOT MEMORY & STATE (For Limits, PnL & TP/SL)
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

# ==========================================
# 2. HITECH AI BOT CLASS (Brain 🧠 with Sentiment + Volume)
# ==========================================
class HitechAIBot:
    def __init__(self):
        self.exchange = ccxt.binance({'enableRateLimit': True})

    def get_exchange(self, exchange_name, api_key, secret_key):
        try:
            exchange_class = getattr(ccxt, exchange_name.lower())
            return exchange_class({'apiKey': api_key, 'secret': secret_key, 'enableRateLimit': True})
        except Exception:
            return ccxt.binance({'apiKey': api_key, 'secret': secret_key, 'enableRateLimit': True})

    def get_market_sentiment(self, symbol='BTC/USDT'):
        try:
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe='1d', limit=5)
            if not ohlcv: 
                return "Neutral"
            close_today = ohlcv[-1][4]
            close_yesterday = ohlcv[-2][4]
            if close_today > close_yesterday * 1.02: 
                return "Highly Bullish 🚀"
            elif close_today > close_yesterday: 
                return "Bullish 🟢"
            elif close_today < close_yesterday * 0.98: 
                return "Highly Bearish 🩸"
            else: 
                return "Bearish 🔴"
        except Exception:
            return "Neutral ⚖️"

    # 🚀 ADVANCED SENTIMENT + VOLUME ANALYSIS FILTER
    def analyze_market_conditions(self, symbol='BTC/USDT'):
        try:
            sentiment = self.get_market_sentiment(symbol)
            
            # Fetch 15m candles to check Volume & Price Action
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe='15m', limit=3)
            if not ohlcv or len(ohlcv) < 2:
                return {"signal": "HOLD", "sentiment": sentiment, "current_price": 0}

            current = ohlcv[-1]
            prev = ohlcv[-2]
            
            curr_open, curr_close, curr_vol = current[1], current[4], current[5]
            prev_vol = prev[5]
            
            signal = "HOLD"
            # Volume Spike Confirmation: Current volume must be higher than previous volume
            is_volume_high = curr_vol > prev_vol

            if "Bullish" in sentiment and curr_close > curr_open and is_volume_high:
                signal = "BUY"
            elif "Bearish" in sentiment and curr_close < curr_open and is_volume_high:
                signal = "SELL"
                
            return {
                "status": "success",
                "symbol": symbol,
                "sentiment": sentiment,
                "signal": signal,
                "current_price": curr_close
            }
        except Exception as e:
            return {"status": "error", "message": str(e), "signal": "HOLD", "sentiment": "Neutral", "current_price": 0}

# ==========================================
# 3. FASTAPI SERVER INITIALIZATION
# ==========================================
app = FastAPI(title="Hitech Crypto Trading Engine PRO")
ai_bot = HitechAIBot()  

app.add_middleware(
    CORSMiddleware, 
    allow_origins=["*"], 
    allow_credentials=True, 
    allow_methods=["*"], 
    allow_headers=["*"],
)

# ==========================================
# 4. BACKGROUND AUTO-TRADING LOOP (Fast & Smart)
# ==========================================
async def auto_trade_loop():
    print("🚀 Pro Auto-Trading Engine Started (Sentiment + Volume Mode)...")
    while True:
        try:
            current_date = datetime.now().date().isoformat()
            if bot_state["last_trade_date"] != current_date:
                bot_state["trades_today"] = 0
                bot_state["last_trade_date"] = current_date
                
            if bot_state["api_key"] and bot_state["secret_key"] and bot_state["active_broker"]:
                target_symbol = "BTC/USDT"
                analysis = ai_bot.analyze_market_conditions(target_symbol)
                current_price = analysis.get("current_price", 0)

                # 🛡️ SMART EXIT (Take Profit / Stop Loss Check)
                if bot_state["active_position"] and current_price > 0:
                    pos = bot_state["active_position"]
                    entry_price = pos["entry_price"]
                    
                    if pos["side"] == "BUY":
                        pnl_percent = ((current_price - entry_price) / entry_price) * 100
                    else:
                        pnl_percent = ((entry_price - current_price) / entry_price) * 100
                    
                    if pnl_percent >= 3.0 or pnl_percent <= -1.5:
                        exit_req = {
                            "broker": bot_state["active_broker"], 
                            "symbol": target_symbol, 
                            "api_key": bot_state["api_key"], 
                            "secret_key": bot_state["secret_key"]
                        }
                        exit_trade(exit_req)
                        bot_state["total_pnl"] += pnl_percent
                        time_str = datetime.now().strftime("%Y-%m-%d %H:%M")
                        bot_state["history"].append({"time": time_str, "action": f"Auto-Closed ({pnl_percent:.2f}%)"})
                        bot_state["active_position"] = None
                        print(f"🛡️ Smart Exit Triggered! PnL: {pnl_percent:.2f}%")
                        await asyncio.sleep(30)
                        continue

                # 🎯 MOOD + VOLUME CONFIRMED ENTRY
                signal = analysis.get("signal", "HOLD")
                if signal in ["BUY", "SELL"] and bot_state["trades_today"] < 5 and not bot_state["active_position"]:
                    trade_req = TradeRequest(
                        user_id="auto_bot", 
                        broker=bot_state["active_broker"], 
                        symbol=target_symbol, 
                        side=signal.lower(),
                        amount=0.001, 
                        api_key=bot_state["api_key"], 
                        secret_key=bot_state["secret_key"], 
                        is_futures=(signal == "SELL")
                    )
                    res = execute_real_trade(trade_req)
                    
                    if res.get("status") == "success":
                        bot_state["trades_today"] += 1
                        bot_state["active_position"] = {"symbol": target_symbol, "side": signal, "entry_price": current_price}
                        time_str = datetime.now().strftime("%Y-%m-%d %H:%M")
                        bot_state["history"].append({"time": time_str, "action": f"Volume-Mood {signal} at {current_price}"})
                        print(f"🤖 Volume-Sentiment Trade Entered: {signal} {target_symbol}")
                        
        except Exception as e:
            print(f"Loop Error: {str(e)}")
            
        # ⚡ Fast Speed Loop: Checks every 30 seconds instead of 5 minutes
        await asyncio.sleep(30) 

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(auto_trade_loop())

# ==========================================
# 5. PYDANTIC MODELS
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

# ==========================================
# 6. API ROUTES
# ==========================================
@app.get("/")
def root(): 
    return {"status": "Hitech Crypto Bot PRO Running Online!"}

@app.get("/api/market-sentiment")
def get_sentiment(symbol: str = "BTC/USDT"):
    sentiment = ai_bot.get_market_sentiment(symbol)
    return {"status": "success", "symbol": symbol, "sentiment": sentiment}

@app.get("/api/bot-history")
def get_bot_history():
    return {
        "status": "success",
        "trades_today": f"{bot_state['trades_today']}/5",
        "total_pnl": f"{bot_state['total_pnl']:.2f}%",
        "active_trade": bot_state["active_position"],
        "history": bot_state["history"][::-1]
    }

@app.get("/api/pattern-detector")
def get_pattern(symbol: str = "BTC/USDT"):
    res = ai_bot.analyze_market_conditions(symbol)
    return {
        "status": res["status"],
        "symbol": res["symbol"],
        "pattern": f"Sentiment: {res['sentiment']}",
        "signal": res["signal"],
        "current_price": res["current_price"]
    }

@app.post("/api/save-keys")
def save_user_keys(config: UserConfigRequest):
    try:
        bot_state["active_broker"] = config.exchange_name.lower()
        bot_state["api_key"] = config.api_key
        bot_state["secret_key"] = config.secret_key
        return {"status": "Success", "message": f"Connected to {config.exchange_name.upper()}! AI Engine activated."}
    except Exception as e:
        return {"status": "Error", "message": str(e)}

# ---------------------------------------------------------
# 🎯 REAL ENTRY TRADE API
# ---------------------------------------------------------
@app.post("/api/trade/execute")
def execute_real_trade(trade: TradeRequest):
    broker = trade.broker.lower()
    api_key = trade.api_key
    secret_key = trade.secret_key
    symbol = trade.symbol
    side = trade.side.lower()
    amount = trade.amount
    is_futures = trade.is_futures
    
    if not api_key or not secret_key: 
        return {"status": "error", "message": "API keys required."}
    
    try:
        if broker == 'coindcx':
            base_cur = symbol.split('/')[0] if '/' in symbol else symbol
            market_pair = f"{base_cur}USDT"
            ts = int(round(time.time() * 1000))
            sec_bytes = bytes(secret_key, encoding='utf-8')
            
            markets_data = requests.get('https://api.coindcx.com/exchange/v1/markets_details').json()
            step_size = next((float(m.get("step", 1.0)) for m in markets_data if m.get("coindcx_name") == market_pair), 1.0)
            
            trade_qty = round(int(amount / step_size) * step_size, 8)
            if trade_qty <= 0: 
                return {"status": "error", "message": "Trade qty too small."}
            
            order_body = {
                "timestamp": ts, 
                "order": {
                    "side": side, 
                    "order_type": "market_order", 
                    "market": market_pair, 
                    "total_quantity": trade_qty
                }
            }
            order_json = json.dumps(order_body)
            order_sig = hmac.new(sec_bytes, order_json.encode(), hashlib.sha256).hexdigest()
            headers = {
                'Content-Type': 'application/json', 
                'X-AUTH-APIKEY': api_key, 
                'X-AUTH-SIGNATURE': order_sig
            }
            
            resp = requests.post('https://api.coindcx.com/exchange/v1/orders/create', data=order_json, headers=headers).json()
            if 'message' in resp: 
                return {"status": "error", "message": resp.get('message')}
            return {"status": "success", "message": f"Executed {side.upper()} for {trade_qty} {base_cur}!"}
        else:
            opts = {'apiKey': api_key, 'secret': secret_key, 'enableRateLimit': True}
            if is_futures: 
                opts['options'] = {'defaultType': 'future'}
            exchange = getattr(ccxt, broker)(opts)
            if side == 'buy': 
                exchange.create_market_buy_order(symbol, amount)
            elif side == 'sell': 
                exchange.create_market_sell_order(symbol, amount)
            return {"status": "success", "message": f"Executed {side.upper()} for {amount} {symbol}!"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# ---------------------------------------------------------
# 💰 REAL PORTFOLIO 
# ---------------------------------------------------------
@app.post("/api/portfolio/balance")
def get_real_portfolio(data: dict):
    try:
        broker = data.get("broker", "binance").lower()
        api_key = data.get("api_key")
        secret_key = data.get("secret_key")
        
        if not api_key: 
            return {"status": "success", "balance": "$0.00", "history": []}

        if broker == 'coindcx':
            ts = int(round(time.time() * 1000))
            json_body = json.dumps({"timestamp": ts})
            sig = hmac.new(bytes(secret_key, encoding='utf-8'), json_body.encode(), hashlib.sha256).hexdigest()
            headers = {'Content-Type': 'application/json', 'X-AUTH-APIKEY': api_key, 'X-AUTH-SIGNATURE': sig}
            
            resp = requests.post('https://api.coindcx.com/exchange/v1/users/balances', data=json_body, headers=headers)
            if resp.status_code != 200: 
                return {"status": "error", "message": "Exchange API down", "balance": "$0.00", "history": []}
            balances = resp.json()
            
            tickers = requests.get('https://api.coindcx.com/exchange/ticker').json()
            price_map = {t.get('market', ''): float(t.get('last_price', 0)) for t in tickers}
            
            tot_inr = 0.0
            assets = []
            for b in balances:
                qty = float(b.get('balance', 0.0)) + float(b.get('locked', 0.0))
                cur = b.get('currency', '')
                if qty > 0:
                    if cur == 'INR': 
                        tot_inr += qty
                        assets.append({"symbol": "INR", "side": "HOLD", "price": f"₹{qty:,.2f}"})
                    else:
                        c_inr = price_map.get(f"{cur}INR", price_map.get(f"{cur}USDT", 0) * price_map.get('USDTINR', 83))
                        val = qty * c_inr
                        tot_inr += val
                        if val > 1: 
                            assets.append({"symbol": f"{cur}/USDT", "side": f"Qty: {qty:.4f}", "price": f"₹{val:,.2f}"})
            
            return {"status": "success", "balance": f"${tot_inr/83.0:,.2f} (₹{tot_inr:,.2f})", "history": assets}
        else:
            bal = getattr(ccxt, broker)({'apiKey': api_key, 'secret': secret_key}).fetch_balance()
            return {"status": "success", "balance": f"${bal.get('total', {}).get('USDT', 0.0):,.2f}", "history": []}
    except Exception as e:
        return {"status": "error", "balance": "$0.00", "history": []}

# ---------------------------------------------------------
# 🛑 MANUAL & AUTO EXIT TRADE API
# ---------------------------------------------------------
@app.post("/api/trade/exit")
def exit_trade(data: dict):
    broker = data.get("broker", "coindcx").lower()
    symbol = data.get("symbol")
    api_key = data.get("api_key")
    secret_key = data.get("secret_key")
    
    if not api_key: 
        return {"status": "error", "message": "API keys required"}
    try:
        if broker == 'coindcx':
            base_cur = symbol.split('/')[0] if '/' in symbol else symbol
            market_pair = f"{base_cur}USDT"
            ts, sec_bytes = int(round(time.time() * 1000)), bytes(secret_key, encoding='utf-8')
            json_body = json.dumps({"timestamp": ts})
            sig = hmac.new(sec_bytes, json_body.encode(), hashlib.sha256).hexdigest()
            headers = {'Content-Type': 'application/json', 'X-AUTH-APIKEY': api_key, 'X-AUTH-SIGNATURE': sig}
            
            bals = requests.post('https://api.coindcx.com/exchange/v1/users/balances', data=json_body, headers=headers).json()
            raw_qty = next((float(b.get('balance', 0)) for b in bals if b.get('currency') == base_cur), 0.0)
            if raw_qty <= 0: 
                return {"status": "error", "message": f"No balance"}
            
            m_data = requests.get('https://api.coindcx.com/exchange/v1/markets_details').json()
            step = next((float(m.get("step", 1.0)) for m in m_data if m.get("coindcx_name") == market_pair), 1.0)
            
            sell_qty = round(int(raw_qty / step) * step, 8)
            if sell_qty <= 0: 
                return {"status": "error", "message": "Qty too small"}
            
            order_json = json.dumps({
                "timestamp": ts, 
                "order": {
                    "side": "sell", 
                    "order_type": "market_order", 
                    "market": market_pair, 
                    "total_quantity": sell_qty
                }
            })
            order_sig = hmac.new(sec_bytes, order_json.encode(), hashlib.sha256).hexdigest()
            headers['X-AUTH-SIGNATURE'] = order_sig
            
            resp = requests.post('https://api.coindcx.com/exchange/v1/orders/create', data=order_json, headers=headers).json()
            if 'message' in resp: 
                return {"status": "error", "message": resp.get('message')}
            return {"status": "success", "message": f"Manual Exit: Sold {sell_qty} {base_cur}!"}
        else:
            ex = getattr(ccxt, broker)({'apiKey': api_key, 'secret': secret_key})
            base_cur = symbol.split('/')[0] if '/' in symbol else symbol
            amt = float(ex.fetch_balance()['total'].get(base_cur, 0))
            if amt <= 0: 
                return {"status": "error", "message": "No balance"}
            ex.create_market_sell_order(symbol, amt)
            return {"status": "success", "message": f"Manual Exit: Sold {amt} {base_cur}!"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# 🛡️ SAFETY ENDPOINTS (Flexible Keys)
@app.post("/api/verify-key")
def verify_key(data: dict):
    user_key = data.get("key", "").strip()
    if len(user_key) >= 5: 
        return {"status": "success", "message": "Bot Activated Successfully!"}
    return {"status": "error", "message": "Invalid Key"}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("server:app", host="0.0.0.0", port=port)
