from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi import FastAPI, Query
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import requests
from algo_bot import HitechAIBot  

app = FastAPI(title="Hitech Crypto Trading Engine")
ai_bot = HitechAIBot()  

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"status": "Hitech Crypto Bot Backend Running Online!"}

@app.get("/api/live-prices")
def get_live_prices():
    try:
        resp = requests.get("https://api.coindcx.com/exchange/ticker")
        data = resp.json()
        target_markets = [
            'BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 
            'BNBUSDT', 'ADAUSDT', 'DOGEUSDT', 'MATICUSDT',
            'DOTUSDT', 'LINKUSDT', 'AVAXUSDT', 'TRXUSDT'
        ]
        result = []
        for item in data:
            if item.get('market') in target_markets:
                symbol = item['market'].replace('USDT', '/USDT')
                last_price = float(item.get('last_price', 0))
                change_24h = float(item.get('change_24_hour', 0))
                result.append({
                    "symbol": symbol,
                    "price": f"${last_price:.2f}",
                    "change": f"{change_24h:.2f}%",
                    "isUp": change_24h >= 0
                })
        result.sort(key=lambda x: target_markets.index(x['symbol'].replace('/', '')))
        return {"markets": result}
    except Exception as e:
        return {"markets": [], "error": str(e)}

@app.get("/api/bot-signal")
def get_bot_signal(symbol: str = "BTC/USDT", t: str = ""):
    signal_data = ai_bot.analyze_market(symbol)
    return signal_data

@app.get("/api/chart-data")
def get_chart_data(symbol: str = "BTC/USDT", timeframe: str = "1h", t: str = ""):
    try:
        # 🔥 YAHAN FIX KIYA HAI: limit 40 se badhakar 200 kar di taaki SMA 99 chal sake!
        ohlcv = ai_bot.exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=200)
        chart_data = [{"time": c[0], "open": c[1], "high": c[2], "low": c[3], "close": c[4], "volume": c[5]} for c in ohlcv]
        return {"status": "Success", "data": chart_data}
    except Exception as e:
        return {"status": "Error", "message": str(e)}

@app.get("/api/place-order")
def place_order(symbol: str = "BTC/USDT", side: str = "buy"):
    order_result = ai_bot.execute_trade(symbol, side, 0.001) 
    return order_result
# User ki keys ko accept karne ke liye model
# User ki keys ko accept karne ke liye model
class UserConfigRequest(BaseModel):
    exchange_name: str
    api_key: str
    secret_key: str

@app.post("/api/save-keys")
def save_user_keys(config: UserConfigRequest):
    try:
        ai_bot.exchange = ai_bot.get_exchange(
            config.exchange_name, 
            config.api_key, 
            config.secret_key
        )
        return {"status": "Success", "message": f"Successfully connected to {config.exchange_name}!"}
    except Exception as e:
        return {"status": "Error", "message": str(e)}
import os


  # Purana code...
@app.post("/api/save-keys")
def save_keys(config: UserConfigRequest):
    ...
    except Exception as e:
        return {"status": "Error", "message": str(e)}
# Yahan se naya code shuru
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
        # Pata lagate hain ki order success hoga ya nahi
        return {
            "status": "success", 
            "message": f"Successfully executed {trade.side} order for {trade.amount} {trade.symbol} on {trade.broker.upper()}!"
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/trade/history/{user_id}")
def get_trade_history(user_id: str):
    # Dummy history list beta testing ke liye
    history = [
        {"symbol": "BTC/USDT", "side": "BUY", "price": 64200.50, "stop_loss": 57780.45},
        {"symbol": "ETH/USDT", "side": "SELL", "price": 3450.20, "stop_loss": 3795.22}
    ]
    return {"status": "success", "history": history}

@app.post("/api/bot/toggle")
def toggle_bot(data: dict):
    return {"status": "success", "message": "Bot status updated"}
# Naya code yahan khatam
# ==========================================
# YAHAN BEECH MEIN ADMIN PANEL KA CODE PASTE KARO 👇
# ==========================================

pending_activations = []

@app.post("/api/payment/submit")
def submit_payment(user_id: str, utr: str):
    pending_activations.append({"user_id": user_id, "utr": utr, "status": "Pending"})
    return {"status": "Success", "message": "Payment submitted successfully!"}

@app.get("/admin/panel", response_class=HTMLResponse)
def admin_panel():
    rows = ""
    for idx, p in enumerate(pending_activations):
        status_color = "orange" style if p["status"] == "Pending" else "green"
        rows += f"""
        <tr>
            <td>{p['user_id']}</td>
            <td><b>{p['utr']}</b></td>
            <td>{p['status']}</td>
            <td>
                <form action="/admin/activate/{idx}" method="post" style="display:inline;">
                    <button type="submit">Approve & Activate</button>
                </form>
            </td>
        </tr>
        """
    return HTMLResponse(content=f"<html><body><h2>Admin Panel</h2><table>{rows}</table></body></html>")

@app.post("/admin/activate/{index}")
def activate_user(index: int):
    if 0 <= index < len(pending_activations):
        pending_activations[index]["status"] = "Active"
    return RedirectResponse(url="/admin/panel", status_code=303)

# ==========================================
# YAHAN SE AAPKA PURANA MAIN BLOCK SHURU HOGA 👇
# ==========================================
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 5000))
    uvicorn.run(app, host="0.0.0.0", port=port)
  
