import os, time, hmac, hashlib, json, requests, uvicorn, asyncio, random
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime

bot_state = {
    "active_broker": "coindcx", 
    "api_key": "YAHAN_APNI_API_KEY_DAALO", 
    "secret_key": "YAHAN_APNI_SECRET_KEY_DAALO", 
    "active_position": None, 
    "trades_today": 0, 
    "history": []
}
SCAN_COINS = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'ADA/USDT']

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

async def auto_trade_loop():
    print("🚀 CoinDCX Engine Started...")
    while True:
        try:
            if bot_state["api_key"] and bot_state["secret_key"] and not bot_state["active_position"]:
                sym = random.choice(SCAN_COINS)
                base_cur = sym.split('/')[0]
                market_pair = f"{base_cur}USDT"
                
                # Fetch price from CoinDCX
                try:
                    tickers = requests.get('https://api.coindcx.com/exchange/ticker').json()
                    price = next((float(t['last_price']) for t in tickers if t['market'] == market_pair), 0)
                except:
                    price = 0
                
                if price > 0:
                    # Approx $2 worth of coin (safe for small balance)
                    amount = 2.0 / price 
                    time_str = datetime.now().strftime("%H:%M:%S")
                    
                    try:
                        ts = int(round(time.time() * 1000))
                        sec_bytes = bytes(bot_state["secret_key"], encoding='utf-8')
                        
                        markets_data = requests.get('https://api.coindcx.com/exchange/v1/markets_details').json()
                        step_size = next((float(m.get("step", 1.0)) for m in markets_data if m.get("coindcx_name") == market_pair), 1.0)
                        
                        trade_qty = round(int(amount / step_size) * step_size, 8)
                        
                        if trade_qty > 0:
                            order_body = {
                                "timestamp": ts, 
                                "order": {"side": "buy", "order_type": "market_order", "market": market_pair, "total_quantity": trade_qty}
                            }
                            order_json = json.dumps(order_body)
                            order_sig = hmac.new(sec_bytes, order_json.encode(), hashlib.sha256).hexdigest()
                            headers = {'Content-Type': 'application/json', 'X-AUTH-APIKEY': bot_state["api_key"], 'X-AUTH-SIGNATURE': order_sig}
                            
                            resp = requests.post('https://api.coindcx.com/exchange/v1/orders/create', data=order_json, headers=headers).json()
                            
                            if 'message' in resp:
                                # Rejection aayega toh history mein likhega
                                bot_state["history"].insert(0, {"time": time_str, "action": f"❌ REJECTED ({sym}): {resp['message']}"})
                            else:
                                bot_state["active_position"] = {"symbol": sym, "entry": price}
                                bot_state["trades_today"] += 1
                                bot_state["history"].insert(0, {"time": time_str, "action": f"✅ SUCCESS: Bought {sym} at {price}"})
                        
                    except Exception as trade_err:
                        bot_state["history"].insert(0, {"time": time_str, "action": f"❌ ERROR ({sym}): {str(trade_err)[:40]}"})
                        
        except Exception as e:
            print(f"Loop Error: {str(e)}")
            
        await asyncio.sleep(30)

@app.on_event("startup")
async def startup(): asyncio.create_task(auto_trade_loop())

@app.get("/api/bot-history")
def get_bot_history():
    return {"status": "success", "trades_today": bot_state['trades_today'], "history": bot_state["history"]}

@app.post("/api/save-keys")
def save_keys(data: dict):
    bot_state.update({"api_key": data.get("api_key"), "secret_key": data.get("secret_key"), "active_broker": "coindcx"})
    return {"status": "success"}

@app.get("/")
def root(): return {"status": "HiTech CoinDCX Bot Running! 🚀"}

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
