import os, time, hmac, hashlib, json, requests, uvicorn, asyncio, random
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime

# Commercial Setup: Keys yahan blank rahengi aur App se aayengi
bot_state = {
    "active_broker": "coindcx", 
    "api_key": None, 
    "secret_key": None, 
    "active_position": None, 
    "trades_today": 0, 
    "history": []
}
SCAN_COINS = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'ADA/USDT']

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

async def auto_trade_loop():
    print("🚀 CoinDCX Commercial Engine Started... Waiting for user keys from App.")
    while True:
        try:
            # Bot tab tak wait karega jab tak App se keys nahi mil jati
            if bot_state["api_key"] and bot_state["secret_key"] and not bot_state["active_position"]:
                sym = random.choice(SCAN_COINS)
                base_cur = sym.split('/')[0]
                market_pair = f"{base_cur}USDT"
                
                try:
                    tickers = requests.get('https://api.coindcx.com/exchange/ticker').json()
                    price = next((float(t['last_price']) for t in tickers if t['market'] == market_pair), 0)
                except:
                    price = 0
                
                if price > 0:
                    amount = 2.0 / price 
                    time_str = datetime.now().strftime("%H:%M:%S")
                    
                    try:
                        ts = int(round(time.time() * 1000))
                        sec_bytes = bytes(bot_state["secret_key"], encoding='utf-8')
                        
                        markets_data = requests.get('https://api.coindcx.com/exchange/v1/markets_details').json()
                        step_size = next((float(m.get("step", 1.0)) for m in markets_data if m.get("coindcx_name") == market_pair), 1.0)
                        
                        trade_qty = round(int(amount / step_size) * step_size, 8)
                        
                        if trade_qty > 0:
                            # Perfect flat format for CoinDCX
                            order_body = {
                                "side": "buy", 
                                "order_type": "market_order", 
                                "market": market_pair, 
                                "total_quantity": trade_qty,
                                "timestamp": ts
                            }
                            
                            order_json = json.dumps(order_body, separators=(',', ':'))
                            order_sig = hmac.new(sec_bytes, order_json.encode('utf-8'), hashlib.sha256).hexdigest()
                            
                            headers = {
                                'Content-Type': 'application/json', 
                                'X-AUTH-APIKEY': bot_state["api_key"], 
                                'X-AUTH-SIGNATURE': order_sig
                            }
                            
                            resp = requests.post('https://api.coindcx.com/exchange/v1/orders/create', data=order_json, headers=headers)
                            
                            try:
                                resp_data = resp.json()
                            except:
                                resp_data = {"message": f"HTTP Error: {resp.status_code}"}
                            
                            if 'message' in resp_data:
                                bot_state["history"].insert(0, {"time": time_str, "action": f"❌ REJECTED ({sym}): {resp_data['message']}"})
                            elif 'orders' in resp_data or 'id' in resp_data:
                                bot_state["active_position"] = {"symbol": sym, "entry": price}
                                bot_state["trades_today"] += 1
                                bot_state["history"].insert(0, {"time": time_str, "action": f"✅ SUCCESS: Bought {sym} at {price}"})
                            else:
                                bot_state["history"].insert(0, {"time": time_str, "action": f"⚠️ UNKNOWN ({sym}): Check App!"})
                                
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

# Ab ye GET aur POST dono requests ko accept karega (App ki problem khatam!)
@app.api_route("/api/save-keys", methods=["GET", "POST"])
async def save_keys(request: Request):
    try:
        # Check if data is coming via POST or GET URL parameters
        if request.method == "POST":
            data = await request.json()
        else:
            data = dict(request.query_params)
            
        print(f"📥 BINGO! App se keys aagayi hain ({request.method}): {data}")
        
        api_key = data.get("api_key") or data.get("apiKey")
        secret_key = data.get("secret_key") or data.get("secretKey")
        
        if api_key and secret_key:
            bot_state.update({
                "api_key": api_key, 
                "secret_key": secret_key, 
                "active_broker": "coindcx"
            })
            return {"status": "success", "message": "Keys activated successfully!"}
        else:
            return {"status": "error", "message": "Keys are missing in request!"}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    
    bot_state.update({
        "api_key": data.get("api_key"), 
        "secret_key": data.get("secret_key"), 
        "active_broker": "coindcx"
    })
    return {"status": "success", "message": "Keys activated for trading!"}

@app.get("/")
def root(): return {"status": "HiTech CoinDCX Commercial Bot API Running! 🚀"}

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
