import os, time, hmac, hashlib, json, requests, ccxt, uvicorn, asyncio, random
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime

bot_state = {
    "active_broker": "binance",  
    "api_key": None, 
    "secret_key": None, 
    "active_position": None, 
    "trades_today": 0, 
    "history": []
}

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

async def auto_trade_loop():
    print("🚀 HiTech Hybrid Multi-Exchange Engine Started...")
    while True:
        try:
            if bot_state["api_key"] and bot_state["secret_key"] and not bot_state["active_position"]:
                broker_name = bot_state["active_broker"].lower().strip()
                time_str = datetime.now().strftime("%H:%M:%S")
                
                # 🟢 1. Agar CoinDCX hai toh Native API code chalega (100% Working)
                if "coindcx" in broker_name:
                    try:
                        sym = random.choice(['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'XRP/USDT', 'ADA/USDT'])
                        base_cur = sym.split('/')[0]
                        market_pair = f"{base_cur}USDT"
                        
                        tickers = requests.get('https://api.coindcx.com/exchange/ticker').json()
                        price = next((float(t['last_price']) for t in tickers if t['market'] == market_pair), 0)
                        
                        if price > 0:
                            amount = 6.0 / price
                            ts = int(round(time.time() * 1000))
                            sec_bytes = bytes(bot_state["secret_key"], encoding='utf-8')
                            
                            markets_data = requests.get('https://api.coindcx.com/exchange/v1/markets_details').json()
                            step_size = next((float(m.get("step", 1.0)) for m in markets_data if m.get("coindcx_name") == market_pair), 1.0)
                            
                            trade_qty = round(int(amount / step_size) * step_size, 8)
                            
                            if trade_qty > 0:
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
                                resp_data = resp.json()
                                
                                if 'message' in resp_data:
                                    bot_state["history"].insert(0, {"time": time_str, "action": f"❌ REJECTED (COINDCX): {resp_data['message']}"})
                                elif 'orders' in resp_data or 'id' in resp_data:
                                    bot_state["active_position"] = {"symbol": sym, "entry": price}
                                    bot_state["trades_today"] += 1
                                    bot_state["history"].insert(0, {"time": time_str, "action": f"✅ SUCCESS (COINDCX): Bought {sym} at {price}"})
                                else:
                                    bot_state["history"].insert(0, {"time": time_str, "action": f"⚠️ UNKNOWN: {str(resp_data)[:30]}"})
                    except Exception as native_err:
                        bot_state["history"].insert(0, {"time": time_str, "action": f"❌ ERROR (COINDCX): {str(native_err)[:40]}"})
                
                # 🔵 2. Agar koi aur exchange hai (Binance, Bybit, etc.) toh CCXT chalega
                else:
                    try:
                        if broker_name not in ccxt.exchanges:
                            raise Exception(f"Exchange '{broker_name}' is not supported!")
                        
                        exchange_class = getattr(ccxt, broker_name)
                        ex = exchange_class({
                            'apiKey': bot_state["api_key"],
                            'secret': bot_state["secret_key"],
                            'enableRateLimit': True
                        })
                        
                        markets = ex.load_markets()
                        usdt_coins = [s for s in markets if s.endswith('/USDT') and markets[s]['active']]
                        sym = random.choice(usdt_coins)
                        
                        ticker = ex.fetch_ticker(sym)
                        price = ticker['last']
                        amount = 2.0 / price
                        
                        order = ex.create_market_buy_order(sym, amount)
                        if order:
                            bot_state["active_position"] = {"symbol": sym, "entry": price}
                            bot_state["trades_today"] += 1
                            bot_state["history"].insert(0, {"time": time_str, "action": f"✅ SUCCESS ({broker_name.upper()}): Bought {sym} at {price}"})
                            
                    except Exception as ccxt_err:
                        err_msg = str(ccxt_err)[:50]
                        bot_state["history"].insert(0, {"time": time_str, "action": f"❌ REJECTED ({broker_name.upper()}): {err_msg}"})
                        
        except Exception as e:
            print(f"Loop Error: {str(e)}")
            
        await asyncio.sleep(30)

@app.on_event("startup")
async def startup(): asyncio.create_task(auto_trade_loop())

@app.get("/api/bot-history")
def get_bot_history():
    return {
        "status": "success", 
        "broker": bot_state['active_broker'],
        "trades_today": bot_state['trades_today'], 
        "history": bot_state["history"]
    }

@app.api_route("/api/save-keys", methods=["GET", "POST"])
async def save_keys(request: Request):
    try:
        if request.method == "POST":
            data = await request.json()
        else:
            data = dict(request.query_params)
            
        api_key = data.get("api_key") or data.get("apiKey")
        secret_key = data.get("secret_key") or data.get("secretKey")
        broker = data.get("broker") or data.get("active_broker") or "binance"
        
        if api_key and secret_key:
            bot_state.update({
                "api_key": api_key, 
                "secret_key": secret_key, 
                "active_broker": broker.lower().strip()
            })
            return {"status": "success", "message": f"Successfully connected to {broker.upper()}!"}
        else:
            return {"status": "error", "message": "API Key or Secret Key missing!"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/")
def root(): return {"status": "HiTech Hybrid Multi-Exchange Platform is Live! 🚀"}

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
