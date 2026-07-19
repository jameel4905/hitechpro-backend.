from flask import Flask, jsonify, request
from flask_cors import CORS
from SmartApi import SmartConnect
import pyotp
import threading
import time
import os
app = Flask(__name__)
CORS(app)

# ==========================================
# SAAS BACKEND - AUTO & MANUAL SL/TARGET ENGINE
# ==========================================
is_running = False
current_pnl = 0.0
current_price = 0.0
smartApi = None
current_asset = "BSE:SBIN" 

has_bought = False
buy_price = 0.0

ANGEL_TOKENS = {
    "BSE:SBIN": {"exchange": "NSE", "symbol": "SBIN-EQ", "token": "3045"},
    "BSE:RELIANCE": {"exchange": "NSE", "symbol": "RELIANCE-EQ", "token": "2885"},
    "BSE:HDFCBANK": {"exchange": "NSE", "symbol": "HDFCBANK-EQ", "token": "1333"}
}

def login_angel_one(api_key, client_id, pin, totp_secret):
    global smartApi
    try:
        smartApi = SmartConnect(api_key=api_key)
        totp = pyotp.TOTP(totp_secret).now()
        data = smartApi.generateSession(client_id, pin, totp)
        if data['status']:
            print(f"✅ Login SUCCESSFUL for Client ID: {client_id}")
            return True
        return False
    except Exception as e:
        print(f"⚠️ Error in login: {e}")
        return False

def place_auto_order(transaction_type, symbol, token, qty=1):
    try:
        orderparams = {
            "variety": "NORMAL", "tradingsymbol": symbol, "symboltoken": token,
            "transactiontype": transaction_type, "exchange": "NSE",
            "ordertype": "MARKET", "producttype": "INTRADAY", 
            "duration": "DAY", "quantity": qty
        }
        orderId = smartApi.placeOrder(orderparams)
        print(f"💰 {transaction_type} ORDER PLACED! ID: {orderId}")
        return orderId
    except Exception as e:
        print(f"❌ Order Failed: {e}")
        return None

# ==========================================
# ENGINE: Yahan Auto aur Manual dono ka SL/Target check hota hai
# ==========================================
def bot_engine():
    global is_running, current_pnl, current_price, smartApi, current_asset
    global has_bought, buy_price
    
    while True:
        if is_running and smartApi is not None:
            try:
                if current_asset in ANGEL_TOKENS:
                    stock = ANGEL_TOKENS[current_asset]
                    ltp_response = smartApi.ltpData(stock["exchange"], stock["symbol"], stock["token"])
                    
                    if ltp_response and ltp_response.get('status'):
                        current_price = ltp_response['data']['ltp']
                        
                        if not has_bought:
                            # Auto-Buy Condition (Agar bot khud kharide)
                            # order_id = place_auto_order("BUY", stock["symbol"], stock["token"], 1)
                            # buy_price = current_price
                            # has_bought = True 
                            pass
                            
                        elif has_bought:
                            current_pnl = (current_price - buy_price) * 1 
                            
                            # 10% SL aur 20% Target Calculation
                            target_price = buy_price + (buy_price * 0.20)  
                            stop_loss_price = buy_price - (buy_price * 0.10) 
                            
                            # Exit Conditions
                            if current_price >= target_price:
                                print(f"🎯 Target Hit (20%)! Selling at ₹{current_price}")
                                # place_auto_order("SELL", stock["symbol"], stock["token"], 1)
                                has_bought = False 
                            
                            elif current_price <= stop_loss_price:
                                print(f"🛡️ Stop-Loss Hit (10%)! Selling at ₹{current_price}")
                                # place_auto_order("SELL", stock["symbol"], stock["token"], 1)
                                has_bought = False 
            except Exception as e:
                pass
        time.sleep(1.5) 

threading.Thread(target=bot_engine, daemon=True).start()

@app.route('/')
def home():
    return "HITECHPRO SaaS API is READY!"

@app.route('/toggle', methods=['POST'])
def toggle_bot():
    global is_running, smartApi
    data = request.json
    
    if not is_running:
        api_key = data.get('api_key')
        client_id = data.get('client_id')
        pin = data.get('pin')
        totp_secret = data.get('totp_secret')
        
        if login_angel_one(api_key, client_id, pin, totp_secret):
            is_running = True
            return jsonify({"status": "success", "bot_status": "RUNNING"})
        else:
            return jsonify({"status": "error", "message": "Login Failed! Galat Keys."}), 400
    else:
        is_running = False
        return jsonify({"status": "success", "bot_status": "STOPPED"})

@app.route('/data', methods=['GET'])
def get_data():
    global current_asset
    requested_asset = request.args.get('asset')
    if requested_asset:
        current_asset = requested_asset
    return jsonify({"is_running": is_running, "live_price": current_price, "pnl": round(current_pnl, 2)})

# ==========================================
# MANUAL TRADE UPDATE (Hybrid Feature)
# ==========================================
@app.route('/manual_order', methods=['POST'])
def manual_order():
    global has_bought, buy_price, is_running
    data = request.json
    action = data.get('action') 
    asset = data.get('asset')
    
    if smartApi is not None and asset in ANGEL_TOKENS:
        stock = ANGEL_TOKENS[asset]
        
        # Asli order yahan lagta hai
        # place_auto_order(action, stock["symbol"], stock["token"], 1)
        
        if action == 'BUY':
            # Jaise hi aap Manual Buy dabayenge, bot isko apni nigrani mein le lega
            buy_price = current_price
            has_bought = True
            is_running = True # Engine monitoring start kar dega
            message = f"✅ BUY Order Sent! Auto SL (10%) & Target (20%) ON for {stock['symbol']}"
            
        elif action == 'SELL':
            # Agar aap SL/Target se pehle khud bechna chahein
            has_bought = False
            message = f"✅ SELL Order Sent! Position closed for {stock['symbol']}"
            
        return jsonify({"status": "success", "message": message})
        
    return jsonify({"status": "error", "message": "Bot is not logged in!"}), 400

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)