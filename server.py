import os, time, hmac, hashlib, json, requests, ccxt, uvicorn, asyncio, random
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

bot_state = {
    "is_running": False,
    "active_broker": "paper",   # 'paper' ya 'coindcx'
    "api_key": "",
    "secret_key": "",
    "quote_currency": "USDT",   # 'USDT' ya 'INR'
    "trade_amount": 5.0,         # USDT mein $5 ya INR mein ₹500
    "trade_type": "intraday", 
    "strategy": "volume",
    "logs": ["🤖 Master AI Engine Initialized. VIP Security & Live Execution Ready."],
    "active_trades": [],   
    "trade_history": [],    
    "paper_balance": 10000.0,
    "today_pnl": 0.0, 
    "last_settlement_date": datetime.utcnow().strftime("%Y-%m-%d") 
}

DATA_FILE = "bot_data.json"
KEYS_DB_FILE = "keys_db.json"

# 👑 MASTER LIST OF VIP ACTIVATION KEYS (SINGLE-USE & DEVICE-BOUND)
MASTER_VIP_KEYS = [
    "Ttyux7837", "yyuxv9990", "zazoz7689", "wqxxb8112", "ddrxz9099", "ssolp0112",
    "dxxct8900", "vvvst6090", "topct4562", "jamrt2189", "bcjoz0445", "savvc3188",
    "gyyop5678", "somno8955", "okxdc9967", "ssopx3991", "sddtc0332", "wqplo0349",
    "ccdri8922", "vdszx5678", "Ecxaz8881", "cccto8110", "ffrtc4590", "cvxns4286",
    "drtpc7634", "trxza3339", "hctza7811", "drtrc4589", "ffctb8745", "Ahode3462",
    "yjdtes8950", "huawol7624", "kiyfs8907", "hhyat7866", "hhgat8201", "hgwkl6544",
    "ghlao8900", "hungd8765", "hutes9032", "huowl1425", "uwlak6902", "haqao3430",
    "haeri9023", "lopas8443", "olase9088", "xvcbm3286", "cmzxn0990", "rteoa6723",
    "awalo0120", "smxfg9034", "qoesk0098", "gdncm8674", "azmzn3490", "bhafi6789",
    "plomc7563", "cvbfz5601", "hpctn8823", "qarap1209", "akyce9743", "dyyct1239",
    "lopst2179", "wqlla1356", "bgmvs8040", "daytc7654", "slmpo4597", "ftesr0967",
    "qawas8654", "vcxqa6789", "poiyt1452", "utyuo1001", "wopae9882", "lpost3459",
    "laalo5901", "tyucv7732", "yeduo0111", "waqao9090", "wasar7728", "iitrc4567",
    "tuyvc6610", "resct6712", "ohpor5098", "rwocp8724", "ghuyt6723", "Jiuno0989",
    "ploar7093", "aeiop9321", "ppout9955", "ictno7766", "aicio7711", "ddrco3750",
    "abovc8023", "ddcrt9959", "qoplu1898", "oiuyt4587", "qpoui0908", "woplt1010",
    "mnuni4089", "dcvna3090", "aavvc0001", "aolct0099", "sasat7890", "llpot8686",
    "kkubx0567", "ilctn4590", "actto1209", "ssdco5678"
]

keys_db = {}

def load_keys_database():
    global keys_db
    if os.path.exists(KEYS_DB_FILE):
        try:
            with open(KEYS_DB_FILE, "r") as f:
                keys_db = json.load(f)
        except:
            keys_db = {}
    
    # Initialize unseeded keys
    for k in MASTER_VIP_KEYS:
        clean_k = k.strip()
        if clean_k not in keys_db:
            keys_db[clean_k] = {
                "used": False,
                "device_id": None,
                "activated_at": None,
                "expires_at": None,
                "referral_count": 0
            }
    save_keys_database()

def save_keys_database():
    try:
        with open(KEYS_DB_FILE, "w") as f:
            json.dump(keys_db, f, indent=2)
    except:
        pass

load_keys_database()

def get_global_time():
    return datetime.utcnow().isoformat() + "Z"

def get_curr_symbol():
    return "₹" if bot_state.get("quote_currency") == "INR" else "$"

def load_memory():
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r") as f:
                data = json.load(f)
                bot_state["paper_balance"] = data.get("paper_balance", 10000.0)
                if bot_state["paper_balance"] < 10.0:
                    bot_state["paper_balance"] = 10000.0
                bot_state["active_trades"] = data.get("active_trades", [])
                bot_state["trade_history"] = data.get("trade_history", [])
                bot_state["is_running"] = data.get("is_running", False)
                bot_state["quote_currency"] = data.get("quote_currency", "USDT")
                bot_state["trade_amount"] = data.get("trade_amount", 5.0)
                bot_state["trade_type"] = data.get("trade_type", "intraday")
                bot_state["strategy"] = data.get("strategy", "volume")
                bot_state["today_pnl"] = data.get("today_pnl", 0.0)
                bot_state["last_settlement_date"] = data.get("last_settlement_date", datetime.utcnow().strftime("%Y-%m-%d"))
                bot_state["active_broker"] = data.get("active_broker", "paper")
                bot_state["api_key"] = data.get("api_key", "")
                bot_state["secret_key"] = data.get("secret_key", "")
        except:
            pass

def save_memory():
    try:
        with open(DATA_FILE, "w") as f:
            json.dump({
                "paper_balance": bot_state["paper_balance"],
                "active_trades": bot_state["active_trades"],
                "trade_history": bot_state["trade_history"],
                "is_running": bot_state["is_running"],
                "quote_currency": bot_state["quote_currency"],
                "trade_amount": bot_state["trade_amount"],
                "trade_type": bot_state["trade_type"],
                "strategy": bot_state["strategy"],
                "today_pnl": bot_state["today_pnl"],
                "last_settlement_date": bot_state["last_settlement_date"],
                "active_broker": bot_state["active_broker"],
                "api_key": bot_state["api_key"],
                "secret_key": bot_state["secret_key"]
            }, f)
    except:
        pass

load_memory()

def add_log(msg):
    time_str = get_global_time()
    bot_state["logs"].insert(0, f"{time_str}|{msg}") 
    if len(bot_state["logs"]) > 60:
        bot_state["logs"].pop()

def check_midnight_settlement():
    current_date = datetime.utcnow().strftime("%Y-%m-%d")
    if current_date != bot_state["last_settlement_date"]:
        curr_sym = get_curr_symbol()
        bot_state["paper_balance"] += bot_state["today_pnl"]
        bot_state["paper_balance"] = round(bot_state["paper_balance"], 2)
        settled_amount = bot_state["today_pnl"]
        bot_state["today_pnl"] = 0.0
        bot_state["last_settlement_date"] = current_date
        add_log(f"🏦 Midnight Settlement: {curr_sym}{settled_amount} moved to Wallet.")
        save_memory()

# 🔑 VIP SUBSCRIPTION VERIFICATION (30-DAY + DEVICE LOCK + REFERRAL ENGINE)
@app.post("/api/verify-vip-key")
async def verify_vip_key(request: Request):
    data = await request.json()
    key = data.get("key", "").strip()
    device_id = data.get("device_id", "").strip()
    referral_code = data.get("referral_code", "").strip()

    if not key:
        return {"status": "error", "message": "Key cannot be empty."}

    if key not in keys_db:
        return {"status": "error", "message": "Invalid Activation Key. Please verify with admin."}

    record = keys_db[key]
    now_dt = datetime.utcnow()

    # Case 1: Key already used previously
    if record["used"]:
        # If device matches, check if still within 30-day window
        if record["device_id"] == device_id:
            try:
                exp_dt = datetime.fromisoformat(record["expires_at"].replace("Z", ""))
                if exp_dt > now_dt:
                    days_left = (exp_dt - now_dt).days + 1
                    return {
                        "status": "success",
                        "message": f"Key verified! Valid for {days_left} remaining day(s).",
                        "expires_at": record["expires_at"]
                    }
                else:
                    return {"status": "error", "message": "This VIP Key has expired. Please renew for ₹199."}
            except:
                pass
        return {"status": "error", "message": "This key has already been activated on another device!"}

    # Case 2: Fresh Key Activation (Strict 30 Days)
    activation_time = now_dt
    expiry_time = activation_time + timedelta(days=30)

    record["used"] = True
    record["device_id"] = device_id if device_id else f"DEV_{int(time.time())}"
    record["activated_at"] = activation_time.isoformat() + "Z"
    record["expires_at"] = expiry_time.isoformat() + "Z"

    # Case 3: Referral Bonus Engine (+10 Days for Referrer)
    if referral_code and referral_code in keys_db and referral_code != key:
        ref_record = keys_db[referral_code]
        if ref_record["used"] and ref_record["expires_at"]:
            try:
                ref_exp = datetime.fromisoformat(ref_record["expires_at"].replace("Z", ""))
                base_time = ref_exp if ref_exp > now_dt else now_dt
                new_ref_exp = base_time + timedelta(days=10)
                ref_record["expires_at"] = new_ref_exp.isoformat() + "Z"
                ref_record["referral_count"] = ref_record.get("referral_count", 0) + 1
                add_log(f"🎁 REFERRAL REWARD: 10 extra days added to referrer key [{referral_code}]!")
            except Exception as ex:
                add_log(f"⚠️ Referral extension error: {str(ex)[:30]}")

    save_keys_database()
    add_log(f"👑 VIP KEY ACTIVATED: [{key}] unlocked for 30 days on device [{record['device_id'][:10]}...].")
    
    return {
        "status": "success",
        "message": "VIP Key verified successfully! 30-Day access granted.",
        "expires_at": record["expires_at"]
    }

# 🌐 UNIVERSAL MARKET DATA FEED
def fetch_active_exchange_markets():
    broker = bot_state.get("active_broker", "paper")
    quote = bot_state.get("quote_currency", "USDT").upper()
    
    if broker == "coindcx":
        try:
            res = requests.get("https://api.coindcx.com/exchange/ticker", timeout=8)
            data = res.json()
            market_list = []
            for item in data:
                m = item.get("market", "")
                is_match = False
                if quote == "INR" and (m.endswith("_INR") or "INR" in m):
                    is_match = True
                elif quote == "USDT" and (m.endswith("_USDT") or "USDT" in m):
                    is_match = True

                if is_match:
                    clean_sym = m.replace("B-", "").replace("I-", "").replace("_", "")
                    try:
                        price = float(item.get("last_price", 0.0))
                        vol = float(item.get("volume", 0.0))
                        change = float(item.get("change_24_hour", 0.0))
                        if price > 0:
                            market_list.append({
                                "symbol": clean_sym,
                                "raw_symbol": m,
                                "price": price,
                                "volume": vol,
                                "change": change
                            })
                    except:
                        continue
            return market_list
        except Exception as e:
            add_log(f"⚠️ CoinDCX Feed Error: {str(e)[:30]}")
            return []

    elif broker in ccxt.exchanges:
        try:
            exchange_class = getattr(ccxt, broker)
            inst = exchange_class({'enableRateLimit': True})
            tickers = inst.fetch_tickers()
            market_list = []
            target_suffix = f"/{quote}"
            for sym, t in tickers.items():
                if sym.endswith(target_suffix):
                    market_list.append({
                        "symbol": sym.replace("/", ""),
                        "raw_symbol": sym,
                        "price": float(t.get("last", 0.0)),
                        "volume": float(t.get("quoteVolume", 0.0) or 0.0),
                        "change": float(t.get("percentage", 0.0) or 0.0)
                    })
            if not market_list and quote == "INR":
                for sym, t in tickers.items():
                    if sym.endswith("/USDT"):
                        market_list.append({
                            "symbol": sym.replace("/", ""),
                            "raw_symbol": sym,
                            "price": float(t.get("last", 0.0)),
                            "volume": float(t.get("quoteVolume", 0.0) or 0.0),
                            "change": float(t.get("percentage", 0.0) or 0.0)
                        })
            return market_list
        except Exception as e:
            add_log(f"⚠️ {broker.upper()} Feed Error: {str(e)[:30]}")

    if quote == "INR":
        try:
            res = requests.get("https://api.coindcx.com/exchange/ticker", timeout=8)
            data = res.json()
            return [{
                "symbol": item.get("market", "").replace("I-", "").replace("B-", "").replace("_", ""),
                "raw_symbol": item.get("market", ""),
                "price": float(item.get("last_price", 0.0)),
                "volume": float(item.get("volume", 0.0)),
                "change": float(item.get("change_24_hour", 0.0))
            } for item in data if item.get("market", "").endswith("_INR") and float(item.get("last_price", 0.0)) > 0]
        except:
            pass

    try:
        res = requests.get("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=8)
        data = res.json()
        return [{
            "symbol": c["symbol"],
            "raw_symbol": c["symbol"],
            "price": float(c["lastPrice"]),
            "volume": float(c["quoteVolume"]),
            "change": float(c["priceChangePercent"])
        } for c in data if c["symbol"].endswith("USDT")]
    except:
        return []

# 🔥 REAL BUY ENGINE (COINDCX DUAL MARKET SUPPORT)
def execute_coindcx_buy(symbol, target_amount=5.0):
    api_key = bot_state.get("api_key", "").strip()
    secret_key = bot_state.get("secret_key", "").strip()
    quote = bot_state.get("quote_currency", "USDT").upper()

    if not api_key or not secret_key:
        return False, None, 0, "API Keys missing"

    try:
        clean_coin = symbol.replace("USDT", "").replace("INR", "").replace("/", "").replace("B-", "").replace("I-", "").replace("_", "").upper()
        ticker_res = requests.get("https://api.coindcx.com/exchange/ticker", timeout=8)
        tickers = ticker_res.json()
        
        target_market = f"I-{clean_coin}_INR" if quote == "INR" else f"B-{clean_coin}_USDT"
        current_price = 0.0
        
        for t in tickers:
            m = t.get('market', '')
            if quote == "INR" and clean_coin in m and "INR" in m:
                target_market = m
                current_price = float(t.get('last_price', 0.0))
                break
            elif quote == "USDT" and clean_coin in m and "USDT" in m:
                target_market = m
                current_price = float(t.get('last_price', 0.0))
                break

        if current_price <= 0:
            return False, None, 0, f"Price not found for {clean_coin} ({quote})"

        calc_qty = target_amount / current_price
        quantity = int(round(calc_qty)) if clean_coin in ["DOGE", "TRX", "SHIB"] else round(calc_qty, 3)
        if quantity <= 0:
            quantity = 1

        time_stamp = int(round(time.time() * 1000))
        body = {
            "side": "buy",
            "order_type": "market_order",
            "market": target_market,
            "total_quantity": quantity,
            "timestamp": time_stamp
        }

        json_body = json.dumps(body, separators=(',', ':'))
        signature = hmac.new(secret_key.encode('utf-8'), json_body.encode('utf-8'), hashlib.sha256).hexdigest()
        headers = {'Content-Type': 'application/json', 'X-AUTH-APIKEY': api_key, 'X-AUTH-SIGNATURE': signature}

        res = requests.post("https://api.coindcx.com/exchange/v1/orders/create", data=json_body, headers=headers, timeout=10)
        res_data = res.json()

        if res.status_code == 200 and ("orders" in res_data or "id" in res_data or isinstance(res_data, list)):
            return True, current_price, quantity, res_data
        else:
            err_msg = res_data.get("message", str(res_data)) if isinstance(res_data, dict) else str(res_data)
            return False, current_price, quantity, err_msg
    except Exception as e:
        return False, 0, 0, str(e)

# 🔥 BULLETPROOF REAL SELL ENGINE (WITH LIVE BALANCE AUDIT)
def execute_coindcx_sell(symbol, quantity=0):
    api_key = bot_state.get("api_key", "").strip()
    secret_key = bot_state.get("secret_key", "").strip()
    quote = bot_state.get("quote_currency", "USDT").upper()

    if not api_key or not secret_key:
        add_log("❌ SELL ABORTED: CoinDCX API Keys missing!")
        return False, "API keys missing"

    try:
        clean_coin = symbol.replace("USDT", "").replace("INR", "").replace("/", "").replace("B-", "").replace("I-", "").replace("_", "").upper()
        
        # Live wallet balance check to handle fee deduction
        timeStamp = int(round(time.time() * 1000))
        bal_body = json.dumps({"timestamp": timeStamp}, separators=(',', ':'))
        bal_sig = hmac.new(secret_key.encode('utf-8'), bal_body.encode('utf-8'), hashlib.sha256).hexdigest()
        bal_headers = {'Content-Type': 'application/json', 'X-AUTH-APIKEY': api_key, 'X-AUTH-SIGNATURE': bal_sig}
        
        bal_res = requests.post("https://api.coindcx.com/exchange/v1/users/balances", data=bal_body, headers=bal_headers, timeout=8)
        bal_data = bal_res.json()
        
        actual_available = 0.0
        if isinstance(bal_data, list):
            for item in bal_data:
                if item.get("currency") == clean_coin:
                    actual_available = float(item.get("balance", 0.0))
                    break

        sell_qty = actual_available if actual_available > 0 else float(quantity)
        
        if clean_coin in ["DOGE", "TRX", "SHIB"]:
            sell_qty = int(sell_qty)
        else:
            sell_qty = round(sell_qty, 3)

        if sell_qty <= 0:
            add_log(f"⚠️ Sell Skipped: 0 {clean_coin} balance on CoinDCX!")
            return False, f"Zero {clean_coin} in wallet"

        ticker_res = requests.get("https://api.coindcx.com/exchange/ticker", timeout=8)
        tickers = ticker_res.json()
        target_market = f"I-{clean_coin}_INR" if quote == "INR" else f"B-{clean_coin}_USDT"
        for t in tickers:
            m = t.get('market', '')
            if quote == "INR" and clean_coin in m and "INR" in m:
                target_market = m
                break
            elif quote == "USDT" and clean_coin in m and "USDT" in m:
                target_market = m
                break

        time_stamp = int(round(time.time() * 1000))
        order_body = {
            "side": "sell",
            "order_type": "market_order",
            "market": target_market,
            "total_quantity": sell_qty,
            "timestamp": time_stamp
        }
        json_order = json.dumps(order_body, separators=(',', ':'))
        sig = hmac.new(secret_key.encode('utf-8'), json_order.encode('utf-8'), hashlib.sha256).hexdigest()
        headers = {'Content-Type': 'application/json', 'X-AUTH-APIKEY': api_key, 'X-AUTH-SIGNATURE': sig}
        
        res = requests.post("https://api.coindcx.com/exchange/v1/orders/create", data=json_order, headers=headers, timeout=10)
        res_data = res.json()

        if res.status_code == 200 and ("orders" in res_data or "id" in res_data or isinstance(res_data, list)):
            add_log(f"📤 REAL EXIT SUCCESS: Sold {sell_qty} {clean_coin} on CoinDCX ({target_market})")
            return True, res_data
        else:
            err_msg = res_data.get("message", str(res_data)) if isinstance(res_data, dict) else str(res_data)
            add_log(f"❌ REAL SELL REJECTED: {err_msg[:40]}")
            return False, err_msg
            
    except Exception as e:
        add_log(f"❌ Sell Exception: {str(e)[:35]}")
        return False, str(e)

# 🔄 DYNAMIC BROKER MODE SWITCH ENDPOINT (Real vs Paper)
@app.post("/api/set-broker-mode")
async def set_broker_mode(request: Request):
    data = await request.json()
    mode = data.get("mode", "paper").lower()
    
    if mode == "real":
        if not bot_state.get("api_key") or not bot_state.get("secret_key"):
            return {"status": "error", "message": "API keys not set. Connect in Portfolio!"}
        bot_state["active_broker"] = "coindcx"
    else:
        bot_state["active_broker"] = "paper"
        
    save_memory()
    add_log(f"⚡ Broker Mode Switched to: {bot_state['active_broker'].upper()}")
    return {"status": "success", "active_broker": bot_state["active_broker"]}

# 🔄 DYNAMIC CURRENCY SWITCH ENDPOINT (INR vs USDT)
@app.post("/api/set-currency")
async def set_currency(request: Request):
    data = await request.json()
    currency = data.get("currency", "USDT").upper()
    if currency not in ["USDT", "INR"]:
        return {"status": "error", "message": "Only 'USDT' and 'INR' are supported"}
    
    bot_state["quote_currency"] = currency
    if currency == "INR" and bot_state["trade_amount"] < 100:
        bot_state["trade_amount"] = 500.0
    elif currency == "USDT" and bot_state["trade_amount"] >= 100:
        bot_state["trade_amount"] = 5.0

    save_memory()
    add_log(f"💱 Currency switched to {currency} ({get_curr_symbol()})")
    return {
        "status": "success",
        "currency": currency,
        "symbol": get_curr_symbol(),
        "trade_amount": bot_state["trade_amount"]
    }

@app.post("/api/connect-exchange")
async def connect_exchange(request: Request):
    data = await request.json()
    exchange_id = data.get("exchange", "binance").lower()
    api_key = data.get("api_key", "").strip()
    secret_key = data.get("secret_key", "").strip()
    force_bot_run = data.get("is_bot_running")

    bot_state["active_broker"] = exchange_id
    bot_state["api_key"] = api_key
    bot_state["secret_key"] = secret_key
    save_memory()

    try:
        if exchange_id == "paper":
            if force_bot_run == True and not bot_state["is_running"]:
                bot_state["is_running"] = True
                add_log("🔄 Bot resumed on Paper Mode.")
            return {"status": "success", "message": "🟢 Paper Trading Synced!", "balances": {bot_state["quote_currency"]: bot_state["paper_balance"]}}
            
        elif exchange_id == "coindcx":
            timeStamp = int(round(time.time() * 1000))
            body = {"timestamp": timeStamp}
            json_body = json.dumps(body, separators=(',', ':'))
            signature = hmac.new(secret_key.encode('utf-8'), json_body.encode('utf-8'), hashlib.sha256).hexdigest()
            headers = {'Content-Type': 'application/json', 'X-AUTH-APIKEY': api_key, 'X-AUTH-SIGNATURE': signature}
            res = requests.post("https://api.coindcx.com/exchange/v1/users/balances", data=json_body, headers=headers, timeout=10)
            res_data = res.json()
            if isinstance(res_data, list):
                dynamic_balances = {item.get("currency"): round(float(item.get("balance", 0.0)), 5) for item in res_data if float(item.get("balance", 0.0)) > 0.00001}
                add_log("🔗 Connected to CoinDCX Direct API.")
                return {"status": "success", "message": "Connected to CoinDCX!", "balances": dynamic_balances}
            else:
                return {"status": "error", "message": "CoinDCX Key Invalid!"}
        else:
            if not hasattr(ccxt, exchange_id): 
                return {"status": "error", "message": f"Exchange '{exchange_id}' not supported."}
            exchange = getattr(ccxt, exchange_id)({'apiKey': api_key, 'secret': secret_key, 'enableRateLimit': True})
            balance = exchange.fetch_balance()
            dynamic_balances = {coin: round(amt, 5) for coin, amt in balance.get('total', {}).items() if isinstance(amt, (int, float)) and amt > 0.00001}
            add_log(f"🔗 Connected to {exchange_id.upper()} via CCXT.")
            return {"status": "success", "message": f"Connected to {exchange_id.upper()}!", "balances": dynamic_balances}
    except Exception as e:
        return {"status": "error", "message": "API Error: Invalid Keys!"}

@app.post("/api/test-coindcx-order")
async def test_coindcx_order(request: Request):
    try:
        data = await request.json()
        api_key = data.get('api_key', bot_state["api_key"]).strip()
        secret_key = data.get('secret_key', bot_state["secret_key"]).strip()

        if not api_key or not secret_key:
            return {"status": "error", "message": "API Keys missing! Connect in Portfolio first."}

        bot_state["active_broker"] = "coindcx"
        bot_state["api_key"] = api_key
        bot_state["secret_key"] = secret_key
        save_memory()

        quote = bot_state.get("quote_currency", "USDT")
        test_amount = 100.0 if quote == "INR" else 5.0
        success, price, qty, res = execute_coindcx_buy(f"DOGE{quote}", test_amount)
        curr_sym = get_curr_symbol()

        if success:
            new_trade = {
                "id": int(time.time()),
                "symbol": f"DOGE{quote}",
                "currency": quote,
                "type": "LONG",
                "entry_price": price,
                "quantity": qty,
                "amount": round(qty * price, 2),
                "highest_price": price,
                "lowest_price": price,
                "sl_price": price * 0.98,
                "time": get_global_time()
            }
            bot_state["active_trades"].insert(0, new_trade)
            save_memory()
            add_log(f"🧪 REAL TEST ORDER: Bought {qty} DOGE at {curr_sym}{price}")
            return {"status": "success", "message": "Order Placed Successfully", "trade": {"symbol": f"DOGE/{quote}", "entry_price": price, "amount": qty}}
        else:
            return {"status": "error", "message": f"CoinDCX: {res}"}
    except Exception as e:
        return {"status": "error", "message": f"Server Error: {str(e)}"}

@app.post("/api/bot-control")
async def bot_control(request: Request):
    data = await request.json()
    action = data.get("action")
    if action == "start":
        bot_state["is_running"] = True
        if "currency" in data and data["currency"].upper() in ["USDT", "INR"]:
            bot_state["quote_currency"] = data["currency"].upper()
        if "amount" in data and float(data["amount"]) > 0:
            bot_state["trade_amount"] = float(data["amount"])
        bot_state["trade_type"] = data.get("trade_type", bot_state["trade_type"])
        bot_state["strategy"] = data.get("strategy", bot_state["strategy"])
        save_memory()
        curr_sym = get_curr_symbol()
        add_log(f"🚀 BOT STARTED on {bot_state['active_broker'].upper()} | {bot_state['quote_currency']} Mode | Lot: {curr_sym}{bot_state['trade_amount']}")
        return {"status": "success", "message": "Bot Started!"}
    elif action == "stop":
        bot_state["is_running"] = False
        save_memory()
        add_log("🛑 BOT STOPPED! Market scanning halted.")
        return {"status": "success", "message": "Bot Stopped!"}

# 🔥 CLOSE TRADE API WITH STRICT REAL EXECUTION HANDSHAKE
@app.post("/api/close-trade")
async def close_trade(request: Request):
    data = await request.json()
    trade_id = data.get("id")
    
    trade_to_close = None
    for t in bot_state["active_trades"]:
        if t["id"] == trade_id:
            trade_to_close = t
            break
            
    if not trade_to_close:
        return {"status": "error", "message": "Trade not found!"}
        
    try:
        if bot_state.get("active_broker") == "coindcx":
            sold, msg = execute_coindcx_sell(trade_to_close.get("symbol", "DOGEUSDT"), trade_to_close.get("quantity", 0))
            if not sold:
                return {"status": "error", "message": f"CoinDCX Exit Failed: {msg}"}

        markets = fetch_active_exchange_markets()
        exit_price = trade_to_close["entry_price"]
        for m in markets:
            if m["symbol"] == trade_to_close["symbol"]:
                exit_price = m["price"]
                break
        
        if trade_to_close["type"] == "LONG":
            pnl_percent = ((exit_price - trade_to_close["entry_price"]) / trade_to_close["entry_price"]) * 100
        else:
            pnl_percent = ((trade_to_close["entry_price"] - exit_price) / trade_to_close["entry_price"]) * 100
            
        trade_amount = trade_to_close.get("amount", 5.0)
        pnl_val = (trade_amount * pnl_percent) / 100
        
        trade_to_close["pnl_percent"] = round(pnl_percent, 2)
        trade_to_close["pnl_val"] = round(pnl_val, 2)
        trade_to_close["exit_price"] = exit_price
        trade_to_close["close_time"] = get_global_time()
        
        if bot_state["active_broker"] == "paper":
            bot_state["today_pnl"] += trade_to_close["pnl_val"]
            bot_state["today_pnl"] = round(bot_state["today_pnl"], 2)

        bot_state["active_trades"].remove(trade_to_close)
        bot_state["trade_history"].insert(0, trade_to_close)
        if len(bot_state["trade_history"]) > 30: bot_state["trade_history"].pop()
        
        save_memory()
        return {"status": "success", "message": f"Exit Confirmed on CoinDCX! PNL: {trade_to_close['pnl_percent']}%"}
    except Exception as e:
        return {"status": "error", "message": f"API Error: {str(e)}"}

@app.get("/api/bot-logs")
def get_bot_logs():
    return {
        "status": "success", 
        "is_running": bot_state["is_running"], 
        "logs": bot_state["logs"],
        "active_broker": bot_state["active_broker"],
        "quote_currency": bot_state["quote_currency"],
        "currency_symbol": get_curr_symbol(),
        "trade_amount": bot_state["trade_amount"]
    }

@app.get("/api/get-trades")
def get_trades():
    return {
        "status": "success", 
        "active": bot_state["active_trades"], 
        "history": bot_state["trade_history"],
        "paper_balance": bot_state["paper_balance"],
        "quote_currency": bot_state["quote_currency"],
        "currency_symbol": get_curr_symbol(),
        "today_pnl": bot_state["today_pnl"] 
    }

# 🔄 SCANNER LOOP (DUAL CURRENCY & SMART AUTO EXECUTION)
async def market_scanner_loop():
    while True:
        check_midnight_settlement() 
        
        if bot_state["is_running"]:
            try:
                all_coins = fetch_active_exchange_markets()
                if not all_coins:
                    await asyncio.sleep(4)
                    continue

                live_prices = {c['symbol']: c['price'] for c in all_coins}
                
                # Active Trades TSL Check
                trades_to_close = []
                for trade in bot_state["active_trades"]:
                    sym = trade["symbol"]
                    if sym in live_prices:
                        curr_p = live_prices[sym]
                        if trade["type"] == "LONG":
                            if curr_p > trade["highest_price"]:
                                trade["highest_price"] = curr_p
                                trade["sl_price"] = max(trade["sl_price"], curr_p * 0.98)
                            if curr_p <= trade["sl_price"]:
                                trades_to_close.append(trade)
                
                for trade in trades_to_close:
                    exit_p = live_prices[trade["symbol"]]
                    
                    if bot_state.get("active_broker") == "coindcx":
                        execute_coindcx_sell(trade.get("symbol", "DOGEUSDT"), trade.get("quantity", 0))

                    pnl_percent = ((exit_p - trade["entry_price"]) / trade["entry_price"]) * 100
                    trade_amt = trade.get("amount", 5.0)
                    pnl_val = (trade_amt * pnl_percent) / 100
                    
                    trade["pnl_percent"] = round(pnl_percent, 2)
                    trade["pnl_val"] = round(pnl_val, 2)
                    trade["exit_price"] = exit_p
                    trade["close_time"] = get_global_time()
                    
                    if bot_state["active_broker"] == "paper":
                        bot_state["today_pnl"] += trade["pnl_val"]
                        bot_state["today_pnl"] = round(bot_state["today_pnl"], 2)

                    bot_state["active_trades"].remove(trade)
                    bot_state["trade_history"].insert(0, trade)
                    if len(bot_state["trade_history"]) > 30: bot_state["trade_history"].pop()
                    add_log(f"🔔 TSL HIT: {trade['symbol']} | P&L: {trade['pnl_percent']}%")
                
                save_memory()

                # Scanner Auto Trigger
                if len(bot_state["active_trades"]) < 1:
                    order_amount = bot_state["trade_amount"]
                    quote = bot_state.get("quote_currency", "USDT")
                    curr_sym = get_curr_symbol()

                    target_coin = next((c for c in all_coins if "DOGE" in c['symbol']), None)
                    if not target_coin and all_coins:
                        target_coin = all_coins[0]

                    if target_coin:
                        coin_sym = target_coin['symbol']
                        current_p = target_coin['price']

                        if bot_state["active_broker"] == "coindcx":
                            success, buy_price, buy_qty, res = execute_coindcx_buy(coin_sym, order_amount)
                            if success:
                                new_trade = {
                                    "id": int(time.time()),
                                    "symbol": coin_sym,
                                    "currency": quote,
                                    "type": "LONG",
                                    "entry_price": buy_price,
                                    "quantity": buy_qty,
                                    "amount": round(buy_qty * buy_price, 2),
                                    "highest_price": buy_price,
                                    "lowest_price": buy_price,
                                    "sl_price": buy_price * 0.98,
                                    "time": get_global_time()
                                }
                                bot_state["active_trades"].insert(0, new_trade)
                                save_memory()
                                add_log(f"⚡ REAL BUY: {buy_qty} {coin_sym} at {curr_sym}{buy_price}")
                            else:
                                add_log(f"⚠️ Auto-Buy Failed: {str(res)[:35]}")
                        
                        elif bot_state["active_broker"] == "paper":
                            calc_qty = int(order_amount / current_p) if current_p > 0 else 1
                            new_trade = {
                                "id": int(time.time()),
                                "symbol": coin_sym,
                                "currency": quote,
                                "type": "LONG",
                                "entry_price": current_p,
                                "quantity": calc_qty,
                                "amount": order_amount,
                                "highest_price": current_p,
                                "lowest_price": current_p,
                                "sl_price": current_p * 0.98,
                                "time": get_global_time()
                            }
                            bot_state["active_trades"].insert(0, new_trade)
                            save_memory()
                            add_log(f"⚡ [PAPER] BUY: {coin_sym} at {curr_sym}{current_p}")

                        await asyncio.sleep(5)
                        
            except Exception as e:
                add_log(f"❌ Scanner Loop Error: {str(e)[:35]}")
        
        await asyncio.sleep(3)

@app.on_event("startup")
async def startup(): 
    asyncio.create_task(market_scanner_loop())

@app.get("/")
def root(): 
    return {
        "status": "HiTech AI Engine Live!", 
        "broker": bot_state["active_broker"], 
        "currency": bot_state["quote_currency"],
        "symbol": get_curr_symbol(),
        "total_vip_keys": len(keys_db)
    }

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
