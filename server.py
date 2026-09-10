import os
import time
import hmac
import hashlib
import json
import sqlite3
import requests
import ccxt
import uvicorn
import asyncio
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager

# ----------------- DATABASE SETUP (PERMANENT HISTORY) -----------------
DB_FILE = "trades_history.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id TEXT UNIQUE,
            device_id TEXT,
            symbol TEXT,
            currency TEXT,
            side TEXT,
            entry_price REAL,
            exit_price REAL,
            quantity REAL,
            amount REAL,
            sl_price REAL,
            target_price REAL,
            pnl_percent REAL,
            pnl_val REAL,
            status TEXT,
            broker TEXT,
            close_time TEXT
        )
    """)
    conn.commit()
    conn.close()

init_db()

def db_save_trade(trade: dict, device_id: str, broker: str):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO trades (
                trade_id, device_id, symbol, currency, side, entry_price, 
                exit_price, quantity, amount, sl_price, target_price, 
                pnl_percent, pnl_val, status, broker, close_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            str(trade.get("id", int(time.time()))),
            device_id,
            trade.get("symbol", ""),
            trade.get("currency", "INR"),
            trade.get("type", "BUY"),
            float(trade.get("entry_price", 0.0)),
            float(trade.get("exit_price", 0.0)),
            float(trade.get("quantity", 0.0)),
            float(trade.get("amount", 0.0)),
            float(trade.get("sl_price", 0.0)),
            float(trade.get("target_price", 0.0)),
            float(trade.get("pnl_percent", 0.0)),
            float(trade.get("pnl_val", 0.0)),
            trade.get("status", "CLOSED"),
            broker,
            trade.get("close_time", datetime.now(timezone.utc).isoformat() + "Z")
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"DB Error: {e}")

def db_get_all_trades(device_id: str, limit: int = 1000):
    try:
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM trades 
            WHERE device_id = ? OR device_id = 'DEFAULT_DEVICE'
            ORDER BY id DESC LIMIT ?
        """, (device_id, limit))
        rows = cursor.fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []

# ----------------- APP LIFECYCLE & STATE -----------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    scanner_task = asyncio.create_task(market_scanner_loop())
    yield
    scanner_task.cancel()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

user_sessions = {}

def get_user_session(device_id: str):
    if not device_id:
        device_id = "DEFAULT_DEVICE"
    if device_id not in user_sessions:
        user_sessions[device_id] = {
            "is_running": False,
            "active_broker": "coindcx",
            "market_mode": "spot",
            "api_key": "",
            "secret_key": "",
            "quote_currency": "INR",
            "trade_amount": 500.0,
            "trade_type": "intraday",
            "strategy": "volume",
            "deal_condition": "ASAP",
            "selected_coin": "AUTO",
            "logs": ["🤖 Master AI Dual Engine Initialized. Ready for Real & Paper Trading."],
            "active_trades": [],
            "paper_balance": 500000.0,
            "today_pnl": 0.0,
            "session_start_fund": 744.0,
            "sleep_until": None,
            "sleep_reason": "",
            "last_settlement_date": datetime.now(timezone.utc).strftime("%Y-%m-%d")
        }
    return user_sessions[device_id]

KEYS_DB_FILE = "keys_db.json"
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
    return datetime.now(timezone.utc).isoformat() + "Z"

def get_curr_symbol(state):
    return "₹" if state.get("quote_currency") == "INR" else "$"

def add_log(state, msg):
    time_str = get_global_time()
    state["logs"].insert(0, f"{time_str}|{msg}")
    if len(state["logs"]) > 80:
        state["logs"].pop()

def check_midnight_settlement(state):
    current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if current_date != state["last_settlement_date"]:
        curr_sym = get_curr_symbol(state)
        state["paper_balance"] += state["today_pnl"]
        state["paper_balance"] = round(state["paper_balance"], 2)
        settled_amount = state["today_pnl"]
        state["today_pnl"] = 0.0
        state["last_settlement_date"] = current_date
        add_log(state, f"🏦 Midnight Settlement: {curr_sym}{settled_amount} moved to Wallet.")

# ----------------- VIP ACTIVATION ENDPOINT -----------------
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
    now_dt = datetime.now(timezone.utc)

    if record["used"]:
        if record["device_id"] == device_id:
            try:
                exp_dt = datetime.fromisoformat(record["expires_at"].replace("Z", "+00:00"))
                if exp_dt > now_dt:
                    days_left = (exp_dt - now_dt).days + 1
                    return {
                        "status": "success",
                        "message": f"Key verified! Valid for {days_left} remaining day(s).",
                        "expires_at": record["expires_at"]
                    }
                else:
                    return {"status": "error", "message": "This VIP Key has expired. Please renew."}
            except:
                pass
        return {"status": "error", "message": "This key has already been activated on another device!"}

    activation_time = now_dt
    expiry_time = activation_time + timedelta(days=30)

    record["used"] = True
    record["device_id"] = device_id if device_id else f"DEV_{int(time.time())}"
    record["activated_at"] = activation_time.isoformat()
    record["expires_at"] = expiry_time.isoformat()

    if referral_code and referral_code in keys_db and referral_code != key:
        ref_record = keys_db[referral_code]
        if ref_record["used"] and ref_record["expires_at"]:
            try:
                ref_exp = datetime.fromisoformat(ref_record["expires_at"].replace("Z", "+00:00"))
                base_time = ref_exp if ref_exp > now_dt else now_dt
                new_ref_exp = base_time + timedelta(days=10)
                ref_record["expires_at"] = new_ref_exp.isoformat()
                ref_record["referral_count"] = ref_record.get("referral_count", 0) + 1
                add_log(get_user_session(ref_record["device_id"]), "🎁 REFERRAL REWARD: 10 extra days added!")
            except:
                pass

    save_keys_database()
    return {
        "status": "success",
        "message": "VIP Key verified successfully! 30-Day access granted.",
        "expires_at": record["expires_at"]
    }

# ----------------- MARKET SCANNER ENGINE -----------------
def fetch_active_exchange_markets(state):
    broker = state.get("active_broker", "coindcx")
    quote = state.get("quote_currency", "INR").upper()

    if broker == "coindcx":
        try:
            res = requests.get("https://api.coindcx.com/exchange/ticker", timeout=8)
            data = res.json()
            market_list = []
            liquid_bases = ["BTC", "ETH", "SOL", "DOGE", "SHIB", "XRP", "ADA", "MATIC", "TRX", "PEPE", "LTC", "NEAR", "SUI"]

            for item in data:
                m = item.get("market", "")
                price = float(item.get("last_price", 0.0))
                vol = float(item.get("volume", 0.0))
                change = float(item.get("change_24_hour", 0.0))
                if price <= 0:
                    continue

                clean_coin = m.replace("B-", "").replace("I-", "").replace("_", "").replace("INR", "").replace("USDT", "").upper()

                if quote == "INR" and (m.endswith("_INR") or m.endswith("INR")) and not ("USDT" in m):
                    if clean_coin in liquid_bases or vol > 100:
                        market_list.append({
                            "symbol": clean_coin + "INR",
                            "base_coin": clean_coin,
                            "raw_symbol": m,
                            "price": price,
                            "volume": vol,
                            "change": change
                        })
                elif quote == "USDT" and (m.endswith("_USDT") or m.endswith("USDT")) and not ("INR" in m):
                    if clean_coin in liquid_bases or vol > 1000:
                        market_list.append({
                            "symbol": clean_coin + "USDT",
                            "base_coin": clean_coin,
                            "raw_symbol": m,
                            "price": price,
                            "volume": vol,
                            "change": change
                        })

            if market_list:
                return market_list
        except Exception:
            pass

    if broker in ccxt.exchanges:
        try:
            exchange_class = getattr(ccxt, broker)
            inst = exchange_class({'enableRateLimit': True})
            tickers = inst.fetch_tickers()
            market_list = []
            target_suffix = f"/{quote}"
            for sym, t in tickers.items():
                if sym.endswith(target_suffix):
                    c_base = sym.split("/")[0]
                    market_list.append({
                        "symbol": sym.replace("/", ""),
                        "base_coin": c_base,
                        "raw_symbol": sym,
                        "price": float(t.get("last", 0.0)),
                        "volume": float(t.get("quoteVolume", 0.0) or 0.0),
                        "change": float(t.get("percentage", 0.0) or 0.0)
                    })
            if market_list:
                return market_list
        except Exception:
            pass

    try:
        res = requests.get("https://data-api.binance.vision/api/v3/ticker/24hr", timeout=8)
        data = res.json()
        usd_to_inr = 89.5
        out = []
        for c in data:
            if c["symbol"].endswith("USDT") and not any(x in c["symbol"] for x in ["CREAM", "UP", "DOWN"]):
                p = float(c["lastPrice"])
                clean_sym = c["symbol"].replace("USDT", "")
                final_p = p * usd_to_inr if quote == "INR" else p
                out.append({
                    "symbol": clean_sym + quote,
                    "base_coin": clean_sym,
                    "raw_symbol": c["symbol"],
                    "price": final_p,
                    "volume": float(c["quoteVolume"]),
                    "change": float(c["priceChangePercent"])
                })
        return out
    except:
        return []

def get_coin_precision(clean_coin, current_price):
    if "BTC" in clean_coin:
        return 5
    elif "ETH" in clean_coin:
        return 4
    elif any(c in clean_coin for c in ["SOL", "BNB", "LTC", "AVAX"]):
        return 2
    elif any(c in clean_coin for c in ["DOGE", "XRP", "ADA", "TRX", "MATIC"]):
        return 1 if current_price > 10 else 0
    elif any(c in clean_coin for c in ["SHIB", "PEPE", "BONK", "FLOKI", "BRISE"]):
        return 0
    else:
        if current_price < 20:
            return 0
        elif current_price < 1000:
            return 2
        else:
            return 4

# ----------------- COINDCX LIVE POSITION SYNC -----------------
def get_coindcx_live_portfolio(state):
    api_key = state.get("api_key", "").strip()
    secret_key = state.get("secret_key", "").strip()
    if not api_key or not secret_key:
        return []

    try:
        time_stamp = int(round(time.time() * 1000))
        body = {"timestamp": time_stamp}
        json_body = json.dumps(body, separators=(',', ':'))
        signature = hmac.new(secret_key.encode('utf-8'), json_body.encode('utf-8'), hashlib.sha256).hexdigest()
        headers = {'Content-Type': 'application/json', 'X-AUTH-APIKEY': api_key, 'X-AUTH-SIGNATURE': signature}
        
        res = requests.post("https://api.coindcx.com/exchange/v1/users/balances", data=json_body, headers=headers, timeout=8)
        data = res.json()
        
        holdings = []
        if isinstance(data, list):
            for item in data:
                bal = float(item.get("balance", 0.0))
                curr = item.get("currency", "").upper()
                if curr not in ["INR", "USDT"] and bal > 0:
                    holdings.append({"coin": curr, "balance": bal})
        return holdings
    except Exception:
        return []

# ----------------- ORDER EXECUTION (STRICT USER SYMBOL - NO BTC FALLBACK) -----------------
def execute_coindcx_order(state, raw_symbol, side="buy", target_amount=100.0, exact_qty=0):
    api_key = state.get("api_key", "").strip()
    secret_key = state.get("secret_key", "").strip()
    quote = state.get("quote_currency", "INR").upper()

    if not api_key or not secret_key:
        return False, 0, 0, "API Keys missing! Connect in Portfolio tab."

    try:
        clean_coin = raw_symbol.replace("USDT", "").replace("INR", "").replace("/", "").replace("B-", "").replace("I-", "").replace("_", "").strip().upper()

        ticker_res = requests.get("https://api.coindcx.com/exchange/ticker", timeout=8)
        tickers = ticker_res.json()

        target_market = None
        current_price = 0.0

        for t in tickers:
            m = t.get('market', '')
            if quote == "INR":
                if m == f"{clean_coin}INR" or m == f"I-{clean_coin}_INR" or m == f"B-{clean_coin}_INR":
                    target_market = m
                    current_price = float(t.get('last_price', 0.0))
                    break
            else:
                if m == f"{clean_coin}USDT" or m == f"B-{clean_coin}_USDT":
                    target_market = m
                    current_price = float(t.get('last_price', 0.0))
                    break

        if not target_market or current_price <= 0:
            return False, 0, 0, f"Valid CoinDCX pair for '{clean_coin}' ({quote}) not found! Please check coin symbol."

        precision = get_coin_precision(clean_coin, current_price)

        if exact_qty > 0:
            quantity = exact_qty if precision == 0 else round(exact_qty, precision)
            if precision == 0: quantity = int(round(quantity))
        else:
            calc_qty = float(target_amount) / current_price
            if precision == 0:
                quantity = int(round(calc_qty))
                if quantity <= 0: quantity = 1
            else:
                quantity = round(calc_qty, precision)

        if side.lower() == "buy" and quote == "INR" and (quantity * current_price) < 102.0:
            if precision == 0:
                quantity = int(round(115.0 / current_price)) + 1
            else:
                quantity = round(115.0 / current_price, precision)
                if (quantity * current_price) < 100.0:
                    quantity += round(1.0 / (10 ** precision), precision)

        if quantity <= 0:
            quantity = 1 if precision == 0 else round(1.0 / (10 ** precision), precision)

        time_stamp = int(round(time.time() * 1000))
        body = {
            "side": side.lower(),
            "order_type": "market_order",
            "market": target_market,
            "total_quantity": quantity,
            "timestamp": time_stamp
        }

        json_body = json.dumps(body, separators=(',', ':'))
        signature = hmac.new(secret_key.encode('utf-8'), json_body.encode('utf-8'), hashlib.sha256).hexdigest()
        headers = {
            'Content-Type': 'application/json',
            'X-AUTH-APIKEY': api_key,
            'X-AUTH-SIGNATURE': signature
        }

        res = requests.post("https://api.coindcx.com/exchange/v1/orders/create", data=json_body, headers=headers, timeout=10)
        res_data = res.json()

        if res.status_code == 200 and ("orders" in res_data or "id" in res_data or isinstance(res_data, list)):
            return True, current_price, quantity, res_data
        else:
            err_msg = res_data.get("message", str(res_data)) if isinstance(res_data, dict) else str(res_data)
            return False, current_price, quantity, err_msg
    except Exception as e:
        return False, 0, 0, str(e)

# ----------------- DIRECT SELL (ASSET EXIT) -----------------
@app.post("/api/direct-sell")
async def direct_sell(request: Request):
    try:
        data = await request.json()
        device_id = data.get("device_id", "DEFAULT_DEVICE")
        state = get_user_session(device_id)

        coin = data.get("symbol", "").strip().upper()
        quantity = float(data.get("quantity", 0))

        api_key = data.get("api_key", state.get("api_key", "")).strip()
        secret_key = data.get("secret_key", state.get("secret_key", "")).strip()

        if not api_key or not secret_key:
            return {"status": "error", "message": "API Keys missing! Reconnect in Portfolio."}

        state["api_key"] = api_key
        state["secret_key"] = secret_key

        sold, current_price, sell_qty, res_data = execute_coindcx_order(state, coin, side="sell", exact_qty=quantity)

        if sold:
            sold_trade = {
                "id": int(time.time()),
                "symbol": f"{coin}INR",
                "currency": "INR",
                "type": "SELL",
                "entry_price": current_price,
                "exit_price": current_price,
                "quantity": sell_qty,
                "amount": round(sell_qty * current_price, 2),
                "sl_price": 0.0,
                "target_price": 0.0,
                "pnl_percent": 0.0,
                "pnl_val": 0.0,
                "status": "MANUAL EXIT",
                "close_time": get_global_time()
            }
            # Save permanently to DB
            db_save_trade(sold_trade, device_id, state.get("active_broker", "coindcx"))
            add_log(state, f"✅ MANUAL EXIT FILLED: Sold {sell_qty} {coin} at ₹{current_price} on CoinDCX!")
            return {"status": "success", "message": f"Successfully Sold {sell_qty} {coin} on CoinDCX!"}
        else:
            return {"status": "error", "message": f"CoinDCX Exit Failed: {res_data}"}

    except Exception as e:
        return {"status": "error", "message": f"Server Error: {str(e)}"}

# ----------------- USER COMMAND / AUTO EXECUTE ORDER -----------------
@app.post("/api/execute-order")
async def execute_order(request: Request):
    try:
        data = await request.json()
        device_id = data.get("device_id", "DEFAULT_DEVICE")
        state = get_user_session(device_id)

        exchange = data.get("exchange", state.get("active_broker", "coindcx")).lower()
        api_key = data.get("api_key", state.get("api_key", "")).strip()
        secret_key = data.get("secret_key", state.get("secret_key", "")).strip()
        
        # User specified symbol or default
        symbol = data.get("symbol", "").upper().strip()
        if not symbol:
            symbol = "BTCINR" if state.get("quote_currency") == "INR" else "BTCUSDT"

        currency = data.get("currency", state.get("quote_currency", "INR")).upper()
        amount = float(data.get("amount", state.get("trade_amount", 500)))
        side = data.get("side", "BUY").lower()
        mode = data.get("mode", state.get("market_mode", "spot")).lower()

        sl_pct = float(data.get("sl_percent", 2.0)) / 100.0
        target_pct = float(data.get("target_percent", 1.5)) / 100.0

        state["active_broker"] = exchange
        state["quote_currency"] = currency
        state["market_mode"] = mode
        if api_key: state["api_key"] = api_key
        if secret_key: state["secret_key"] = secret_key

        curr_sym = get_curr_symbol(state)

        if exchange == "coindcx":
            success, price, qty, res = execute_coindcx_order(state, symbol, side=side, target_amount=amount)
            if success:
                new_trade = {
                    "id": int(time.time()),
                    "symbol": symbol,
                    "currency": currency,
                    "type": "LONG" if side == "buy" else "SHORT",
                    "entry_price": price,
                    "quantity": qty,
                    "amount": round(qty * price, 2),
                    "highest_price": price,
                    "lowest_price": price,
                    "sl_price": price * (1.0 - sl_pct) if side == "buy" else price * (1.0 + sl_pct),
                    "target_price": price * (1.0 + target_pct) if side == "buy" else price * (1.0 - target_pct),
                    "time": get_global_time()
                }
                state["active_trades"].insert(0, new_trade)
                add_log(state, f"✅ USER ORDER FILLED: {qty} {symbol} at {curr_sym}{price} on CoinDCX")
                return {"status": "success", "message": f"Real {side.upper()} order filled on CoinDCX!", "price": price, "qty": qty}
            else:
                add_log(state, f"❌ CoinDCX Rejected: {res}")
                return {"status": "error", "message": str(res)}

        else:
            markets = fetch_active_exchange_markets(state)
            clean_coin = symbol.replace("INR", "").replace("USDT", "")
            match = next((m for m in markets if clean_coin in m["symbol"]), None)
            sim_price = match["price"] if match else (8500000.0 if "BTC" in symbol else 150.0)
            calc_qty = int(amount / sim_price) if sim_price < 20 else round(amount / sim_price, 4)
            if calc_qty <= 0: calc_qty = 1

            new_trade = {
                "id": int(time.time()),
                "symbol": symbol,
                "currency": currency,
                "type": "LONG" if side == "buy" else "SHORT",
                "entry_price": sim_price,
                "quantity": calc_qty,
                "amount": amount,
                "highest_price": sim_price,
                "lowest_price": sim_price,
                "sl_price": sim_price * (1.0 - sl_pct) if side == "buy" else sim_price * (1.0 + sl_pct),
                "target_price": sim_price * (1.0 + target_pct) if side == "buy" else sim_price * (1.0 - target_pct),
                "time": get_global_time()
            }
            state["active_trades"].insert(0, new_trade)
            add_log(state, f"⚡ [PAPER] {side.upper()}: {calc_qty} {symbol} at {curr_sym}{sim_price}")
            return {"status": "success", "message": f"Paper {side.upper()} order placed!", "price": sim_price, "qty": calc_qty}

    except Exception as e:
        return {"status": "error", "message": f"Execution Error: {str(e)}"}

# ----------------- TRADING CONTROLS -----------------
@app.post("/api/set-market-mode")
async def set_market_mode(request: Request):
    data = await request.json()
    state = get_user_session(data.get("device_id", ""))
    mode = data.get("mode", "spot").lower()
    if mode in ["spot", "futures"]:
        state["market_mode"] = mode
        add_log(state, f"🎯 Market Mode Changed to: {mode.upper()}")
        return {"status": "success", "market_mode": mode}
    return {"status": "error", "message": "Mode must be 'spot' or 'futures'"}

@app.post("/api/set-broker-mode")
async def set_broker_mode(request: Request):
    data = await request.json()
    state = get_user_session(data.get("device_id", ""))
    mode = data.get("mode", "real").lower()
    state["active_broker"] = "coindcx" if mode == "real" else "paper"
    add_log(state, f"⚡ Broker Switched to: {state['active_broker'].upper()}")
    return {"status": "success", "active_broker": state["active_broker"]}

@app.post("/api/set-currency")
async def set_currency(request: Request):
    data = await request.json()
    state = get_user_session(data.get("device_id", ""))
    currency = data.get("currency", "INR").upper()
    if currency not in ["USDT", "INR"]:
        return {"status": "error", "message": "Only 'USDT' and 'INR' are supported"}

    state["quote_currency"] = currency
    state["trade_amount"] = 500.0 if currency == "INR" else 5.0
    add_log(state, f"💱 Currency switched to {currency} ({get_curr_symbol(state)})")
    return {
        "status": "success",
        "currency": currency,
        "symbol": get_curr_symbol(state),
        "trade_amount": state["trade_amount"]
    }

@app.post("/api/connect-exchange")
async def connect_exchange(request: Request):
    data = await request.json()
    state = get_user_session(data.get("device_id", ""))
    exchange_id = data.get("exchange", "coindcx").lower()
    api_key = data.get("api_key", "").strip()
    secret_key = data.get("secret_key", "").strip()

    state["active_broker"] = exchange_id
    state["api_key"] = api_key
    state["secret_key"] = secret_key

    try:
        if exchange_id == "paper":
            return {"status": "success", "message": "🟢 Paper Trading Synced!", "balances": {state["quote_currency"]: state["paper_balance"]}}
        elif exchange_id == "coindcx":
            timeStamp = int(round(time.time() * 1000))
            body = {"timestamp": timeStamp}
            json_body = json.dumps(body, separators=(',', ':'))
            signature = hmac.new(secret_key.encode('utf-8'), json_body.encode('utf-8'), hashlib.sha256).hexdigest()
            headers = {'Content-Type': 'application/json', 'X-AUTH-APIKEY': api_key, 'X-AUTH-SIGNATURE': signature}
            res = requests.post("https://api.coindcx.com/exchange/v1/users/balances", data=json_body, headers=headers, timeout=10)
            res_data = res.json()
            if isinstance(res_data, list):
                dynamic_balances = {item.get("currency"): round(float(item.get("balance", 0.0)), 8) for item in res_data if float(item.get("balance", 0.0)) > 0.00000001}
                inr_bal = dynamic_balances.get("INR", 744.0)
                state["session_start_fund"] = inr_bal if state["quote_currency"] == "INR" else dynamic_balances.get("USDT", 8.31)
                add_log(state, f"🔗 Connected to CoinDCX! Live Cash: {get_curr_symbol(state)}{state['session_start_fund']}")
                return {"status": "success", "message": "Connected to CoinDCX!", "balances": dynamic_balances}
            else:
                return {"status": "error", "message": "CoinDCX Key Invalid!"}
        else:
            if not hasattr(ccxt, exchange_id):
                return {"status": "error", "message": f"Exchange '{exchange_id}' not supported."}
            exchange = getattr(ccxt, exchange_id)({'apiKey': api_key, 'secret': secret_key, 'enableRateLimit': True})
            balance = exchange.fetch_balance()
            dynamic_balances = {coin: round(amt, 8) for coin, amt in balance.get('total', {}).items() if isinstance(amt, (int, float)) and amt > 0.00000001}
            return {"status": "success", "message": f"Connected to {exchange_id.upper()}!", "balances": dynamic_balances}
    except Exception as e:
        return {"status": "error", "message": f"API Error: {str(e)}"}

@app.post("/api/bot-control")
async def bot_control(request: Request):
    data = await request.json()
    device_id = data.get("device_id", "DEFAULT_DEVICE")
    state = get_user_session(device_id)
    action = data.get("action")
    if action == "start":
        now_ts = time.time()
        if state.get("sleep_until") and now_ts < state["sleep_until"]:
            hrs_left = round((state["sleep_until"] - now_ts) / 3600, 1)
            return {"status": "error", "message": f"Bot is in 12-Hour Cooldown Mode ({state['sleep_reason']}). {hrs_left}h remaining!"}

        state["is_running"] = True
        if "currency" in data: state["quote_currency"] = data["currency"].upper()
        if "amount" in data: state["trade_amount"] = float(data["amount"])
        if "exchange" in data: state["active_broker"] = data["exchange"].lower()
        if "deal_condition" in data: state["deal_condition"] = data["deal_condition"]
        if "target_coin" in data: state["selected_coin"] = data["target_coin"].upper()

        curr_sym = get_curr_symbol(state)
        target_info = state["selected_coin"] if state["selected_coin"] != "AUTO" else "Top Gainer Scanner"
        add_log(state, f"🚀 BOT STARTED | Target: {target_info} | Broker: {state['active_broker'].upper()} | Lot: {curr_sym}{state['trade_amount']}")
        return {"status": "success", "message": "Bot Started!"}
    elif action == "stop":
        state["is_running"] = False
        add_log(state, "🛑 BOT STOPPED! Market scanning halted.")
        return {"status": "success", "message": "Bot Stopped!"}

@app.post("/api/close-trade")
async def close_trade(request: Request):
    data = await request.json()
    device_id = data.get("device_id", "DEFAULT_DEVICE")
    state = get_user_session(device_id)
    trade_id = data.get("id")

    trade_to_close = next((t for t in state["active_trades"] if t["id"] == trade_id), None)
    if not trade_to_close:
        return await direct_sell(request)

    try:
        side_to_exit = "sell" if trade_to_close["type"] == "LONG" else "buy"
        if state.get("active_broker") == "coindcx":
            sold, exit_price, _, msg = execute_coindcx_order(state, trade_to_close.get("symbol"), side=side_to_exit, exact_qty=trade_to_close.get("quantity", 0))
            if not sold:
                return {"status": "error", "message": f"CoinDCX Exit Failed: {msg}"}
        else:
            markets = fetch_active_exchange_markets(state)
            exit_price = next((m["price"] for m in markets if m["symbol"] == trade_to_close["symbol"]), trade_to_close["entry_price"])

        if trade_to_close["type"] == "LONG":
            pnl_percent = ((exit_price - trade_to_close["entry_price"]) / trade_to_close["entry_price"]) * 100
        else:
            pnl_percent = ((trade_to_close["entry_price"] - exit_price) / trade_to_close["entry_price"]) * 100

        trade_amount = trade_to_close.get("amount", 500.0)
        pnl_val = (trade_amount * pnl_percent) / 100

        trade_to_close["pnl_percent"] = round(pnl_percent, 2)
        trade_to_close["pnl_val"] = round(pnl_val, 2)
        trade_to_close["exit_price"] = exit_price
        trade_to_close["close_time"] = get_global_time()
        trade_to_close["status"] = "MANUAL EXIT"

        state["today_pnl"] += trade_to_close["pnl_val"]
        state["today_pnl"] = round(state["today_pnl"], 2)

        state["active_trades"].remove(trade_to_close)
        
        # Save to SQLite DB
        db_save_trade(trade_to_close, device_id, state.get("active_broker", "coindcx"))

        return {"status": "success", "message": f"Exit Confirmed! PNL: {trade_to_close['pnl_percent']}%"}
    except Exception as e:
        return {"status": "error", "message": f"API Error: {str(e)}"}

@app.get("/api/bot-logs")
def get_bot_logs(device_id: str = "DEFAULT_DEVICE"):
    state = get_user_session(device_id)
    now_ts = time.time()
    is_sleeping = bool(state.get("sleep_until") and now_ts < state["sleep_until"])
    sleep_left_h = round((state["sleep_until"] - now_ts) / 3600, 1) if is_sleeping else 0
    return {
        "status": "success",
        "is_running": state["is_running"],
        "is_sleeping": is_sleeping,
        "sleep_hours_left": sleep_left_h,
        "market_mode": state["market_mode"],
        "logs": state["logs"],
        "active_broker": state["active_broker"],
        "quote_currency": state["quote_currency"],
        "currency_symbol": get_curr_symbol(state),
        "trade_amount": state["trade_amount"]
    }

# ----------------- GET TRADES WITH LIVE PORTFOLIO & DATABASE SYNC -----------------
@app.get("/api/get-trades")
def get_trades(device_id: str = "DEFAULT_DEVICE"):
    state = get_user_session(device_id)

    # 1. CoinDCX Portfolio Direct Sync (Active Positions Bug Fix)
    if state.get("active_broker") == "coindcx" and state.get("api_key"):
        holdings = get_coindcx_live_portfolio(state)
        existing_coins = [t["symbol"].replace("INR", "").replace("USDT", "") for t in state["active_trades"]]
        
        for h in holdings:
            c = h["coin"]
            if c not in existing_coins:
                state["active_trades"].append({
                    "id": int(time.time()),
                    "symbol": f"{c}INR" if state["quote_currency"] == "INR" else f"{c}USDT",
                    "currency": state["quote_currency"],
                    "type": "LONG",
                    "entry_price": 0.0,
                    "quantity": h["balance"],
                    "amount": 0.0,
                    "highest_price": 0.0,
                    "lowest_price": 0.0,
                    "sl_price": 0.0,
                    "target_price": 0.0,
                    "time": get_global_time()
                })

    # 2. Permanent SQLite History Pull
    history_records = db_get_all_trades(device_id, limit=500)

    return {
        "status": "success",
        "active": state["active_trades"],
        "history": history_records,
        "paper_balance": state["paper_balance"],
        "market_mode": state["market_mode"],
        "quote_currency": state["quote_currency"],
        "currency_symbol": get_curr_symbol(state),
        "today_pnl": state["today_pnl"]
    }

# ----------------- BACKGROUND SCANNER LOOP -----------------
async def market_scanner_loop():
    while True:
        try:
            now_ts = time.time()
            for dev_id, state in list(user_sessions.items()):
                check_midnight_settlement(state)

                if state.get("sleep_until"):
                    if now_ts < state["sleep_until"]:
                        continue
                    else:
                        state["sleep_until"] = None
                        state["sleep_reason"] = ""
                        state["today_pnl"] = 0.0
                        state["is_running"] = True
                        add_log(state, "⏰ 12-Hour Cooldown Completed! Bot Engine Resumed.")

                base_fund = state.get("session_start_fund", 744.0)
                if base_fund > 0:
                    profit_limit_15 = base_fund * 0.15
                    loss_limit_10 = -(base_fund * 0.10)
                    curr_sym = get_curr_symbol(state)

                    if state["today_pnl"] >= profit_limit_15:
                        state["is_running"] = False
                        state["sleep_until"] = now_ts + (12 * 3600)
                        state["sleep_reason"] = f"+15% Target Hit ({curr_sym}{state['today_pnl']})"
                        add_log(state, f"🎉 15% PROFIT TARGET HIT (+{curr_sym}{state['today_pnl']})! Bot Sleeping for 12 Hours.")
                        continue

                    elif state["today_pnl"] <= loss_limit_10:
                        state["is_running"] = False
                        state["sleep_until"] = now_ts + (12 * 3600)
                        state["sleep_reason"] = f"-10% Loss Shield ({curr_sym}{state['today_pnl']})"
                        add_log(state, f"🛡️ 10% LOSS SHIELD TRIGGERED ({curr_sym}{state['today_pnl']})! Bot Sleeping for 12 Hours.")
                        continue

                if state["is_running"]:
                    all_coins = fetch_active_exchange_markets(state)
                    if not all_coins:
                        continue

                    live_prices = {c['symbol']: c['price'] for c in all_coins}

                    # SL & TARGET TRACKING
                    trades_to_close = []
                    for trade in list(state["active_trades"]):
                        sym = trade["symbol"]
                        if sym in live_prices and trade.get("entry_price", 0) > 0:
                            curr_p = live_prices[sym]
                            target_p = trade.get("target_price", trade["entry_price"] * 1.015)
                            sl_p = trade.get("sl_price", trade["entry_price"] * 0.98)

                            if trade["type"] == "LONG":
                                if curr_p > trade.get("highest_price", trade["entry_price"]):
                                    trade["highest_price"] = curr_p
                                    trade["sl_price"] = max(trade["sl_price"], curr_p * 0.98)
                                if curr_p <= sl_p or curr_p >= target_p:
                                    trade["close_reason"] = "TARGET HIT" if curr_p >= target_p else "SL HIT"
                                    trades_to_close.append(trade)
                            elif trade["type"] == "SHORT":
                                if curr_p < trade.get("lowest_price", trade["entry_price"]):
                                    trade["lowest_price"] = curr_p
                                    trade["sl_price"] = min(trade["sl_price"], curr_p * 1.02)
                                if curr_p >= sl_p or curr_p <= target_p:
                                    trade["close_reason"] = "TARGET HIT" if curr_p <= target_p else "SL HIT"
                                    trades_to_close.append(trade)

                    for trade in trades_to_close:
                        exit_p = live_prices.get(trade["symbol"], trade["entry_price"])
                        side_to_exit = "sell" if trade["type"] == "LONG" else "buy"

                        if state.get("active_broker") == "coindcx":
                            execute_coindcx_order(state, trade.get("symbol"), side=side_to_exit, exact_qty=trade.get("quantity", 0))

                        pnl_percent = ((exit_p - trade["entry_price"]) / trade["entry_price"] * 100) if trade["type"] == "LONG" else ((trade["entry_price"] - exit_p) / trade["entry_price"] * 100)
                        trade_amt = trade.get("amount", 500.0)
                        pnl_val = (trade_amt * pnl_percent) / 100

                        trade["pnl_percent"] = round(pnl_percent, 2)
                        trade["pnl_val"] = round(pnl_val, 2)
                        trade["exit_price"] = exit_p
                        trade["close_time"] = get_global_time()
                        trade["status"] = trade.get("close_reason", "COMPLETED")

                        state["today_pnl"] += trade["pnl_val"]
                        state["today_pnl"] = round(state["today_pnl"], 2)

                        if trade in state["active_trades"]:
                            state["active_trades"].remove(trade)

                        # Permanent DB Save
                        db_save_trade(trade, dev_id, state.get("active_broker", "coindcx"))
                        add_log(state, f"🎯 DEAL CLOSED: {trade['type']} {trade['symbol']} | P&L: {trade['pnl_percent']}% ({trade['status']})")

                    # NEW TRADE TRIGGER (STRICT SELECTION OR SCANNER)
                    if len(state["active_trades"]) < 1:
                        valid = [c for c in all_coins if c['price'] > 0]
                        if valid:
                            selected = state.get("selected_coin", "AUTO")
                            if selected != "AUTO":
                                target_coin = next((c for c in valid if selected in c['symbol']), None)
                            else:
                                gainers = sorted(valid, key=lambda x: x.get('change', 0.0), reverse=True)
                                target_coin = gainers[0] if gainers else None

                            if target_coin:
                                pos_type = "LONG"
                                coin_sym = target_coin['symbol']
                                current_p = target_coin['price']
                                order_amount = state["trade_amount"]
                                quote = state.get("quote_currency", "INR")
                                curr_sym = get_curr_symbol(state)

                                if state["active_broker"] == "coindcx":
                                    side = "buy" if pos_type == "LONG" else "sell"
                                    success, buy_price, buy_qty, res = execute_coindcx_order(state, coin_sym, side=side, target_amount=order_amount)
                                    if success:
                                        new_trade = {
                                            "id": int(time.time()),
                                            "symbol": coin_sym,
                                            "currency": quote,
                                            "type": pos_type,
                                            "entry_price": buy_price,
                                            "quantity": buy_qty,
                                            "amount": round(buy_qty * buy_price, 2),
                                            "highest_price": buy_price,
                                            "lowest_price": buy_price,
                                            "sl_price": buy_price * 0.98,
                                            "target_price": buy_price * 1.015,
                                            "time": get_global_time()
                                        }
                                        state["active_trades"].insert(0, new_trade)
                                        add_log(state, f"⚡ REAL {pos_type}: {buy_qty} {coin_sym} at {curr_sym}{buy_price}")

                                elif state["active_broker"] == "paper":
                                    calc_qty = int(order_amount / current_p) if current_p < 20 else round(order_amount / current_p, 4)
                                    if calc_qty <= 0: calc_qty = 1
                                    new_trade = {
                                        "id": int(time.time()),
                                        "symbol": coin_sym,
                                        "currency": quote,
                                        "type": pos_type,
                                        "entry_price": current_p,
                                        "quantity": calc_qty,
                                        "amount": order_amount,
                                        "highest_price": current_p,
                                        "lowest_price": current_p,
                                        "sl_price": current_p * 0.98,
                                        "target_price": current_p * 1.015,
                                        "time": get_global_time()
                                    }
                                    state["active_trades"].insert(0, new_trade)
                                    add_log(state, f"⚡ [PAPER] {pos_type}: {coin_sym} at {curr_sym}{current_p}")
        except Exception:
            pass

        await asyncio.sleep(3.5)

@app.get("/")
def root():
    return {
        "status": "HiTech Dual AI Engine Live!",
        "database": "SQLite Trades Active",
        "total_vip_keys": len(keys_db)
    }

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
