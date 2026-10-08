import os
import time
import math
import hmac
import hashlib
import json
import sqlite3
import requests
import ccxt
import uvicorn
import asyncio
import threading
import uuid
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from urllib.parse import urlencode, unquote_plus

try:
    from cryptography.hazmat.primitives.asymmetric import ed25519
except Exception:
    ed25519 = None

# ----------------- DATABASE SETUP -----------------
DB_FILE = "trades_history.db"

def get_db():
    conn = sqlite3.connect(DB_FILE, timeout=45.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def init_db():
    conn = get_db()
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
            exchange TEXT,
            close_time TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bot_state (
            device_id TEXT PRIMARY KEY,
            is_running INTEGER,
            active_broker TEXT,
            market_mode TEXT,
            quote_currency TEXT,
            trade_amount REAL,
            max_trades INTEGER,
            target_percent REAL,
            sl_percent REAL,
            selected_coin TEXT,
            deal_condition TEXT,
            paper_balance REAL DEFAULT 500000.0,
            today_pnl REAL DEFAULT 0.0,
            last_settlement_date TEXT,
            market_data_exchange TEXT DEFAULT 'coindcx'
        )
    """)
    for ddl in [
        "ALTER TABLE bot_state ADD COLUMN paper_balance REAL DEFAULT 500000.0",
        "ALTER TABLE bot_state ADD COLUMN today_pnl REAL DEFAULT 0.0",
        "ALTER TABLE bot_state ADD COLUMN last_settlement_date TEXT",
        "ALTER TABLE bot_state ADD COLUMN market_data_exchange TEXT DEFAULT 'coindcx'",
        "ALTER TABLE trades ADD COLUMN exchange TEXT",
        "ALTER TABLE active_trades ADD COLUMN exchange TEXT"
    ]:
        try:
            cursor.execute(ddl)
        except sqlite3.OperationalError:
            pass

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS active_trades (
            trade_id TEXT PRIMARY KEY,
            device_id TEXT NOT NULL,
            symbol TEXT,
            currency TEXT,
            side TEXT,
            entry_price REAL,
            quantity REAL,
            amount REAL,
            sl_price REAL,
            target_price REAL,
            highest_price REAL,
            lowest_price REAL,
            current_price REAL,
            current_pnl_percent REAL DEFAULT 0,
            current_pnl_val REAL DEFAULT 0,
            unrealized_pnl REAL DEFAULT 0,
            unrealized_pnl_percent REAL DEFAULT 0,
            broker TEXT,
            opened_at TEXT,
            updated_at TEXT,
            status TEXT DEFAULT 'OPEN',
            exchange TEXT
        )
    """)
    try:
        cursor.execute("ALTER TABLE active_trades ADD COLUMN exchange TEXT")
    except sqlite3.OperationalError:
        pass
    conn.commit()
    conn.close()

init_db()

def get_global_time():
    return datetime.now(timezone.utc).isoformat() + "Z"

def db_save_trade(trade: dict, device_id: str, broker: str):
    try:
        conn = get_db()
        cursor = conn.cursor()
        t_id = str(trade.get("id", int(time.time() * 1000)))
        sym = str(trade.get("symbol", "BTCINR"))
        curr = str(trade.get("currency", "INR"))
        side = str(trade.get("type", trade.get("side", "BUY")))
        entry = float(trade.get("entry_price", 0.0) or 0.0)
        exit_p = float(trade.get("exit_price", 0.0) or 0.0)
        qty = float(trade.get("quantity", 0.0) or 0.0)
        amt = float(trade.get("amount", 0.0) or 0.0)
        sl = float(trade.get("sl_price", 0.0) or 0.0)
        tgt = float(trade.get("target_price", 0.0) or 0.0)
        pnl_pct = float(trade.get("pnl_percent", 0.0) or 0.0)
        pnl_val = float(trade.get("pnl_val", 0.0) or 0.0)
        status = str(trade.get("status", "CLOSED"))
        exchange = str(trade.get("exchange", trade.get("market_data_exchange", broker)) or broker)
        close_t = str(trade.get("close_time", get_global_time()))

        cursor.execute("""
            INSERT OR REPLACE INTO trades (
                trade_id, device_id, symbol, currency, side, entry_price, 
                exit_price, quantity, amount, sl_price, target_price, 
                pnl_percent, pnl_val, status, broker, exchange, close_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (t_id, device_id, sym, curr, side, entry, exit_p, qty, amt, sl, tgt, pnl_pct, pnl_val, status, broker, exchange, close_t))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"DB Save Critical Error: {e}")

def db_save_active_trade(trade: dict, device_id: str, broker: str):
    conn = get_db()
    try:
        t_id = str(trade.get("id"))
        if not t_id or t_id == "None":
            raise ValueError("Active trade has no id")

        now = get_global_time()
        conn.execute("""
            INSERT OR REPLACE INTO active_trades (
                trade_id, device_id, symbol, currency, side, entry_price, quantity,
                amount, sl_price, target_price, highest_price, lowest_price,
                current_price, current_pnl_percent, current_pnl_val,
                unrealized_pnl, unrealized_pnl_percent, broker, exchange, opened_at,
                updated_at, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN')
        """, (
            t_id, device_id,
            str(trade.get("symbol", "")),
            str(trade.get("currency", "INR")),
            str(trade.get("type", trade.get("side", "LONG"))),
            float(trade.get("entry_price", 0) or 0),
            float(trade.get("quantity", 0) or 0),
            float(trade.get("amount", 0) or 0),
            float(trade.get("sl_price", 0) or 0),
            float(trade.get("target_price", 0) or 0),
            float(trade.get("highest_price", trade.get("entry_price", 0)) or 0),
            float(trade.get("lowest_price", trade.get("entry_price", 0)) or 0),
            float(trade.get("current_price", trade.get("entry_price", 0)) or 0),
            float(trade.get("current_pnl_percent", 0) or 0),
            float(trade.get("current_pnl_val", 0) or 0),
            float(trade.get("unrealized_pnl", 0) or 0),
            float(trade.get("unrealized_pnl_percent", 0) or 0),
            broker,
            str(trade.get("exchange", trade.get("market_data_exchange", broker)) or broker),
            str(trade.get("time", now)), now
        ))
        conn.commit()
    finally:
        conn.close()

def db_update_active_trade(trade: dict, device_id: str, broker: str):
    db_save_active_trade(trade, device_id, broker)

def db_delete_active_trade(trade_id):
    conn = get_db()
    try:
        conn.execute("DELETE FROM active_trades WHERE trade_id = ?", (str(trade_id),))
        conn.commit()
    finally:
        conn.close()

def db_load_active_trades(device_id: str):
    conn = get_db()
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("""
            SELECT * FROM active_trades
            WHERE device_id = ? AND status = 'OPEN'
            ORDER BY rowid DESC
        """, (device_id,)).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["id"] = d.pop("trade_id")
            d["time"] = d.get("opened_at") or get_global_time()
            result.append(d)
        return result
    finally:
        conn.close()

def db_get_all_trades(device_id: str, limit: int = 1000):
    try:
        conn = get_db()
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

def save_state_to_db(device_id: str, state: dict):
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO bot_state (
            device_id, is_running, active_broker, market_mode, quote_currency,
            trade_amount, max_trades, target_percent, sl_percent, selected_coin,
            deal_condition, paper_balance, today_pnl, last_settlement_date, market_data_exchange
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            device_id,
            1 if state.get("is_running") else 0,
            state.get("active_broker", "paper"),
            state.get("market_mode", "spot"),
            state.get("quote_currency", "INR"),
            state.get("trade_amount", 500.0),
            state.get("max_trades", 5),
            state.get("target_percent", 2.5),
            state.get("sl_percent", 1.5),
            state.get("selected_coin", "AUTO"),
            state.get("deal_condition", "ASAP"),
            float(state.get("paper_balance", 500000.0) or 0.0),
            float(state.get("today_pnl", 0.0) or 0.0),
            state.get("last_settlement_date") or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            state.get("market_data_exchange", "coindcx")
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"DB State Save Error: {e}")

# ----------------- APP LIFECYCLE & STATE -----------------
SCANNER_TASK = None
SCANNER_LOCK = None
user_sessions = {}
_analysis_cache = {}
_market_ticker_cache = {}
MARKET_TICKER_CACHE_TTL = 8.0

def _load_persisted_runtime_sessions():
    try:
        conn = get_db()
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM bot_state WHERE is_running = 1 OR device_id IN (SELECT device_id FROM active_trades WHERE status='OPEN')").fetchall()
        conn.close()
        for row in rows:
            dev = row["device_id"]
            if dev not in user_sessions:
                user_sessions[dev] = {
                    "is_running": bool(row["is_running"]),
                    "active_broker": row["active_broker"] or "paper",
                    "market_data_exchange": row["market_data_exchange"] or "coindcx",
                    "market_mode": row["market_mode"] or "spot",
                    "api_key": "", "secret_key": "",
                    "quote_currency": row["quote_currency"] or "INR",
                    "trade_amount": float(row["trade_amount"] or 500.0),
                    "max_trades": int(row["max_trades"] or 5),
                    "target_percent": float(row["target_percent"] or 2.5),
                    "sl_percent": float(row["sl_percent"] or 1.5),
                    "selected_coin": row["selected_coin"] or "AUTO",
                    "deal_condition": row["deal_condition"] or "ASAP",
                    "trade_type": "intraday", "strategy": "volume",
                    "logs": ["🔄 Runtime recovered from database."],
                    "active_trades": db_load_active_trades(dev),
                    "paper_balance": float(row["paper_balance"] if row["paper_balance"] is not None else 500000.0),
                    "today_pnl": float(row["today_pnl"] if row["today_pnl"] is not None else 0.0),
                    "session_start_fund": 0.0, "sleep_until": None, "sleep_reason": "",
                    "last_settlement_date": row["last_settlement_date"] or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    "_closing_ids": set(), "scanner_reports": [], "scanner_cycle": 0, "scanner_updated_at": "", "scanner_exchange": "coindcx"
                }
    except Exception as exc:
        print(f"Runtime recovery error: {exc}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    global SCANNER_TASK, SCANNER_LOCK
    SCANNER_LOCK = asyncio.Lock()
    _load_persisted_runtime_sessions()
    SCANNER_TASK = asyncio.create_task(market_scanner_loop())
    yield
    if SCANNER_TASK:
        SCANNER_TASK.cancel()
        try:
            await SCANNER_TASK
        except asyncio.CancelledError:
            pass

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_market_data_exchange(state):
    broker = str(state.get("active_broker", "paper") or "paper").lower()
    if broker == "paper":
        return str(state.get("market_data_exchange", "coindcx") or "coindcx").lower()
    return broker

def get_user_session(device_id: str):
    if not device_id:
        device_id = "DEFAULT_DEVICE"
    if device_id not in user_sessions:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT is_running, active_broker, market_mode, quote_currency, trade_amount,
                   max_trades, target_percent, sl_percent, selected_coin, deal_condition,
                   paper_balance, today_pnl, last_settlement_date, market_data_exchange
            FROM bot_state WHERE device_id = ?
        """, (device_id,))
        row = cursor.fetchone()
        conn.close()

        if row:
            user_sessions[device_id] = {
                "is_running": bool(row[0]),
                "active_broker": row[1] or "paper",
                "market_data_exchange": row[13] or "coindcx",
                "market_mode": row[2] or "spot",
                "api_key": "", "secret_key": "",
                "quote_currency": row[3] or "INR",
                "trade_amount": float(row[4] or 500.0),
                "max_trades": int(row[5] or 5),
                "target_percent": float(row[6] or 2.5),
                "sl_percent": float(row[7] or 1.5),
                "selected_coin": row[8] or "AUTO",
                "deal_condition": row[9] or "ASAP",
                "trade_type": "intraday", "strategy": "volume",
                "logs": ["🤖 Bot session recovered."],
                "active_trades": db_load_active_trades(device_id),
                "paper_balance": float(row[10] if row[10] is not None else 500000.0),
                "today_pnl": float(row[11] if row[11] is not None else 0.0),
                "session_start_fund": 0.0, "sleep_until": None, "sleep_reason": "",
                "last_settlement_date": row[12] or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "_closing_ids": set(), "scanner_reports": [], "scanner_cycle": 0, "scanner_updated_at": "", "scanner_exchange": "coindcx"
            }
        else:
            user_sessions[device_id] = {
                "is_running": False, "active_broker": "paper", "market_data_exchange": "coindcx",
                "market_mode": "spot", "api_key": "", "secret_key": "", "quote_currency": "INR",
                "trade_amount": 500.0, "max_trades": 5, "trade_type": "intraday", "strategy": "volume",
                "deal_condition": "ASAP", "selected_coin": "AUTO", "target_percent": 2.5, "sl_percent": 1.5,
                "logs": ["🤖 Dual Engine Initialized."], "active_trades": [], "paper_balance": 500000.0,
                "today_pnl": 0.0, "session_start_fund": 0.0, "sleep_until": None, "sleep_reason": "",
                "last_settlement_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"), "_closing_ids": set(),
                "scanner_reports": [], "scanner_cycle": 0, "scanner_updated_at": "", "scanner_exchange": "coindcx"
            }
    state = user_sessions[device_id]
    state.setdefault("max_trades", 5)
    state.setdefault("target_percent", 2.5)
    state.setdefault("sl_percent", 1.5)
    state.setdefault("_closing_ids", set())
    state.setdefault("scanner_reports", [])
    state.setdefault("scanner_cycle", 0)
    state.setdefault("scanner_updated_at", "")
    state.setdefault("scanner_exchange", get_market_data_exchange(state))
    return state

# ----------------- COMPLETE 100+ MASTER VIP KEYS DATABASE -----------------
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
        except Exception:
            keys_db = {}
    for k in MASTER_VIP_KEYS:
        clean_k = k.strip()
        if clean_k not in keys_db:
            keys_db[clean_k] = {
                "used": False, "device_id": None, "activated_at": None, "expires_at": None, "referral_count": 0
            }
    save_keys_database()

def save_keys_database():
    try:
        with open(KEYS_DB_FILE, "w") as f:
            json.dump(keys_db, f, indent=2)
    except Exception:
        pass

load_keys_database()

# ----------------- VIP KEY VERIFICATION ENDPOINT -----------------
@app.post("/api/verify-vip-key")
async def verify_vip_key(request: Request):
    data = await request.json()
    raw_key = data.get("key", "").strip()
    device_id = data.get("device_id", "").strip()
    referral_code = data.get("referral_code", "").strip()

    if not raw_key:
        return {"status": "error", "message": "Key cannot be empty."}

    matched_key = next((k for k in keys_db if k.lower() == raw_key.lower()), None)

    if not matched_key:
        return {"status": "error", "message": "Invalid Activation Key. Please verify with admin."}

    record = keys_db[matched_key]
    now_dt = datetime.now(timezone.utc)

    if record["used"]:
        if record["device_id"] == device_id:
            try:
                exp_dt = datetime.fromisoformat(record["expires_at"].replace("Z", "+00:00"))
                if exp_dt > now_dt:
                    days_left = (exp_dt - now_dt).days + 1
                    return {
                        "status": "success",
                        "message": f"Key verified! {days_left} remaining day(s).",
                        "expires_at": record["expires_at"]
                    }
                else:
                    return {"status": "error", "message": "This VIP Key has expired. Please renew."}
            except Exception:
                pass
        return {"status": "error", "message": "This key has already been activated on another device!"}

    activation_time = now_dt
    expiry_time = activation_time + timedelta(days=30)

    record["used"] = True
    record["device_id"] = device_id if device_id else f"DEV_{int(time.time())}"
    record["activated_at"] = activation_time.isoformat()
    record["expires_at"] = expiry_time.isoformat()

    if referral_code and referral_code in keys_db and referral_code.lower() != matched_key.lower():
        ref_record = keys_db[referral_code]
        if ref_record["used"] and ref_record["expires_at"]:
            try:
                ref_exp = datetime.fromisoformat(ref_record["expires_at"].replace("Z", "+00:00"))
                base_time = ref_exp if ref_exp > now_dt else now_dt
                new_ref_exp = base_time + timedelta(days=10)
                ref_record["expires_at"] = new_ref_exp.isoformat()
                ref_record["referral_count"] = ref_record.get("referral_count", 0) + 1
            except Exception:
                pass

    save_keys_database()
    return {
        "status": "success",
        "message": "VIP Key verified successfully! 30-Day access granted.",
        "expires_at": record["expires_at"]
    }

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
        save_state_to_db(next((k for k, v in user_sessions.items() if v is state), "DEFAULT_DEVICE"), state)
        add_log(state, f"🏦 Midnight Settlement: {curr_sym}{settled_amount} moved to Wallet.")

# ----------------- TECHNICAL INDICATORS -----------------
def _ema(values, period):
    vals = [float(v) for v in values if v is not None]
    if len(vals) < period: return None
    k = 2.0 / (period + 1.0)
    e = sum(vals[:period]) / period
    for v in vals[period:]:
        e = (v * k) + (e * (1.0 - k))
    return e

def _rsi(closes, period=14):
    if len(closes) < period + 1: return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period
    if avg_loss == 0: return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

def _macd(closes):
    if len(closes) < 35: return None, None, None
    macd_series = []
    for i in range(26, len(closes)):
        fast = _ema(closes[:i+1], 12)
        slow = _ema(closes[:i+1], 26)
        if fast is not None and slow is not None:
            macd_series.append(fast - slow)
    if len(macd_series) < 9: return None, None, None
    signal = _ema(macd_series, 9)
    if signal is None: return None, None, None
    return macd_series[-1], signal, macd_series[-2] if len(macd_series) > 1 else macd_series[-1]

# ----------------- BROKER REGISTRY & MARKET DATA -----------------
BROKER_CCXT_ALIASES = {
    "crypto_com": "cryptocom", "crypto.com": "cryptocom", "gate.io": "gateio",
    "gate": "gateio", "huobi": "htx", "coinbase": "coinbase", "delta_exchange": "delta"
}

def _ccxt_id_for_broker(broker):
    broker = str(broker or "").lower().strip()
    return BROKER_CCXT_ALIASES.get(broker, broker)

def fetch_active_exchange_markets(state):
    broker = get_market_data_exchange(state)
    quote = state.get("quote_currency", "INR").upper()
    cache_key = f"{broker}|{quote}"
    now = time.time()
    cached = _market_ticker_cache.get(cache_key)
    if cached and (now - cached.get("ts", 0)) < MARKET_TICKER_CACHE_TTL:
        return list(cached.get("data", []))

    market_list = []
    try:
        if broker in ["binance", "paper"]:
            res = requests.get("https://api.binance.com/api/v3/ticker/24hr", timeout=6)
            res.raise_for_status()
            data = res.json()
            pairs = [x for x in data if x['symbol'].endswith('USDT') and not any(k in x['symbol'] for k in ['UP', 'DOWN'])][:100]
            for p in pairs:
                base = p['symbol'].replace('USDT', '')
                price = float(p.get("lastPrice", 0) or 0)
                if quote == "INR": price *= 89.5
                market_list.append({
                    "symbol": base + quote, "base_coin": base, "raw_symbol": p["symbol"],
                    "price": price, "volume": float(p.get("quoteVolume", 0) or 0),
                    "change": float(p.get("priceChangePercent", 0) or 0), "exchange": broker
                })
        elif broker == "coindcx":
            res = requests.get("https://api.coindcx.com/exchange/ticker", timeout=6)
            res.raise_for_status()
            for item in res.json():
                m = str(item.get("market", "") or "")
                price = float(item.get("last_price", 0.0) or 0.0)
                if price <= 0: continue
                is_inr = m.endswith("_INR") or m.endswith("INR")
                is_usdt = m.endswith("_USDT") or m.endswith("USDT")
                if quote == "INR" and not is_inr: continue
                if quote == "USDT" and not is_usdt: continue
                clean = m.replace("B-", "").replace("I-", "").replace("_", "").replace("INR", "").replace("USDT", "").upper()
                market_list.append({
                    "symbol": clean + quote, "base_coin": clean, "raw_symbol": m,
                    "price": price, "volume": float(item.get("volume", 0.0) or 0.0),
                    "change": float(item.get("change_24_hour", 0.0) or 0.0), "exchange": "coindcx"
                })
        elif broker == "wazirx":
            res = requests.get("https://api.wazirx.com/sapi/v1/tickers/24hr", timeout=6)
            res.raise_for_status()
            for item in res.json():
                q = str(item.get("quoteAsset", "") or "").upper()
                base = str(item.get("baseAsset", "") or "").upper()
                if q != quote or not base: continue
                price = float(item.get("lastPrice", 0) or 0)
                if price <= 0: continue
                open_p = float(item.get("openPrice", 0) or 0)
                chg = ((price - open_p) / open_p * 100.0) if open_p > 0 else 0.0
                market_list.append({
                    "symbol": base + quote, "base_coin": base, "raw_symbol": base + quote,
                    "price": price, "volume": float(item.get("volume", 0) or 0),
                    "change": chg, "exchange": "wazirx"
                })
    except Exception as exc:
        add_log(state, f"⚠️ Market fetch error: {exc}")
        if cached and cached.get("data"):
            return list(cached["data"])
        return []

    market_list.sort(key=lambda x: float(x.get("volume", 0.0) or 0.0), reverse=True)
    market_list = market_list[:100]
    _market_ticker_cache[cache_key] = {"ts": now, "data": list(market_list)}
    return market_list

def _fetch_analysis_ohlcv(base_coin, state, raw_symbol=None, timeframe="15m", limit=200):
    coin = str(base_coin or "").upper().strip()
    broker = get_market_data_exchange(state)
    quote = state.get("quote_currency", "INR").upper()
    try:
        if broker in ["binance", "paper"]:
            pair = f"{coin}USDT"
            r = requests.get("https://api.binance.com/api/v3/klines", params={"symbol": pair, "interval": timeframe, "limit": min(limit, 250)}, timeout=6)
            r.raise_for_status()
            return [{"time": int(x[0]), "open": float(x[1]), "high": float(x[2]), "low": float(x[3]), "close": float(x[4]), "volume": float(x[5])} for x in r.json()]
        elif broker == "coindcx":
            pair = raw_symbol or f"B-{coin}_{quote}"
            r = requests.get("https://api.coindcx.com/market_data/candles", params={"pair": pair, "interval": timeframe, "limit": min(limit, 250)}, timeout=6)
            r.raise_for_status()
            out = [{"time": int(x.get("time", 0)), "open": float(x["open"]), "high": float(x["high"]), "low": float(x["low"]), "close": float(x["close"]), "volume": float(x.get("volume", 0))} for x in r.json()]
            return list(reversed(out))
    except Exception:
        pass
    return []

def _analyze_coin(coin, state):
    base = str(coin.get("base_coin", "")).upper()
    cache_key = f"{base}|{state.get('quote_currency')}"
    cached = _analysis_cache.get(cache_key)
    if cached and (time.time() - cached.get("ts", 0)) < 30:
        return cached.get("analysis")

    c15 = _fetch_analysis_ohlcv(base, state, coin.get("raw_symbol"), "15m", 120)
    if len(c15) < 35: return None

    closes = [x["close"] for x in c15]
    price = closes[-1]
    rsi = _rsi(closes, 14) or 50.0
    macd, macd_signal, macd_prev = _macd(closes)
    ema50 = _ema(closes, 50)

    bullish_pts = 0
    bearish_pts = 0
    reasons = []

    if rsi < 35: bullish_pts += 3; reasons.append(f"RSI oversold ({rsi:.1f})")
    elif rsi > 65: bearish_pts += 3; reasons.append(f"RSI overbought ({rsi:.1f})")

    if macd and macd_signal and macd > macd_signal:
        bullish_pts += 3; reasons.append("MACD bullish")
    else:
        bearish_pts += 3

    if ema50 and price > ema50: bullish_pts += 2; reasons.append("Price > EMA50")
    else: bearish_pts += 2

    score = min(100, int((max(bullish_pts, bearish_pts) / 8.0) * 100))
    direction = "LONG" if bullish_pts >= bearish_pts else "SHORT"

    analysis = {
        "base_coin": base, "price": price, "rsi": rsi, "score": score,
        "direction": direction, "reasons": reasons, "volume_ratio": 1.2
    }
    _analysis_cache[cache_key] = {"ts": time.time(), "analysis": analysis}
    return analysis

def _signal_passes(analysis, condition, market_mode):
    if not analysis: return False
    direction = analysis["direction"]
    if market_mode == "spot" and direction != "LONG": return False
    if condition == "ASAP": return True
    if condition == "RSI_DIP": return analysis["rsi"] < 35
    if condition == "MACD_CROSS": return "MACD bullish" in analysis.get("reasons", [])
    return analysis["score"] >= 60

# ----------------- ORDER EXECUTION & SYNC -----------------
def _close_trade_at_market(state, device_id, trade, reason="MANUAL EXIT", current_price=0.0):
    exit_p = float(current_price or trade.get("current_price", 0.0) or trade.get("entry_price", 0.0))
    entry_p = float(trade.get("entry_price", 0.0) or 0.0)
    qty = float(trade.get("quantity", 0.0) or 0.0)
    amount = float(trade.get("amount", 0.0) or 0.0)
    is_long = trade.get("type") in ["LONG", "BUY"]

    pnl_pct = ((exit_p - entry_p) / entry_p * 100.0) if (is_long and entry_p > 0) else (((entry_p - exit_p) / entry_p * 100.0) if entry_p > 0 else 0.0)
    pnl_val = (amount * pnl_pct / 100.0)

    trade["exit_price"] = exit_p
    trade["pnl_percent"] = round(pnl_pct, 2)
    trade["pnl_val"] = round(pnl_val, 2)
    trade["status"] = reason
    trade["close_time"] = get_global_time()

    state["paper_balance"] = round(state.get("paper_balance", 500000.0) + amount + pnl_val, 2)
    state["today_pnl"] = round(state.get("today_pnl", 0.0) + pnl_val, 2)

    if trade in state.get("active_trades", []):
        state["active_trades"].remove(trade)

    db_delete_active_trade(trade.get("id"))
    db_save_trade(trade, device_id, state.get("active_broker", "paper"))
    save_state_to_db(device_id, state)
    return True, exit_p, qty, "Closed"

def fetch_real_cash_balance(state):
    broker = state.get("active_broker", "coindcx").lower()
    if broker == "paper":
        return round(float(state.get("paper_balance", 500000.0)), 2)
    return 0.0

# ----------------- MULTI-SLOT SCANNER ENGINE -----------------
async def market_scanner_loop():
    while True:
        try:
            if SCANNER_LOCK is not None and SCANNER_LOCK.locked():
                await asyncio.sleep(0.5)
                continue

            lock_ctx = SCANNER_LOCK if SCANNER_LOCK is not None else asyncio.Lock()
            async with lock_ctx:
                for dev_id, state in list(user_sessions.items()):
                    try:
                        # 1. Trailing SL & Target Monitoring
                        active = list(state.get("active_trades", []))
                        if active:
                            markets = await asyncio.to_thread(fetch_active_exchange_markets, state)
                            price_map = {m["base_coin"]: float(m["price"]) for m in markets if m.get("base_coin")}

                            for trade in active:
                                sym = str(trade.get("symbol", "")).replace("INR", "").replace("USDT", "").upper()
                                curr_p = price_map.get(sym, float(trade.get("current_price", 0.0)))
                                entry_p = float(trade.get("entry_price", 0.0))
                                target_p = float(trade.get("target_price", 0.0))
                                sl_p = float(trade.get("sl_price", 0.0))
                                is_long = trade.get("type") in ["LONG", "BUY"]

                                if curr_p > 0 and entry_p > 0:
                                    trade["current_price"] = curr_p
                                    pnl_pct = ((curr_p - entry_p) / entry_p * 100.0) if is_long else ((entry_p - curr_p) / entry_p * 100.0)
                                    trade["current_pnl_percent"] = round(pnl_pct, 2)
                                    trade["current_pnl_val"] = round(float(trade.get("amount", 0.0)) * pnl_pct / 100.0, 2)

                                    # Trailing Stop Loss Logic
                                    sl_pct_val = float(state.get("sl_percent", 1.5)) / 100.0
                                    if is_long and curr_p > float(trade.get("highest_price", entry_p)):
                                        trade["highest_price"] = curr_p
                                        new_sl = curr_p * (1.0 - sl_pct_val)
                                        if new_sl > sl_p:
                                            trade["sl_price"] = new_sl
                                            sl_p = new_sl
                                    elif (not is_long) and curr_p < float(trade.get("lowest_price", entry_p)):
                                        trade["lowest_price"] = curr_p
                                        new_sl = curr_p * (1.0 + sl_pct_val)
                                        if new_sl < sl_p:
                                            trade["sl_price"] = new_sl
                                            sl_p = new_sl

                                    db_update_active_trade(trade, dev_id, state.get("active_broker", "paper"))

                                    target_hit = curr_p >= target_p if is_long else curr_p <= target_p
                                    sl_hit = curr_p <= sl_p if is_long else curr_p >= sl_p

                                    if target_hit or sl_hit:
                                        reason = "TARGET HIT" if target_hit else "SL/TSL HIT"
                                        _close_trade_at_market(state, dev_id, trade, reason, curr_p)
                                        add_log(state, f"🎯 {reason}: {sym} at {curr_p} | P&L: {pnl_pct:.2f}%")

                        # 2. Scanning for New Slots (Multi-Slot Support: 1, 3, 5, 10)
                        if not state.get("is_running"):
                            continue

                        allowed_slots = max(1, int(state.get("max_trades", 5)))
                        current_active_count = len(state.get("active_trades", []))
                        free_slots = allowed_slots - current_active_count

                        if free_slots <= 0:
                            continue

                        all_coins = await asyncio.to_thread(fetch_active_exchange_markets, state)
                        if not all_coins:
                            continue

                        active_bases = {str(t.get("symbol", "")).replace("INR", "").replace("USDT", "").upper() for t in state.get("active_trades", [])}
                        valid = [c for c in all_coins if float(c.get("price", 0) or 0) > 0 and str(c.get("base_coin", "")).upper() not in active_bases]

                        selected = str(state.get("selected_coin", "AUTO")).upper()
                        deal_cond = str(state.get("deal_condition", "ASAP")).upper()
                        market_mode = state.get("market_mode", "spot").lower()

                        if selected != "AUTO":
                            scan_pool = [c for c in valid if str(c.get("base_coin", "")).upper() == selected.replace("INR","").replace("USDT","")]
                        else:
                            scan_pool = valid[:30]

                        approved_candidates = []
                        for coin in scan_pool:
                            if len(approved_candidates) >= free_slots:
                                break
                            analysis = await asyncio.to_thread(_analyze_coin, coin, state)
                            if analysis and _signal_passes(analysis, deal_cond, market_mode):
                                approved_candidates.append((coin, analysis))

                        order_amount = float(state.get("trade_amount", 500.0))
                        target_pct = float(state.get("target_percent", 2.5)) / 100.0
                        sl_pct = float(state.get("sl_percent", 1.5)) / 100.0

                        for target_coin, analysis in approved_candidates:
                            if len(state["active_trades"]) >= allowed_slots:
                                break
                            if float(state.get("paper_balance", 500000.0)) < order_amount:
                                add_log(state, "⚠️ Insufficient Balance for new slot")
                                break

                            price = float(target_coin["price"])
                            base = target_coin["base_coin"]
                            trade_type = "LONG" if (market_mode == "spot" or analysis["direction"] != "SHORT") else "SHORT"

                            state["paper_balance"] = round(state["paper_balance"] - order_amount, 2)
                            qty = round(order_amount / price, 6) if price > 0 else 1.0

                            target_price = price * (1.0 + target_pct) if trade_type == "LONG" else price * (1.0 - target_pct)
                            sl_price = price * (1.0 - sl_pct) if trade_type == "LONG" else price * (1.0 + sl_pct)

                            new_trade = {
                                "id": f"{int(time.time()*1000)}_{uuid.uuid4().hex[:6]}",
                                "symbol": base + state.get("quote_currency", "INR"),
                                "currency": state.get("quote_currency", "INR"),
                                "type": trade_type,
                                "entry_price": price,
                                "quantity": qty,
                                "amount": order_amount,
                                "highest_price": price,
                                "lowest_price": price,
                                "sl_price": sl_price,
                                "target_price": target_price,
                                "current_price": price,
                                "current_pnl_percent": 0.0,
                                "current_pnl_val": 0.0,
                                "time": get_global_time()
                            }
                            state["active_trades"].insert(0, new_trade)
                            db_save_active_trade(new_trade, dev_id, state.get("active_broker", "paper"))
                            save_state_to_db(dev_id, state)
                            add_log(state, f"⚡ OPENED {trade_type} [{len(state['active_trades'])}/{allowed_slots}]: {base} at {price}")

                    except Exception as loop_err:
                        print(f"Error in user loop: {loop_err}")

        except Exception as e:
            print(f"Scanner fatal error: {e}")

        await asyncio.sleep(2.0)

# ----------------- REST API ROUTES -----------------
@app.post("/api/set-broker-mode")
async def set_broker_mode(request: Request):
    """Saves Paper vs Real mode permanently to prevent flip-back"""
    data = await request.json()
    device_id = data.get("device_id", "DEFAULT_DEVICE")
    state = get_user_session(device_id)
    mode = str(data.get("mode", "paper")).lower()

    if mode == "paper":
        state["active_broker"] = "paper"
    else:
        state["active_broker"] = state.get("market_data_exchange", "coindcx")

    save_state_to_db(device_id, state)
    add_log(state, f"⚡ Broker Mode: {mode.upper()} | Active: {state['active_broker'].upper()}")
    return {"status": "success", "active_broker": state["active_broker"]}

@app.get("/api/market-index")
async def get_market_index(device_id: str = "DEFAULT_DEVICE"):
    """Fixes HTTP 404 for top 100 coins feed"""
    state = get_user_session(device_id)
    try:
        coins = await asyncio.to_thread(fetch_active_exchange_markets, state)
        return {
            "status": "success",
            "exchange": get_market_data_exchange(state),
            "coins": coins
        }
    except Exception as e:
        return {"status": "error", "message": str(e), "coins": []}

@app.post("/api/bot-control")
async def bot_control(request: Request):
    data = await request.json()
    device_id = data.get("device_id", "DEFAULT_DEVICE")
    state = get_user_session(device_id)
    action = data.get("action")

    if action == "start":
        state["is_running"] = True
        if "max_trades" in data:
            state["max_trades"] = max(1, int(data["max_trades"]))
        if "amount" in data:
            state["trade_amount"] = float(data["amount"])
        if "target_percent" in data:
            state["target_percent"] = float(data["target_percent"])
        if "sl_percent" in data:
            state["sl_percent"] = float(data["sl_percent"])
        if "currency" in data:
            state["quote_currency"] = str(data["currency"]).upper()
        if "deal_condition" in data:
            state["deal_condition"] = str(data["deal_condition"])
        if "target_coin" in data:
            state["selected_coin"] = str(data["target_coin"]).upper()

        save_state_to_db(device_id, state)
        add_log(state, f"🚀 BOT STARTED | Slots: {state['max_trades']} | Amount: {get_curr_symbol(state)}{state['trade_amount']}")
        return {
            "status": "success",
            "is_running": True,
            "max_trades": state["max_trades"],
            "active_broker": state["active_broker"]
        }

    if action == "stop":
        state["is_running"] = False
        save_state_to_db(device_id, state)
        add_log(state, "🛑 BOT STOPPED")
        return {"status": "success", "is_running": False}

    return {"status": "error", "message": "Unknown action"}

@app.get("/api/get-trades")
def get_trades(device_id: str = "DEFAULT_DEVICE"):
    state = get_user_session(device_id)
    check_midnight_settlement(state)
    history_records = db_get_all_trades(device_id, limit=500)
    current_currency = str(state.get("quote_currency", "INR")).upper()
    display_today_pnl = round(sum(
        float(t.get("pnl_val", 0.0) or 0.0)
        for t in history_records
        if str(t.get("currency", current_currency)).upper() == current_currency
    ), 2)
    return {
        "status": "success",
        "active": state["active_trades"],
        "history": history_records,
        "paper_balance": fetch_real_cash_balance(state),
        "market_mode": state["market_mode"],
        "quote_currency": state["quote_currency"],
        "currency_symbol": get_curr_symbol(state),
        "today_pnl": display_today_pnl,
        "is_running": bool(state.get("is_running")),
        "max_trades": state.get("max_trades", 5),
        "target_percent": state.get("target_percent", 2.5),
        "sl_percent": state.get("sl_percent", 1.5),
        "active_broker": state.get("active_broker", "paper"),
        "market_data_exchange": get_market_data_exchange(state)
    }

@app.get("/api/bot-logs")
def get_bot_logs(device_id: str = "DEFAULT_DEVICE"):
    state = get_user_session(device_id)
    return {
        "status": "success",
        "is_running": state["is_running"],
        "logs": state["logs"],
        "max_trades": state.get("max_trades", 5),
        "active_broker": state["active_broker"],
        "quote_currency": state["quote_currency"],
        "currency_symbol": get_curr_symbol(state),
        "trade_amount": state["trade_amount"],
        "scanner_reports": state.get("scanner_reports", []),
        "scanner_cycle": state.get("scanner_cycle", 0)
    }

@app.post("/api/close-trade")
async def close_trade(request: Request):
    data = await request.json()
    device_id = data.get("device_id", "DEFAULT_DEVICE")
    state = get_user_session(device_id)
    trade_id = str(data.get("id"))

    trade = next((t for t in state["active_trades"] if str(t.get("id")) == trade_id), None)
    if not trade:
        return {"status": "error", "message": "Trade not found"}

    ok, exit_p, qty, msg = _close_trade_at_market(state, device_id, trade, "MANUAL EXIT")
    return {"status": "success", "message": "Trade closed", "active": state["active_trades"]}

@app.get("/api/health")
def health_check():
    return {"status": "ok", "service": "hitechpro", "time": get_global_time()}

@app.get("/")
def root():
    return {"status": "HiTechPro Trading Engine Live", "database": "SQLite Active"}

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
