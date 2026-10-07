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
import threading
import uuid
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from urllib.parse import urlencode

try:
    from cryptography.hazmat.primitives.asymmetric import ed25519
except Exception:
    ed25519 = None

# ----------------- DATABASE SETUP (PERMANENT EXACT HISTORY) -----------------
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
    # Backward-compatible schema upgrades for existing installations.
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

    # Ensure migrations that depend on active_trades run after its CREATE TABLE.
    try:
        cursor.execute("ALTER TABLE active_trades ADD COLUMN exchange TEXT")
    except sqlite3.OperationalError:
        pass
    conn.commit()
    conn.close()

init_db()

def db_save_trade(trade: dict, device_id: str, broker: str):
    try:
        conn = sqlite3.connect(DB_FILE)
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
        close_t = str(trade.get("close_time", datetime.now(timezone.utc).isoformat() + "Z"))

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
    """Persist an OPEN trade immediately. This prevents loss on server restart."""
    conn = sqlite3.connect(DB_FILE)
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN')
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
    conn = sqlite3.connect(DB_FILE)
    try:
        conn.execute("DELETE FROM active_trades WHERE trade_id = ?", (str(trade_id),))
        conn.commit()
    finally:
        conn.close()


def db_load_active_trades(device_id: str):
    conn = sqlite3.connect(DB_FILE)
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


def db_save_wallet_state(device_id: str, state: dict):
    save_state_to_db(device_id, state)


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

def save_state_to_db(device_id: str, state: dict):
    try:
        conn = sqlite3.connect(DB_FILE)
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
            state.get("active_broker", "coindcx"),
            state.get("market_mode", "spot"),
            state.get("quote_currency", "INR"),
            state.get("trade_amount", 500.0),
            state.get("max_trades", 1),
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

def _load_persisted_runtime_sessions():
    """Recover every persisted bot session/position after a process restart.
    Real-money sessions are recovered but will not place new orders until API
    credentials are reconnected; public-price monitoring remains safe.
    """
    try:
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM bot_state WHERE is_running = 1 OR device_id IN (SELECT device_id FROM active_trades WHERE status='OPEN')").fetchall()
        conn.close()
        for row in rows:
            dev = row["device_id"]
            if dev not in user_sessions:
                user_sessions[dev] = {
                    "is_running": bool(row["is_running"]),
                    "active_broker": row["active_broker"] or "coindcx",
                    "market_data_exchange": row["market_data_exchange"] or (row["active_broker"] if row["active_broker"] != "paper" else "coindcx"),
                    "market_mode": row["market_mode"] or "spot",
                    "api_key": "", "secret_key": "",
                    "quote_currency": row["quote_currency"] or "INR",
                    "trade_amount": float(row["trade_amount"] or 500.0),
                    "max_trades": int(row["max_trades"] or 1),
                    "target_percent": float(row["target_percent"] or 2.5),
                    "sl_percent": float(row["sl_percent"] or 1.5),
                    "selected_coin": row["selected_coin"] or "AUTO",
                    "deal_condition": row["deal_condition"] or "ASAP",
                    "trade_type": "intraday", "strategy": "volume",
                    "logs": ["🔄 Runtime recovered from persistent database."],
                    "active_trades": db_load_active_trades(dev),
                    "paper_balance": float(row["paper_balance"] if row["paper_balance"] is not None else 500000.0),
                    "today_pnl": float(row["today_pnl"] if row["today_pnl"] is not None else 0.0),
                    "session_start_fund": 0.0, "sleep_until": None, "sleep_reason": "",
                    "last_settlement_date": row["last_settlement_date"] or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    "_closing_ids": set(), "_last_recovery_log": 0.0
                }
                if user_sessions[dev]["active_trades"] and user_sessions[dev]["active_broker"] != "paper":
                    user_sessions[dev]["is_running"] = False
                    add_log(user_sessions[dev], "🛡️ Real-trading safety pause after server restart: reconnect API credentials before new orders.")
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
    allow_origins=os.environ.get("HITECH_CORS_ORIGINS", "*").split(","),
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

user_sessions = {}
_analysis_cache = {}
_tv_paper_cache = {"ts": 0.0, "quote": "", "coins": []}
_tv_paper_cache_lock = threading.Lock()
TV_PAPER_CACHE_TTL = 45

def get_user_session(device_id: str):
    if not device_id:
        device_id = "DEFAULT_DEVICE"
    if device_id not in user_sessions:
        conn = sqlite3.connect(DB_FILE)
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
                "active_broker": row[1] or "coindcx",
                "market_data_exchange": row[13] or (row[1] if row[1] and row[1] != "paper" else "coindcx"),
                "market_mode": row[2] or "spot",
                "api_key": "",
                "secret_key": "",
                "quote_currency": row[3] or "INR",
                "trade_amount": float(row[4] or 500.0),
                "max_trades": int(row[5] or 1),
                "target_percent": float(row[6] or 2.5),
                "sl_percent": float(row[7] or 1.5),
                "selected_coin": row[8] or "AUTO",
                "deal_condition": row[9] or "ASAP",
                "trade_type": "intraday",
                "strategy": "volume",
                "logs": ["🤖 Bot session recovered from database successfully."],
                "active_trades": db_load_active_trades(device_id),
                "paper_balance": float(row[10] if row[10] is not None else 500000.0),
                "today_pnl": float(row[11] if row[11] is not None else 0.0),
                "session_start_fund": 0.0,
                "sleep_until": None,
                "sleep_reason": "",
                "last_settlement_date": row[12] or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "_closing_ids": set(),
                "scanner_reports": [],
                "scanner_cycle": 0,
                "scanner_updated_at": None
            }
        else:
            user_sessions[device_id] = {
                "is_running": False,
                "active_broker": "coindcx",
                "market_data_exchange": "coindcx",
                "market_mode": "spot",
                "api_key": "",
                "secret_key": "",
                "quote_currency": "INR",
                "trade_amount": 500.0,
                "max_trades": 1,
                "trade_type": "intraday",
                "strategy": "volume",
                "deal_condition": "ASAP",
                "selected_coin": "AUTO",
                "target_percent": 2.5,
                "sl_percent": 1.5,
                "logs": ["🤖 Master AI Dual Engine Initialized. Target/SL monitoring ready."],
                "active_trades": [],
                "paper_balance": 500000.0,
                "today_pnl": 0.0,
                "session_start_fund": 0.0,
                "sleep_until": None,
                "sleep_reason": "",
                "last_settlement_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "_closing_ids": set(),
                "scanner_reports": [],
                "scanner_cycle": 0,
                "scanner_updated_at": None
            }
    state = user_sessions[device_id]
    state.setdefault("target_percent", 2.5)
    state.setdefault("sl_percent", 1.5)
    state.setdefault("_closing_ids", set())
    state.setdefault("scanner_reports", [])
    state.setdefault("scanner_cycle", 0)
    state.setdefault("scanner_updated_at", None)
    return state

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


def record_scanner_report(state, report):
    """Store the latest authoritative per-coin scanner report for the UI.

    Reports are derived only from the same exchange market feed and technical
    analysis used by the bot. Paper mode therefore still uses real market data.
    """
    reports = state.setdefault("scanner_reports", [])
    reports.insert(0, dict(report))
    # Keep enough rows for the terminal without allowing unbounded memory growth.
    del reports[120:]
    state["scanner_updated_at"] = get_global_time()


def _active_position_for_coin(state, base_coin):
    base = str(base_coin or "").upper().replace("INR", "").replace("USDT", "").replace("/", "")
    for trade in state.get("active_trades", []):
        sym = str(trade.get("symbol", "")).upper().replace("INR", "").replace("USDT", "").replace("/", "")
        if sym == base:
            return str(trade.get("type", "ACTIVE")).upper()
    return "FLAT"

def check_midnight_settlement(state):
    current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if current_date != state["last_settlement_date"]:
        # Realized P&L is already credited to paper_balance at every paper exit.
        # Do NOT add today_pnl again at midnight; doing so would double-count profit/loss.
        settled_amount = float(state.get("today_pnl", 0.0) or 0.0)
        state["today_pnl"] = 0.0
        state["last_settlement_date"] = current_date
        save_state_to_db(next((k for k, v in user_sessions.items() if v is state), "DEFAULT_DEVICE"), state)
        add_log(state, f"🏦 Daily P&L rollover: {get_curr_symbol(state)}{settled_amount:.2f} archived; wallet unchanged.")

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

@app.post("/api/backtest")
async def run_backtest(request: Request):
    """Exchange-native historical backtest. Never reports fabricated win rates."""
    try:
        data = await request.json()
        device_id = data.get("device_id", "DEFAULT_DEVICE")
        state = get_user_session(device_id)
        strategy = str(data.get("strategy", "RSI_FAV")).upper()
        days = max(1, min(30, int(data.get("days", 7))))
        coin = str(data.get("symbol", state.get("selected_coin", "BTC"))).upper()
        if coin == "AUTO":
            coin = "BTC"
        clean = coin.replace("INR", "").replace("USDT", "").replace("/", "")
        markets = fetch_active_exchange_markets(state)
        market = next((m for m in markets if str(m.get("base_coin", "")).upper() == clean), None)
        if not market:
            return {"status":"error", "message":f"{clean} is unavailable on {get_market_data_exchange(state).upper()} in {state.get('quote_currency','INR')}."}
        if get_market_data_exchange(state) == "tradingview":
            return {"status":"error","message":"TradingView Paper Mode live scanner does not substitute another exchange for historical backtesting."}
        candles = _fetch_analysis_ohlcv(clean, state, market.get("raw_symbol"), "15m", min(500, days*96+220))
        if len(candles) < 80:
            return {"status":"error", "message":"Not enough historical candles from the selected exchange for a real backtest."}
        closes=[float(x["close"]) for x in candles]
        highs=[float(x["high"]) for x in candles]
        lows=[float(x["low"]) for x in candles]
        fee_bps=float(os.environ.get("HITECH_BACKTEST_FEE_BPS","10"))
        fee=fee_bps/10000.0
        trades=[]; position=None; equity=1.0; wins=losses=0
        lookback=200
        for i in range(lookback, len(closes)-1):
            window=closes[:i+1]
            rsi=_rsi(window,14); macd,signal,_prev=_macd(window); ema50=_ema(window,50); ema200=_ema(window,200)
            if rsi is None or macd is None or signal is None or ema50 is None or ema200 is None: continue
            long_signal = rsi < 35 and macd > signal and closes[i] > ema50 > ema200
            short_signal = rsi > 65 and macd < signal and closes[i] < ema50 < ema200
            if position is None:
                if long_signal or (state.get("market_mode") == "futures" and short_signal):
                    position={"side":"LONG" if long_signal else "SHORT","entry":closes[i],"i":i}
            else:
                side=position["side"]; entry=position["entry"]; exit_now=(side=="LONG" and (short_signal or rsi>70)) or (side=="SHORT" and (long_signal or rsi<30))
                if exit_now:
                    exit_p=closes[i]; gross=(exit_p-entry)/entry if side=="LONG" else (entry-exit_p)/entry
                    net=gross-2*fee; equity*=1+net
                    trades.append(net*100); wins+=net>0; losses+=net<=0; position=None
        if position is not None:
            exit_p=closes[-1]; entry=position["entry"]; gross=(exit_p-entry)/entry if position["side"]=="LONG" else (entry-exit_p)/entry
            net=gross-2*fee; equity*=1+net; trades.append(net*100); wins+=net>0; losses+=net<=0
        total=len(trades); win_rate=(wins/total*100) if total else 0.0
        return {"status":"success","exchange":get_market_data_exchange(state),"symbol":f"{clean}/{state.get('quote_currency','INR').upper()}","strategy":strategy,"period_days":days,"total_deals":total,"win_rate":round(win_rate,2),"winning_deals":wins,"losing_deals":losses,"net_profit_pct":round((equity-1)*100,2),"max_loss_pct":round(min(trades),2) if trades else 0.0,"message":"Backtest uses historical candles from the selected exchange; results are not a profit guarantee."}
    except Exception as e:
        return {"status":"error","message":f"Backtest error: {e}"}

@app.get("/api/sentiment")
def get_market_sentiment(device_id: str = "DEFAULT_DEVICE"):
    """Exchange-native technical mood; deliberately not presented as live news/AI."""
    state=get_user_session(device_id)
    markets=fetch_active_exchange_markets(state)
    if not markets:
        return {"status":"error","message":"No market data available from the selected exchange."}
    changes=[float(m.get("change",0) or 0) for m in markets[:30]]
    avg=sum(changes)/len(changes) if changes else 0.0
    mood="BULLISH" if avg>1.0 else "BEARISH" if avg<-1.0 else "NEUTRAL"
    return {"status":"success","source_exchange":get_market_data_exchange(state),"market_mood":mood,"average_24h_change":round(avg,2),"note":"Technical exchange-ticker mood only; no external news feed is claimed."}

def get_market_data_exchange(state):
    """Paper is always TradingView; Real is always the user's connected broker."""
    broker = str(state.get("active_broker", "coindcx") or "coindcx").lower()
    if broker == "paper":
        return "tradingview"
    return broker


def _tv_scan_cached(payload, cache_key, ttl=45):
    """TradingView public scanner with short caching to reduce repeated 429s."""
    now = time.time()
    cache = getattr(_tv_scan_cached, "_cache", {})
    item = cache.get(cache_key)
    if item and now - item["ts"] < ttl:
        return item["data"]
    response = requests.post("https://scanner.tradingview.com/crypto/scan", json=payload, headers={"User-Agent":"HiTech-Trading-Pro/1.0","Content-Type":"application/json"}, timeout=10)
    response.raise_for_status()
    body = response.json()
    rows = body.get("data", []) if isinstance(body, dict) else []
    cache[cache_key] = {"ts": now, "data": rows}
    _tv_scan_cached._cache = cache
    return rows


def _fetch_tradingview_paper_markets(quote):
    quote = str(quote or "USDT").upper()
    now = time.time()
    with _tv_paper_cache_lock:
        if _tv_paper_cache["coins"] and _tv_paper_cache["quote"] == quote and now - _tv_paper_cache["ts"] < TV_PAPER_CACHE_TTL:
            return list(_tv_paper_cache["coins"])
    columns=["name","close","change","24h_vol|5","RSI|15","RSI|60","MACD.macd|15","MACD.signal|15","EMA20|15","EMA50|15","EMA200|15","EMA50|60","relative_volume_10d_calc|15","Stoch.K|15","Stoch.D|15"]
    payload={"filter":[{"left":"exchange","operation":"equal","right":"BINANCE"}],"symbols":{"query":{"types":[]}},"columns":columns,"sort":{"sortBy":"24h_vol|5","sortOrder":"desc"},"options":{"lang":"en"},"range":[0,100]}
    rows=_tv_scan_cached(payload,"paper-binance-top100",TV_PAPER_CACHE_TTL)
    usd_inr=1.0
    if quote=="INR":
        fx_payload={"filter":[],"symbols":{"tickers":["FX_IDC:USDINR"],"query":{"types":[]}},"columns":["close"],"options":{"lang":"en"},"range":[0,1]}
        fx_rows=_tv_scan_cached(fx_payload,"paper-usdinr",TV_PAPER_CACHE_TTL)
        try: usd_inr=float(fx_rows[0].get("d",[0])[0] or 0) if fx_rows else 0.0
        except Exception: usd_inr=0.0
        if usd_inr<=0: raise RuntimeError("TradingView USDINR quote unavailable")
    out=[]
    for row in rows:
        d=row.get("d",[]) if isinstance(row,dict) else []
        if not d: continue
        vals={columns[i]:(d[i] if i<len(d) else None) for i in range(len(columns))}
        symbol=str(row.get("s","") or "").upper()
        if ":" in symbol: symbol=symbol.split(":",1)[1]
        base=symbol.replace("USDT","").replace("USD","").replace("/","").strip()
        try: usd_price=float(vals.get("close") or 0)
        except Exception: usd_price=0.0
        if not base or usd_price<=0: continue
        mult=usd_inr if quote=="INR" else 1.0
        out.append({"symbol":base+quote,"base_coin":base,"raw_symbol":f"BINANCE:{base}USDT","tv_symbol":f"BINANCE:{base}USDT","price":usd_price*mult,"volume":float(vals.get("24h_vol|5") or 0)*mult,"change":float(vals.get("change") or 0),"exchange":"tradingview","tv":{"rsi15":vals.get("RSI|15"),"rsi60":vals.get("RSI|60"),"macd":vals.get("MACD.macd|15"),"macd_signal":vals.get("MACD.signal|15"),"ema20":vals.get("EMA20|15"),"ema50":vals.get("EMA50|15"),"ema200":vals.get("EMA200|15"),"ema50_60":vals.get("EMA50|60"),"volume_ratio":vals.get("relative_volume_10d_calc|15"),"stoch_k":vals.get("Stoch.K|15"),"stoch_d":vals.get("Stoch.D|15")}})
    out.sort(key=lambda x:float(x.get("volume",0) or 0),reverse=True)
    out=out[:100]
    with _tv_paper_cache_lock: _tv_paper_cache.update({"ts":time.time(),"quote":quote,"coins":out})
    return list(out)


def fetch_active_exchange_markets(state):
    """Return ONLY markets from the exchange selected for this user/session.
    There is deliberately no Binance fallback: cross-exchange leakage would make
    the scanner, price, P&L and execution inconsistent with the selected broker.
    """
    broker = get_market_data_exchange(state)
    quote = state.get("quote_currency", "INR").upper()
    market_list = []

    if broker == "tradingview":
        try: return _fetch_tradingview_paper_markets(quote)
        except Exception as exc:
            add_log(state, f"⚠️ TRADINGVIEW PAPER DATA ERROR: {exc}")
            return []

    try:
        if broker == "wazirx":
            res = requests.get("https://api.wazirx.com/sapi/v1/tickers/24hr", timeout=8)
            res.raise_for_status()
            data = res.json()
            for item in data if isinstance(data, list) else []:
                q = str(item.get("quoteAsset", "") or "").upper()
                base = str(item.get("baseAsset", "") or "").upper()
                if q != quote or not base:
                    continue
                price = float(item.get("lastPrice", 0) or 0)
                if price <= 0:
                    continue
                open_price = float(item.get("openPrice", 0) or 0)
                change = ((price - open_price) / open_price * 100.0) if open_price > 0 else 0.0
                market_list.append({"symbol": base + quote, "base_coin": base, "raw_symbol": base + quote, "price": price, "volume": float(item.get("volume", 0) or 0), "change": change, "exchange": "wazirx"})
        elif broker == "coindcx":
            res = requests.get("https://api.coindcx.com/exchange/ticker", timeout=5)
            res.raise_for_status()
            data = res.json()
            for item in data:
                m = str(item.get("market", "") or "")
                price = float(item.get("last_price", 0.0) or 0.0)
                vol = float(item.get("volume", 0.0) or 0.0)
                change = float(item.get("change_24_hour", 0.0) or 0.0)
                if price <= 0:
                    continue
                is_inr = m.endswith("_INR") or m.endswith("INR")
                is_usdt = m.endswith("_USDT") or m.endswith("USDT")
                if quote == "INR" and not is_inr:
                    continue
                if quote == "USDT" and not is_usdt:
                    continue
                clean_coin = (m.replace("B-", "").replace("I-", "")
                               .replace("_", "").replace("INR", "")
                               .replace("USDT", "").upper())
                market_list.append({
                    "symbol": clean_coin + quote,
                    "base_coin": clean_coin,
                    "raw_symbol": m,
                    "price": price,
                    "volume": vol,
                    "change": change,
                    "exchange": "coindcx"
                })
        elif hasattr(ccxt, broker):
            exchange_class = getattr(ccxt, broker)
            inst = exchange_class({'enableRateLimit': True, 'timeout': 5000})
            tickers = inst.fetch_tickers()
            target_suffix = f"/{quote}"
            for sym, t in tickers.items():
                if not sym.endswith(target_suffix):
                    continue
                c_base = sym.split("/")[0]
                price = float(t.get("last", 0.0) or 0.0)
                if price <= 0:
                    continue
                market_list.append({
                    "symbol": sym.replace("/", ""),
                    "base_coin": c_base,
                    "raw_symbol": sym,
                    "price": price,
                    "volume": float(t.get("quoteVolume", 0.0) or 0.0),
                    "change": float(t.get("percentage", 0.0) or 0.0),
                    "exchange": broker
                })
        else:
            add_log(state, f"⚠️ EXCHANGE UNSUPPORTED: {broker.upper()} has no market-data adapter installed.")
            return []
    except Exception as exc:
        add_log(state, f"⚠️ {broker.upper()} MARKET DATA ERROR: {exc}")
        return []

    market_list.sort(key=lambda x: float(x.get("volume", 0.0) or 0.0), reverse=True)
    return market_list[:100]

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

# ----------------- 30-BROKER API ADAPTER LAYER -----------------
# The UI exposes 30 real exchanges.  This registry prevents UI names from
# being passed blindly to CCXT and gives exchanges with non-CCXT auth a direct
# adapter.  No adapter reports success without an actual authenticated request.
BROKER_CCXT_ALIASES = {
    "crypto_com": "cryptocom",
    "crypto.com": "cryptocom",
    "gate.io": "gateio",
    "gate": "gateio",
    "huobi": "htx",
    "coinbase": "coinbase",
    "delta_exchange": "delta",
}

# Brokers whose authentication format is NOT safe to assume from generic CCXT.
# The UI can use the normal API Key + Secret fields, while adapters below map
# them to the broker's actual authentication scheme.
BROKER_AUTH_MODES = {
    "mudrex": "secret_header",
    "coinswitch": "ed25519",
    "wazirx": "hmac_sha256",
    "delta": "hmac_sha256_headers",
    "coindcx": "hmac_sha256_headers",
}

SUPPORTED_REAL_BROKERS = {
    "coindcx", "wazirx", "coinswitch", "zebpay", "mudrex", "delta",
    "pi42", "bitbns", "giottus", "unocoin", "binance", "bybit", "okx",
    "bitget", "kucoin", "gateio", "mexc", "htx", "kraken", "coinbase",
    "bingx", "phemex", "bitmart", "lbank", "coinex", "deribit", "bitfinex",
    "bitstamp", "crypto_com", "whitebit"
}


def _ccxt_id_for_broker(broker):
    broker = str(broker or "").lower().strip()
    return BROKER_CCXT_ALIASES.get(broker, broker)


def _wazirx_signed_request(state, method, path, params=None, timeout=8):
    """WazirX SAPI signed request. Never logs API credentials/signatures."""
    api_key = str(state.get("api_key", "") or "").strip()
    secret_key = str(state.get("secret_key", "") or "").strip()
    if not api_key or not secret_key:
        raise ValueError("WazirX API key and secret key are required.")

    payload = dict(params or {})
    payload.setdefault("recvWindow", 5000)
    payload["timestamp"] = int(time.time() * 1000)
    query = urlencode(payload)
    payload["signature"] = hmac.new(
        secret_key.encode("utf-8"), query.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    headers = {"X-API-KEY": api_key, "Content-Type": "application/x-www-form-urlencoded"}
    url = "https://api.wazirx.com" + path
    response = requests.request(method.upper(), url, headers=headers, data=payload, timeout=timeout)
    try:
        body = response.json()
    except Exception:
        body = {"message": response.text[:500]}
    if response.status_code >= 400:
        message = body.get("msg") or body.get("message") if isinstance(body, dict) else str(body)
        raise RuntimeError(f"HTTP {response.status_code}: {message or 'WazirX request failed'}")
    if isinstance(body, dict) and body.get("code") not in (None, 0, "0"):
        raise RuntimeError(f"WazirX error {body.get('code')}: {body.get('msg') or body.get('message')}")
    return body


def _wazirx_balances(state):
    data = _wazirx_signed_request(state, "GET", "/sapi/v1/funds")
    balances = {}
    if not isinstance(data, list):
        raise RuntimeError("WazirX returned an unexpected funds response.")
    for item in data:
        asset = str(item.get("asset", "") or "").upper()
        if not asset:
            continue
        free = float(item.get("free", 0) or 0)
        locked = float(item.get("locked", 0) or 0)
        total = free + locked
        if total > 1e-12:
            balances[asset] = total
    return balances


def _json_or_text(response, broker):
    try:
        body = response.json()
    except Exception:
        body = {"message": response.text[:500]}
    if response.status_code >= 400:
        if isinstance(body, dict):
            msg = body.get("message") or body.get("msg") or body.get("error") or body.get("error_description")
        else:
            msg = str(body)
        raise RuntimeError(f"HTTP {response.status_code}: {msg or 'request failed'}")
    return body


def _coinswitch_signed_request(method, path, api_key, secret_key, params=None, body=None, timeout=8):
    """CoinSwitch PRO Spot v2 Ed25519 authentication."""
    if ed25519 is None:
        raise RuntimeError("CoinSwitch requires the 'cryptography' package for Ed25519 authentication.")
    if not api_key or not secret_key:
        raise ValueError("CoinSwitch API key and secret key are required.")
    path = str(path)
    if params:
        path += ("&" if "?" in path else "?") + urlencode(params)
    # CoinSwitch signs METHOD + URL-decoded path+query + epoch.
    from urllib.parse import unquote_plus
    decoded_path = unquote_plus(path)
    epoch = str(int(time.time() * 1000))
    message = method.upper() + decoded_path + epoch
    try:
        private_key = ed25519.Ed25519PrivateKey.from_private_bytes(bytes.fromhex(secret_key))
    except Exception as e:
        raise RuntimeError(f"Invalid CoinSwitch Ed25519 secret format: {e}")
    signature = private_key.sign(message.encode("utf-8")).hex()
    headers = {
        "Content-Type": "application/json",
        "X-AUTH-APIKEY": api_key,
        "X-AUTH-SIGNATURE": signature,
        "X-AUTH-EPOCH": epoch,
    }
    url = "https://coinswitch.co" + decoded_path
    response = requests.request(method.upper(), url, headers=headers, json=body if body is not None else None, timeout=timeout)
    return _json_or_text(response, "CoinSwitch")


def _coinswitch_balances(api_key, secret_key):
    # validate first: a 200 Valid Access is the connection proof.
    _coinswitch_signed_request("GET", "/trade/api/v2/validate/keys", api_key, secret_key)
    data = _coinswitch_signed_request("GET", "/trade/api/v2/portfolio", api_key, secret_key)
    balances = {}
    payload = data.get("data", data) if isinstance(data, dict) else data
    if isinstance(payload, dict):
        # Current API may expose holdings under portfolio/holdings/balances.
        rows = payload.get("portfolio") or payload.get("holdings") or payload.get("balances") or []
    else:
        rows = payload
    if isinstance(rows, dict):
        rows = [{"currency": k, "balance": v} for k, v in rows.items()]
    if isinstance(rows, list):
        for item in rows:
            if not isinstance(item, dict):
                continue
            coin = str(item.get("currency") or item.get("coin") or item.get("asset") or "").upper()
            amount = item.get("total")
            if amount is None: amount = item.get("balance")
            if amount is None: amount = item.get("quantity")
            try: amount = float(amount or 0)
            except Exception: continue
            if coin and amount > 1e-12:
                balances[coin] = amount
    return balances


def _mudrex_request(method, path, secret_key, params=None, body=None, timeout=8):
    if not secret_key:
        raise ValueError("Mudrex API secret is required.")
    url = "https://trade.mudrex.com/fapi/v1" + path
    headers = {"X-Authentication": secret_key, "Content-Type": "application/json"}
    response = requests.request(method.upper(), url, headers=headers, params=params, json=body if body is not None else None, timeout=timeout)
    data = _json_or_text(response, "Mudrex")
    if isinstance(data, dict) and data.get("success") is False:
        errors = data.get("errors") or []
        msg = errors[0].get("text") if errors and isinstance(errors[0], dict) else data.get("message")
        raise RuntimeError(msg or "Mudrex authentication failed")
    return data


def _mudrex_balances(secret_key):
    # Authenticated spot-wallet read is the cleanest connection proof and does
    # not place an order. The endpoint accepts currency=INR/USDT.
    balances = {}
    for currency in ("INR", "USDT"):
        data = _mudrex_request("GET", "/wallet/funds", secret_key, params={"currency": currency})
        payload = data.get("data", data) if isinstance(data, dict) else data
        if isinstance(payload, dict):
            amount = payload.get("balance")
            if amount is None: amount = payload.get("available_balance")
            if amount is None: amount = payload.get("available")
            if amount is not None:
                try:
                    amount = float(amount)
                    if amount > 1e-12:
                        balances[currency] = amount
                except Exception:
                    pass
    return balances


def _delta_signed_request(method, path, api_key, secret_key, params=None, body=None, timeout=8):
    """Delta Exchange REST v2 HMAC authentication."""
    if not api_key or not secret_key:
        raise ValueError("Delta API key and secret key are required.")
    method = method.upper()
    query = urlencode(params or {})
    body_text = json.dumps(body, separators=(",", ":")) if body is not None else ""
    timestamp = str(int(time.time()))
    prehash = method + timestamp + path + query + body_text
    signature = hmac.new(secret_key.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).hexdigest()
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "api-key": api_key,
        "signature": signature,
        "timestamp": timestamp,
    }
    url = "https://api.delta.exchange" + path
    response = requests.request(method, url, headers=headers, params=params, data=body_text if body is not None else None, timeout=timeout)
    data = _json_or_text(response, "Delta")
    if isinstance(data, dict) and data.get("success") is False:
        raise RuntimeError(data.get("message") or data.get("error") or "Delta authentication failed")
    return data


def _delta_balances(api_key, secret_key):
    data = _delta_signed_request("GET", "/v2/wallet/balances", api_key, secret_key)
    rows = data.get("result", []) if isinstance(data, dict) else []
    balances = {}
    if isinstance(rows, list):
        for item in rows:
            if not isinstance(item, dict): continue
            coin = str(item.get("asset_symbol") or item.get("symbol") or "").upper()
            amount = item.get("available_balance")
            if amount is None: amount = item.get("balance")
            try: amount = float(amount or 0)
            except Exception: continue
            if coin and amount > 1e-12:
                balances[coin] = amount
    return balances


def _connect_ccxt_broker(exchange_id, api_key, secret_key, state):
    ccxt_id = _ccxt_id_for_broker(exchange_id)
    if not hasattr(ccxt, ccxt_id):
        raise RuntimeError(
            f"{exchange_id.upper()} needs a dedicated adapter; it is not available in the installed CCXT build."
        )
    exchange_class = getattr(ccxt, ccxt_id)
    exchange = exchange_class({
        "apiKey": api_key,
        "secret": secret_key,
        "enableRateLimit": True,
        "timeout": 8000,
    })
    balance = exchange.fetch_balance()
    total = balance.get("total", {}) or {}
    dynamic = {
        str(coin).upper(): float(amount)
        for coin, amount in total.items()
        if isinstance(amount, (int, float)) and amount > 1e-12
    }
    return dynamic


def fetch_real_cash_balance(state):
    broker = state.get("active_broker", "coindcx").lower()
    api_key = state.get("api_key", "").strip()
    secret_key = state.get("secret_key", "").strip()
    quote = state.get("quote_currency", "INR").upper()

    if broker == "paper":
        return round(max(0.0, float(state.get("paper_balance", 500000.0))), 2)
    if not api_key or not secret_key:
        return 0.0

    try:
        if broker == "coindcx":
            time_stamp = int(round(time.time() * 1000))
            body = {"timestamp": time_stamp}
            json_body = json.dumps(body, separators=(',', ':'))
            signature = hmac.new(secret_key.encode('utf-8'), json_body.encode('utf-8'), hashlib.sha256).hexdigest()
            headers = {'Content-Type': 'application/json', 'X-AUTH-APIKEY': api_key, 'X-AUTH-SIGNATURE': signature}
            res = requests.post("https://api.coindcx.com/exchange/v1/users/balances", data=json_body, headers=headers, timeout=8)
            res.raise_for_status()
            res_data = res.json()
            if isinstance(res_data, list):
                for item in res_data:
                    if str(item.get("currency", "")).upper() == quote:
                        return float(item.get("balance", 0.0) or 0) + float(item.get("lock", 0.0) or 0)
            return 0.0
        elif broker == "wazirx":
            balances = _wazirx_balances(state)
            return float(balances.get(quote, 0.0))
        elif broker == "coinswitch":
            balances = _coinswitch_balances(api_key, secret_key)
            return float(balances.get(quote, 0.0))
        elif broker == "mudrex":
            balances = _mudrex_balances(secret_key)
            return float(balances.get(quote, 0.0))
        elif broker == "delta":
            balances = _delta_balances(api_key, secret_key)
            return float(balances.get(quote, 0.0))
        elif broker in SUPPORTED_REAL_BROKERS:
            balances = _connect_ccxt_broker(broker, api_key, secret_key, state)
            return float(balances.get(quote, 0.0))
        return 0.0
    except Exception as e:
        print(f"Balance Fetch Error ({broker}): {e}")
        return 0.0

def _extract_coindcx_order(data):
    if isinstance(data, dict):
        if isinstance(data.get("orders"), list) and data["orders"]:
            return data["orders"][0]
        if isinstance(data.get("order"), dict):
            return data["order"]
        if data.get("id") is not None:
            return data
    elif isinstance(data, list) and data:
        if isinstance(data[0], dict):
            return data[0]
    return {}

def _coindcx_auth_post(state, endpoint, body, timeout=6):
    api_key = state.get("api_key", "").strip()
    secret_key = state.get("secret_key", "").strip()
    if not api_key or not secret_key:
        raise RuntimeError("API Keys missing! Connect in Portfolio tab.")

    json_body = json.dumps(body, separators=(',', ':'))
    signature = hmac.new(
        secret_key.encode("utf-8"),
        json_body.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    headers = {
        "Content-Type": "application/json",
        "X-AUTH-APIKEY": api_key,
        "X-AUTH-SIGNATURE": signature,
    }
    res = requests.post(
        f"https://api.coindcx.com{endpoint}",
        data=json_body,
        headers=headers,
        timeout=timeout,
    )
    try:
        payload = res.json()
    except Exception:
        payload = {"message": res.text}
    return res, payload

def get_coindcx_order_status(state, order_id):
    body = {
        "id": int(str(order_id)),
        "timestamp": int(round(time.time() * 1000)),
    }
    res, payload = _coindcx_auth_post(state, "/exchange/v1/orders/status", body, timeout=5)
    if res.status_code != 200:
        msg = payload.get("message", str(payload)) if isinstance(payload, dict) else str(payload)
        return False, {}, f"Order status HTTP {res.status_code}: {msg}"

    order = _extract_coindcx_order(payload)
    if not order:
        return False, {}, f"Empty CoinDCX order-status response: {payload}"
    return True, order, ""

def _wait_for_coindcx_fill_sync(state, order_id, timeout_seconds=6.0, poll_seconds=0.5):
    deadline = time.time() + float(timeout_seconds)
    last_order = {}
    last_error = ""

    while time.time() <= deadline:
        try:
            ok, order, err = get_coindcx_order_status(state, order_id)
            if not ok:
                last_error = err
            else:
                last_order = order
                status = str(order.get("status", "")).lower()
                total_qty = float(order.get("total_quantity", 0) or 0)
                remaining_qty = float(order.get("remaining_quantity", 0) or 0)
                avg_price = float(order.get("avg_price", 0) or 0)
                filled_qty = max(0.0, total_qty - remaining_qty)

                if status == "filled" and remaining_qty <= 1e-12:
                    return True, filled_qty, avg_price, "FILLED"

                if status in {"rejected", "cancelled", "partially_cancelled"}:
                    return False, filled_qty, avg_price, status.upper()

                last_error = f"Order still {status or 'unknown'} (remaining={remaining_qty:g})"
        except Exception as exc:
            last_error = str(exc)

        time.sleep(poll_seconds)

    if last_order:
        status = str(last_order.get("status", "unknown")).lower()
        total_qty = float(last_order.get("total_quantity", 0) or 0)
        remaining_qty = float(last_order.get("remaining_quantity", 0) or 0)
        avg_price = float(last_order.get("avg_price", 0) or 0)
        filled_qty = max(0.0, total_qty - remaining_qty)
        return False, filled_qty, avg_price, (
            f"TIMEOUT: order is still {status}; remaining quantity={remaining_qty:g}. "
            f"Local trade was NOT closed. {last_error}"
        )

    return False, 0.0, 0.0, f"Unable to confirm CoinDCX order fill. {last_error}"

def execute_coindcx_order(state, raw_symbol, side="buy", target_amount=100.0, exact_qty=0):
    api_key = state.get("api_key", "").strip()
    secret_key = state.get("secret_key", "").strip()
    quote = state.get("quote_currency", "INR").upper()

    if not api_key or not secret_key:
        return False, 0, 0, "API Keys missing! Connect in Portfolio tab."

    try:
        clean_coin = (
            raw_symbol.replace("USDT", "").replace("INR", "")
            .replace("/", "").replace("B-", "").replace("I-", "")
            .replace("_", "").strip().upper()
        )

        ticker_res = requests.get("https://api.coindcx.com/exchange/ticker", timeout=5)
        ticker_res.raise_for_status()
        tickers = ticker_res.json()

        target_market = None
        current_price = 0.0

        for t in tickers:
            m = t.get("market", "")
            if quote == "INR":
                if m in (f"{clean_coin}INR", f"I-{clean_coin}_INR", f"B-{clean_coin}_INR"):
                    target_market = m
                    current_price = float(t.get("last_price", 0.0) or 0.0)
                    break
            else:
                if m in (f"{clean_coin}USDT", f"B-{clean_coin}_USDT", f"I-{clean_coin}_USDT"):
                    target_market = m
                    current_price = float(t.get("last_price", 0.0) or 0.0)
                    break

        order_quote = quote
        if not target_market and quote == "INR":
            for t in tickers:
                m = t.get("market", "")
                if m in (f"{clean_coin}USDT", f"B-{clean_coin}_USDT", f"I-{clean_coin}_USDT"):
                    target_market = m
                    current_price = float(t.get("last_price", 0.0) or 0.0)
                    order_quote = "USDT"
                    break

        if not target_market or current_price <= 0:
            return False, 0, 0, f"Valid CoinDCX pair for '{clean_coin}' not found! Please check symbol."

        precision = get_coin_precision(clean_coin, current_price)

        if exact_qty > 0:
            quantity = float(exact_qty) if precision == 0 else round(float(exact_qty), precision)
            if precision == 0:
                quantity = int(round(quantity))
        else:
            calc_qty = float(target_amount) / current_price
            if precision == 0:
                quantity = int(round(calc_qty))
                if quantity <= 0:
                    quantity = 1
            else:
                quantity = round(calc_qty, precision)

        if side.lower() == "buy" and order_quote == "INR" and (quantity * current_price) < 102.0:
            if precision == 0:
                quantity = int(round(115.0 / current_price)) + 1
            else:
                quantity = round(115.0 / current_price, precision)
                if (quantity * current_price) < 100.0:
                    quantity += round(1.0 / (10 ** precision), precision)

        if quantity <= 0:
            quantity = 1 if precision == 0 else round(1.0 / (10 ** precision), precision)

        body = {
            "side": side.lower(),
            "order_type": "market_order",
            "market": target_market,
            "total_quantity": quantity,
            "timestamp": int(round(time.time() * 1000)),
        }

        res, res_data = _coindcx_auth_post(
            state, "/exchange/v1/orders/create", body, timeout=6
        )
        if res.status_code != 200:
            err_msg = res_data.get("message", str(res_data)) if isinstance(res_data, dict) else str(res_data)
            return False, current_price, quantity, f"HTTP {res.status_code}: {err_msg}"

        order = _extract_coindcx_order(res_data)
        order_id = order.get("id")
        if order_id is None:
            return False, current_price, quantity, f"CoinDCX returned no usable order id: {res_data}"

        filled_ok, filled_qty, avg_price, fill_msg = _wait_for_coindcx_fill_sync(
            state, order_id
        )
        if not filled_ok or filled_qty <= 0:
            return False, avg_price or current_price, filled_qty, (
                f"Order {order_id} was not confirmed filled: {fill_msg}"
            )

        actual_price = float(avg_price or current_price)
        return True, actual_price, float(filled_qty), {
            "order_id": str(order_id),
            "market": target_market,
            "requested_quantity": quantity,
            "filled_quantity": float(filled_qty),
            "status": "filled",
            "avg_price": actual_price,
            "order": order,
        }
    except Exception as e:
        return False, 0, 0, str(e)

def execute_ccxt_order(state, raw_symbol, side="buy", target_amount=100.0, exact_qty=0):
    broker = state.get("active_broker", "").lower()
    api_key = state.get("api_key", "").strip()
    secret_key = state.get("secret_key", "").strip()
    quote = state.get("quote_currency", "USDT").upper()

    if not hasattr(ccxt, broker):
        return False, 0, 0, f"Broker '{broker}' is not supported by execution engine."

    try:
        exchange_class = getattr(ccxt, broker)
        inst = exchange_class({'apiKey': api_key, 'secret': secret_key, 'enableRateLimit': True, 'timeout': 5000})
        clean_coin = raw_symbol.replace("USDT", "").replace("INR", "").replace("/", "").strip().upper()
        symbol_pair = f"{clean_coin}/{quote}"
        ticker = inst.fetch_ticker(symbol_pair)
        current_price = float(ticker.get('last', 0.0))

        if current_price <= 0:
            return False, 0, 0, f"Unable to fetch price for {symbol_pair}"

        precision = get_coin_precision(clean_coin, current_price)
        if exact_qty > 0:
            quantity = exact_qty if precision == 0 else round(exact_qty, precision)
        else:
            calc_qty = float(target_amount) / current_price
            quantity = int(round(calc_qty)) if precision == 0 else round(calc_qty, precision)

        order = inst.create_market_order(symbol_pair, side.lower(), quantity)

        filled = float(order.get("filled", 0) or 0)
        average = float(order.get("average", 0) or 0)
        if filled <= 0:
            filled = float(order.get("amount", 0) or quantity)
        if average <= 0:
            average = current_price

        status = str(order.get("status", "")).lower()
        if status in {"rejected", "canceled", "cancelled"}:
            return False, average, filled, f"Exchange order status: {status}"

        return True, average, filled, order
    except Exception as e:
        return False, 0, 0, str(e)

def _pnl_for_trade(trade, exit_price, qty):
    entry = float(trade.get("entry_price", 0.0) or 0.0)
    exit_p = float(exit_price or 0.0)
    quantity = float(qty or trade.get("quantity", 0.0) or 0.0)
    amount = float(trade.get("amount", entry * quantity) or (entry * quantity))

    if entry <= 0:
        return 0.0, 0.0

    if trade.get("type") in ["LONG", "BUY"]:
        pct = ((exit_p - entry) / entry) * 100.0
        value = (exit_p - entry) * quantity if quantity > 0 else amount * pct / 100.0
    else:
        pct = ((entry - exit_p) / entry) * 100.0
        value = (entry - exit_p) * quantity if quantity > 0 else amount * pct / 100.0
    return round(pct, 2), round(value, 2)

def _update_trade_unrealized_pnl(trade, current_price):
    try:
        entry = float(trade.get("entry_price", 0.0) or 0.0)
        qty = float(trade.get("quantity", 0.0) or 0.0)
        current = float(current_price or 0.0)
        is_long = str(trade.get("type", "LONG")).upper() in {"LONG", "BUY"}

        if entry <= 0 or qty <= 0 or current <= 0:
            trade["current_price"] = current
            trade["current_pnl_percent"] = 0.0
            trade["current_pnl_val"] = 0.0
            trade["unrealized_pnl"] = 0.0
            trade["unrealized_pnl_percent"] = 0.0
            return False

        if is_long:
            pnl_val = (current - entry) * qty
            pnl_pct = ((current - entry) / entry) * 100.0
        else:
            pnl_val = (entry - current) * qty
            pnl_pct = ((entry - current) / entry) * 100.0

        trade["current_price"] = round(current, 8 if current < 1 else 2)
        trade["current_pnl_percent"] = round(pnl_pct, 4)
        trade["current_pnl_val"] = round(pnl_val, 2)
        trade["unrealized_pnl"] = round(pnl_val, 2)
        trade["unrealized_pnl_percent"] = round(pnl_pct, 4)
        return True
    except Exception:
        return False

def _build_live_price_maps(state):
    live_prices = {}
    live_by_base = {}
    try:
        all_coins = fetch_active_exchange_markets(state)
        for coin in all_coins:
            symbol = str(coin.get("symbol", "")).upper()
            base = str(coin.get("base_coin", "")).upper()
            price = float(coin.get("price", 0.0) or 0.0)
            if price > 0:
                if symbol:
                    live_prices[symbol] = price
                if base:
                    live_by_base[base] = price
    except Exception:
        pass
    return live_prices, live_by_base

def _resolve_trade_live_price(state, trade, live_prices=None, live_by_base=None):
    sym = str(trade.get("symbol", "")).upper()
    clean = sym.replace("INR", "").replace("USDT", "").replace("/", "")

    current = 0.0
    if live_prices is not None:
        current = float(live_prices.get(sym, 0.0) or 0.0)
    if current <= 0 and live_by_base is not None:
        current = float(live_by_base.get(clean, 0.0) or 0.0)

    # Never fall back to Binance here. The active trade must be marked to market
    # using the same exchange that supplied its entry price.
    return current

def _refresh_active_trade_pnl(state):
    active = list(state.get("active_trades", []))
    if not active:
        return
    live_prices, live_by_base = _build_live_price_maps(state)
    for trade in active:
        current_price = _resolve_trade_live_price(state, trade, live_prices, live_by_base)
        if current_price > 0:
            _update_trade_unrealized_pnl(trade, current_price)

def _finalize_closed_trade(state, device_id, trade, exit_price, filled_qty,
                           broker, reason="MANUAL EXIT"):
    exit_p = float(exit_price or trade.get("entry_price", 0.0) or 0.0)
    qty = float(filled_qty or trade.get("quantity", 0.0) or 0.0)
    pnl_pct, pnl_val = _pnl_for_trade(trade, exit_p, qty)

    trade["quantity"] = qty
    trade["pnl_percent"] = pnl_pct
    trade["pnl_val"] = pnl_val
    trade["gross_pnl_percent"] = pnl_pct
    trade["gross_pnl_val"] = pnl_val
    trade["fees"] = float(trade.get("fees", 0.0) or 0.0)
    trade["net_pnl_val"] = round(pnl_val - trade["fees"], 2)
    trade["exit_price"] = round(exit_p, 6 if exit_p < 1 else 2)
    trade["close_time"] = get_global_time()
    trade["status"] = reason
    trade["current_price"] = trade["exit_price"]
    trade["current_pnl_percent"] = pnl_pct
    trade["current_pnl_val"] = pnl_val
    trade.pop("_closing", None)

    state["today_pnl"] = round(float(state.get("today_pnl", 0.0)) + float(trade.get("net_pnl_val", pnl_val)), 2)
    
    if broker == "paper":
        # Return the exact cash reserved when the paper position was opened,
        # then apply realized P&L. This avoids losing rounding dust on cheap coins.
        reserved_amt = float(
            trade.get("reserved_amount", trade.get("amount", 0.0)) or 0.0
        )
        state["paper_balance"] = round(
            float(state.get("paper_balance", 500000.0))
            + reserved_amt
            + float(trade.get("net_pnl_val", pnl_val)),
            2,
        )

    if trade in state["active_trades"]:
        state["active_trades"].remove(trade)

    db_delete_active_trade(trade.get("id"))
    db_save_trade(trade, device_id, broker)
    save_state_to_db(device_id, state)

    try:
        state.get("_closing_ids", set()).discard(str(trade.get("id")))
    except Exception:
        pass

    return trade

def _find_live_price(state, symbol, fallback=0.0):
    try:
        markets = fetch_active_exchange_markets(state)
        target = str(symbol).upper()
        clean = target.replace("INR", "").replace("USDT", "").replace("/", "")
        for m in markets:
            if str(m.get("symbol", "")).upper() == target:
                return float(m.get("price", fallback) or fallback)
            if str(m.get("base_coin", "")).upper() == clean:
                return float(m.get("price", fallback) or fallback)
    except Exception:
        pass
    return float(fallback or 0.0)

def _close_trade_at_market(state, device_id, trade, reason="MANUAL EXIT",
                           preferred_price=None):
    trade_key = str(trade.get("id"))
    closing_ids = state.setdefault("_closing_ids", set())
    if trade_key in closing_ids:
        return False, None, 0.0, "Trade is already being closed."
    closing_ids.add(trade_key)
    trade["_closing"] = True

    broker = state.get("active_broker", "coindcx").lower()
    side_to_exit = "sell" if trade.get("type") in ["LONG", "BUY"] else "buy"
    qty_requested = float(trade.get("quantity", 0.0) or 0.0)

    try:
        if broker == "paper":
            exit_price = float(preferred_price or _find_live_price(
                state, trade.get("symbol", ""), trade.get("entry_price", 0.0)
            ))
            if exit_price <= 0:
                exit_price = float(trade.get("entry_price", 0.0) or 0.0)
            filled_qty = qty_requested
            _finalize_closed_trade(
                state, device_id, trade, exit_price, filled_qty, broker, reason
            )
            return True, exit_price, filled_qty, "Paper exit confirmed"

        if broker == "coindcx":
            ok, exit_price, filled_qty, msg = execute_coindcx_order(
                state, trade.get("symbol"), side=side_to_exit, exact_qty=qty_requested
            )
        elif hasattr(ccxt, broker):
            ok, exit_price, filled_qty, msg = execute_ccxt_order(
                state, trade.get("symbol"), side=side_to_exit, exact_qty=qty_requested
            )
        else:
            return False, None, 0.0, f"Unsupported broker: {broker}"

        if not ok:
            trade.pop("_closing", None)
            closing_ids.discard(trade_key)
            return False, None, 0.0, str(msg)

        _finalize_closed_trade(
            state, device_id, trade, exit_price, filled_qty, broker, reason
        )
        return True, float(exit_price), float(filled_qty), str(msg)
    except Exception as exc:
        trade.pop("_closing", None)
        closing_ids.discard(trade_key)
        return False, None, 0.0, str(exc)

@app.post("/api/direct-sell")
async def direct_sell(request: Request):
    try:
        data = await request.json()
        device_id = data.get("device_id", "DEFAULT_DEVICE")
        state = get_user_session(device_id)

        coin = data.get("symbol", "").strip().upper()
        requested_qty = float(data.get("quantity", 0) or 0)

        if requested_qty <= 0:
            return {"status": "error", "message": "Sell quantity must be greater than 0."}

        api_key = data.get("api_key", state.get("api_key", "")).strip()
        secret_key = data.get("secret_key", state.get("secret_key", "")).strip()
        broker = state.get("active_broker", "coindcx").lower()

        if broker != "paper":
            if not api_key or not secret_key:
                return {"status": "error", "message": "API Keys missing! Reconnect in Portfolio."}
            state["api_key"] = api_key
            state["secret_key"] = secret_key

        matched = None
        for t in list(state["active_trades"]):
            t_coin = str(t.get("symbol", "")).replace("INR", "").replace("USDT", "").replace("/", "").upper()
            if t_coin == coin:
                t_qty = float(t.get("quantity", 0) or 0)
                if abs(t_qty - requested_qty) <= max(1e-8, t_qty * 0.01):
                    matched = t
                    break

        if matched:
            live_price = _find_live_price(state, matched.get("symbol"), matched.get("entry_price", 0.0))
            ok, exit_price, filled_qty, msg = _close_trade_at_market(
                state, device_id, matched, "MANUAL EXIT", live_price
            )
            if not ok:
                add_log(state, f"⚠️ MANUAL EXIT NOT CONFIRMED: {coin} | {msg}")
                return {"status": "error", "message": f"{broker.upper()} Exit Not Confirmed: {msg}"}

            add_log(
                state,
                f"✅ MANUAL EXIT CONFIRMED: {filled_qty} {coin} at "
                f"{get_curr_symbol(state)}{exit_price} on {broker.upper()}!"
            )
        else:
            if broker == "paper":
                exit_price = _find_live_price(state, coin, 0.0)
                if exit_price <= 0:
                    return {"status": "error", "message": "Live price unavailable for direct paper sell."}
                filled_qty = requested_qty
            elif broker == "coindcx":
                ok, exit_price, filled_qty, msg = execute_coindcx_order(
                    state, coin, side="sell", exact_qty=requested_qty
                )
                if not ok:
                    return {"status": "error", "message": f"CoinDCX Exit Not Confirmed: {msg}"}
            elif hasattr(ccxt, broker):
                ok, exit_price, filled_qty, msg = execute_ccxt_order(
                    state, coin, side="sell", exact_qty=requested_qty
                )
                if not ok:
                    return {"status": "error", "message": f"{broker.upper()} Exit Not Confirmed: {msg}"}
            else:
                return {"status": "error", "message": f"Unsupported broker: {broker}"}

            sold_trade = {
                "id": int(time.time() * 1000),
                "symbol": f"{coin}{state.get('quote_currency', 'INR')}",
                "currency": state.get("quote_currency", "INR"),
                "type": "SELL",
                "entry_price": float(exit_price),
                "exit_price": float(exit_price),
                "quantity": float(filled_qty),
                "amount": round(float(filled_qty) * float(exit_price), 2),
                "sl_price": 0.0,
                "target_price": 0.0,
                "pnl_percent": 0.0,
                "pnl_val": 0.0,
                "status": "DIRECT SELL",
                "close_time": get_global_time()
            }
            db_save_trade(sold_trade, device_id, broker)

        return {
            "status": "success",
            "message": "Sell confirmed and history synchronized.",
            "active": state["active_trades"],
            "history": db_get_all_trades(device_id, limit=500)
        }
    except Exception as e:
        return {"status": "error", "message": f"Server Error: {str(e)}"}

@app.post("/api/execute-order")
async def execute_order(request: Request):
    try:
        data = await request.json()
        device_id = data.get("device_id", "DEFAULT_DEVICE")
        state = get_user_session(device_id)

        exchange = data.get("exchange", state.get("active_broker", "coindcx")).lower()
        api_key = data.get("api_key", state.get("api_key", "")).strip()
        secret_key = data.get("secret_key", state.get("secret_key", "")).strip()
        
        symbol = data.get("symbol", "").upper().strip()
        if not symbol:
            symbol = "BTCINR" if state.get("quote_currency") == "INR" else "BTCUSDT"

        currency = data.get("currency", state.get("quote_currency", "INR")).upper()
        amount = float(data.get("amount", state.get("trade_amount", 500)))
        side = data.get("side", "BUY").lower()
        mode = data.get("mode", state.get("market_mode", "spot")).lower()

        if mode == "spot" and side == "sell":
            return {"status": "error", "message": "Spot mode only supports BUY/LONG. Switch to Futures for SHORT/SELL."}

        sl_pct = float(data.get("sl_percent", state.get("sl_percent", 1.5))) / 100.0
        target_pct = float(data.get("target_percent", state.get("target_percent", 2.5))) / 100.0

        state["active_broker"] = exchange
        state["quote_currency"] = currency
        state["market_mode"] = mode
        if api_key: state["api_key"] = api_key
        if secret_key: state["secret_key"] = secret_key

        curr_sym = get_curr_symbol(state)

        if exchange == "paper":
            markets = fetch_active_exchange_markets(state)
            clean_coin = symbol.replace("INR", "").replace("USDT", "")
            match = next((m for m in markets if clean_coin in m["symbol"]), None)
            if not match:
                return {"status": "error", "message": f"{symbol} is not available on {state.get('market_data_exchange','coindcx').upper()}; no fallback price is allowed."}
            sim_price = float(match["price"])
            # Paper execution uses the exact order amount. Fractional quantity is
            # required for low-priced/high-priced assets so P&L always matches the
            # amount actually simulated.
            calc_qty = round(amount / sim_price, 12) if sim_price > 0 else 0.0
            if calc_qty <= 0:
                return {"status": "error", "message": "Order size is too small for the selected market price."}

            if state.get("paper_balance", 500000.0) < amount:
                return {"status": "error", "message": "Insufficient Paper Trading Balance!"}
            state["paper_balance"] = round(state["paper_balance"] - amount, 2)

            trade_type = "LONG" if side == "buy" else "SHORT"
            if trade_type == "SHORT":
                target_price = sim_price * (1.0 - target_pct)
                sl_price = sim_price * (1.0 + sl_pct)
            else:
                target_price = sim_price * (1.0 + target_pct)
                sl_price = sim_price * (1.0 - sl_pct)

            new_trade = {
                "id": int(time.time() * 1000),
                "symbol": symbol,
                "currency": currency,
                "exchange": state.get("market_data_exchange", "coindcx"),
                "type": trade_type,
                "entry_price": sim_price,
                "quantity": calc_qty,
                "amount": round(sim_price * calc_qty, 2),
                "reserved_amount": round(amount, 2),
                "highest_price": sim_price,
                "lowest_price": sim_price,
                "sl_price": sl_price,
                "target_price": target_price,
                "current_price": sim_price,
                "current_pnl_percent": 0.0,
                "current_pnl_val": 0.0,
                "time": get_global_time()
            }
            state["active_trades"].insert(0, new_trade)
            db_save_active_trade(new_trade, device_id, "paper")
            save_state_to_db(device_id, state)
            add_log(state, f"⚡ [PAPER] {trade_type}: {calc_qty} {symbol} at {curr_sym}{sim_price}")
            return {"status": "success", "message": f"Paper {trade_type} order placed!", "price": sim_price, "qty": calc_qty}

        else:
            success, price, qty, res = False, 0.0, 0.0, "No execution adapter selected"
            if exchange == "coindcx":
                success, price, qty, res = execute_coindcx_order(state, symbol, side=side, target_amount=amount)
            elif hasattr(ccxt, exchange):
                success, price, qty, res = execute_ccxt_order(state, symbol, side=side, target_amount=amount)
            else:
                return {"status": "error", "message": f"Exchange '{exchange.upper()}' has no execution adapter. No simulated order was created."}

            if success:
                trade_type = "LONG" if side == "buy" else "SHORT"
                if trade_type == "SHORT":
                    target_price = price * (1.0 - target_pct)
                    sl_price = price * (1.0 + sl_pct)
                else:
                    target_price = price * (1.0 + target_pct)
                    sl_price = price * (1.0 - sl_pct)

                new_trade = {
                    "id": int(time.time() * 1000),
                    "symbol": symbol,
                    "currency": currency,
                    "exchange": exchange,
                    "type": trade_type,
                    "entry_price": price,
                    "quantity": qty,
                    "amount": round(qty * price, 2),
                    "highest_price": price,
                    "lowest_price": price,
                    "sl_price": sl_price,
                    "target_price": target_price,
                    "current_price": price,
                    "current_pnl_percent": 0.0,
                    "current_pnl_val": 0.0,
                    "unrealized_pnl": 0.0,
                    "unrealized_pnl_percent": 0.0,
                    "time": get_global_time(),
                }
                state["active_trades"].insert(0, new_trade)
                db_save_active_trade(new_trade, device_id, exchange)
                save_state_to_db(device_id, state)
                add_log(state, f"✅ REAL {trade_type} ORDER FILLED: {qty} {symbol} at {curr_sym}{price} on {exchange.upper()}")
                return {"status": "success", "message": f"Real {trade_type} order filled on {exchange.upper()}!", "price": price, "qty": qty}
            else:
                add_log(state, f"❌ {exchange.upper()} Rejected: {res}")
                return {"status": "error", "message": str(res)}

    except Exception as e:
        return {"status": "error", "message": f"Execution Error: {str(e)}"}

@app.post("/api/set-market-mode")
async def set_market_mode(request: Request):
    data = await request.json()
    device_id = data.get("device_id", "")
    state = get_user_session(device_id)
    mode = data.get("mode", "spot").lower()
    if mode in ["spot", "futures"]:
        state["market_mode"] = mode
        save_state_to_db(device_id, state)
        add_log(state, f"🎯 Market Mode Changed to: {mode.upper()}")
        return {"status": "success", "market_mode": mode}
    return {"status": "error", "message": "Mode must be 'spot' or 'futures'"}

@app.post("/api/set-broker-mode")
async def set_broker_mode(request: Request):
    data = await request.json()
    device_id = data.get("device_id", "")
    state = get_user_session(device_id)
    mode = data.get("mode", "real").lower()
    if mode == "paper":
        if state.get("active_broker") != "paper":
            state["market_data_exchange"] = state.get("active_broker", "coindcx")
        state["active_broker"] = "paper"
    elif mode == "real":
        state["active_broker"] = state.get("market_data_exchange", "coindcx")
    save_state_to_db(device_id, state)
    add_log(state, f"⚡ Broker Mode: {mode.upper()} | Exchange: {state['active_broker'].upper()}")
    return {"status": "success", "active_broker": state["active_broker"]}

@app.post("/api/set-currency")
async def set_currency(request: Request):
    data = await request.json()
    device_id = data.get("device_id", "")
    state = get_user_session(device_id)
    currency = data.get("currency", "INR").upper()
    if currency not in ["USDT", "INR"]:
        return {"status": "error", "message": "Only 'USDT' and 'INR' are supported"}

    state["quote_currency"] = currency
    state["trade_amount"] = 500.0 if currency == "INR" else 5.0
    save_state_to_db(device_id, state)
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
    device_id = data.get("device_id", "")
    state = get_user_session(device_id)
    exchange_id = str(data.get("exchange", "coindcx") or "coindcx").lower().strip()
    api_key = str(data.get("api_key", "") or "").strip()
    secret_key = str(data.get("secret_key", "") or "").strip()

    if exchange_id == "paper":
        state["active_broker"] = "paper"
        save_state_to_db(device_id, state)
        return {"status": "success", "message": "🟢 Paper Trading Synced!", "balances": {state["quote_currency"]: fetch_real_cash_balance(state)}}

    if exchange_id not in SUPPORTED_REAL_BROKERS:
        return {"status": "error", "message": f"Exchange '{exchange_id.upper()}' is not in the app's supported broker registry."}
    # Mudrex's current API authenticates with the API secret in X-Authentication.
    # The UI may still send the generated API key; it is retained for account
    # metadata, but the secret is the credential actually used on the wire.
    if exchange_id == "mudrex":
        if not secret_key:
            return {"status": "error", "message": "MUDREX requires the API Secret for X-Authentication."}
    elif not api_key or not secret_key:
        return {"status": "error", "message": f"{exchange_id.upper()} requires both API Key and Secret Key."}

    try:
        if exchange_id == "coindcx":
            time_stamp = int(time.time() * 1000)
            body = {"timestamp": time_stamp}
            json_body = json.dumps(body, separators=(',', ':'))
            signature = hmac.new(secret_key.encode('utf-8'), json_body.encode('utf-8'), hashlib.sha256).hexdigest()
            headers = {'Content-Type': 'application/json', 'X-AUTH-APIKEY': api_key, 'X-AUTH-SIGNATURE': signature}
            res = requests.post("https://api.coindcx.com/exchange/v1/users/balances", data=json_body, headers=headers, timeout=8)
            res.raise_for_status()
            res_data = res.json()
            if not isinstance(res_data, list):
                raise RuntimeError("CoinDCX returned an unexpected balance response.")
            dynamic_balances = {}
            for item in res_data:
                curr = str(item.get("currency", "") or "").upper()
                total = float(item.get("balance", 0) or 0) + float(item.get("lock", 0) or 0)
                if curr and total > 1e-12:
                    dynamic_balances[curr] = total

        elif exchange_id == "wazirx":
            # WazirX uses its own signed SAPI. Do not depend on CCXT here.
            dynamic_balances = _wazirx_balances({**state, "api_key": api_key, "secret_key": secret_key})

        elif exchange_id == "coinswitch":
            dynamic_balances = _coinswitch_balances(api_key, secret_key)

        elif exchange_id == "mudrex":
            dynamic_balances = _mudrex_balances(secret_key)

        elif exchange_id == "delta":
            dynamic_balances = _delta_balances(api_key, secret_key)

        else:
            dynamic_balances = _connect_ccxt_broker(exchange_id, api_key, secret_key, state)

        # Only persist credentials after a real authenticated balance request succeeds.
        state["market_data_exchange"] = exchange_id
        state["active_broker"] = exchange_id
        state["api_key"] = api_key
        state["secret_key"] = secret_key
        quote = state.get("quote_currency", "INR").upper()
        state["session_start_fund"] = float(dynamic_balances.get(quote, 0.0))
        save_state_to_db(device_id, state)
        add_log(state, f"🔗 Connected to {exchange_id.upper()}! Live Cash: {get_curr_symbol(state)}{state['session_start_fund']}")
        return {
            "status": "success",
            "message": f"Connected to {exchange_id.upper()}!",
            "balances": dynamic_balances,
            "exchange": exchange_id,
            "authenticated": True,
        }
    except Exception as e:
        # Failed authentication must not be presented as a successful connection.
        return {"status": "error", "message": f"{exchange_id.upper()} connection failed: {str(e)[:500]}"}

@app.get("/api/broker-capabilities")
async def broker_capabilities():
    """Expose the real adapter status so the UI never implies unsupported auth."""
    rows = []
    for broker in sorted(SUPPORTED_REAL_BROKERS):
        mode = BROKER_AUTH_MODES.get(broker, "ccxt")
        rows.append({
            "broker": broker,
            "auth_mode": mode,
            "connection": "dedicated" if mode != "ccxt" else "ccxt",
            "real_auth_required": True,
            "fake_success": False,
        })
    return {"status": "success", "brokers": rows, "count": len(rows)}

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
            return {
                "status": "error",
                "message": f"Bot is in 12-Hour Cooldown Mode ({state['sleep_reason']}). {hrs_left}h remaining!"
            }

        state["is_running"] = True
        if "currency" in data:
            state["quote_currency"] = data["currency"].upper()
        if "amount" in data:
            state["trade_amount"] = float(data["amount"])
        requested_exchange = str(data.get("exchange", state.get("market_data_exchange", "coindcx"))).lower()
        broker_mode = str(data.get("broker_mode", "real")).lower()
        if requested_exchange != "paper":
            state["market_data_exchange"] = requested_exchange
        if broker_mode == "paper":
            state["active_broker"] = "paper"
        else:
            state["active_broker"] = requested_exchange
        if "deal_condition" in data:
            state["deal_condition"] = str(data["deal_condition"])
        if "target_coin" in data:
            state["selected_coin"] = data["target_coin"].upper()
        if "max_trades" in data:
            state["max_trades"] = max(1, int(data["max_trades"]))
        if "target_percent" in data:
            state["target_percent"] = max(0.1, float(data["target_percent"]))
        if "sl_percent" in data:
            state["sl_percent"] = max(0.1, float(data["sl_percent"]))

        save_state_to_db(device_id, state)

        curr_sym = get_curr_symbol(state)
        target_info = (
            state["selected_coin"]
            if state["selected_coin"] != "AUTO"
            else "Nifty-Style Dynamic Top 100 Index Scanner"
        )
        add_log(
            state,
            f"🚀 BOT STARTED | Target: {target_info} | Slots: {state['max_trades']} | "
            f"Broker: {state['active_broker'].upper()} | Data: {get_market_data_exchange(state).upper()} | Lot: {curr_sym}{state['trade_amount']} | "
            f"Target: +{state['target_percent']}% | SL: -{state['sl_percent']}%"
        )
        return {
            "status": "success",
            "message": "Bot Started!",
            "is_running": True,
            "target_percent": state["target_percent"],
            "sl_percent": state["sl_percent"],
            "active_broker": state["active_broker"]
        }

    if action == "stop":
        state["is_running"] = False
        save_state_to_db(device_id, state)
        add_log(state, "🛑 BOT STOPPED! New deal scanning halted; existing target/SL monitoring remains active.")
        return {"status": "success", "message": "Bot Stopped!", "is_running": False}

    return {"status": "error", "message": "Unknown bot action."}

@app.get("/api/market-index")
def get_market_index(device_id: str = "DEFAULT_DEVICE"):
    """Return the selected exchange's authoritative Top-100 volume index."""
    state = get_user_session(device_id)
    try:
        markets = fetch_active_exchange_markets(state)
        clean = []
        seen = set()
        for m in markets:
            base = str(m.get("base_coin", "") or "").upper().strip()
            price = float(m.get("price", 0) or 0)
            if not base or price <= 0 or base in seen:
                continue
            seen.add(base)
            clean.append({
                "rank": len(clean) + 1,
                "symbol": base,
                "market_symbol": str(m.get("raw_symbol") or m.get("symbol") or ""),
                "price": price,
                "change": float(m.get("change", 0) or 0),
                "volume": float(m.get("volume", 0) or 0),
                "exchange": str(m.get("exchange") or get_market_data_exchange(state)),
            })
            if len(clean) >= 100:
                break
        return {
            "status": "success",
            "exchange": get_market_data_exchange(state),
            "quote_currency": str(state.get("quote_currency", "INR")).upper(),
            "count": len(clean),
            "coins": clean,
        }
    except Exception as exc:
        return {
            "status": "error",
            "message": f"Market index unavailable: {exc}",
            "exchange": get_market_data_exchange(state),
            "coins": [],
        }


@app.get("/api/chart-data")
def get_chart_data(device_id: str = "DEFAULT_DEVICE", symbol: str = "BTC", timeframe: str = "15m"):
    state = get_user_session(device_id)
    timeframe = str(timeframe or "15m").lower()
    if timeframe not in {"1m", "5m", "15m", "30m", "1h", "4h", "1d"}:
        return {"status": "error", "message": "Unsupported timeframe. Use 1m, 5m, 15m, 30m, 1h, 4h or 1d."}
    clean = str(symbol or "BTC").upper().replace("INR", "").replace("USDT", "").replace("/", "")
    if not clean:
        clean = "BTC"
    markets = fetch_active_exchange_markets(state)
    coin = next((m for m in markets if str(m.get("base_coin", "")).upper() == clean), None)
    if coin is None:
        return {"status": "error", "message": f"{clean} is not available on {get_market_data_exchange(state).upper()} in {state.get('quote_currency','INR')}.", "exchange": get_market_data_exchange(state)}
    candles = _fetch_analysis_ohlcv(clean, state, coin.get("raw_symbol"), timeframe, 300)
    if not candles:
        return {"status": "error", "message": f"No candle data from {get_market_data_exchange(state).upper()} for {clean}.", "exchange": get_market_data_exchange(state)}
    return {
        "status": "success",
        "exchange": get_market_data_exchange(state),
        "market_symbol": str(coin.get("raw_symbol") or coin.get("symbol") or f"{clean}/{state.get('quote_currency','INR').upper()}"),
        "symbol": f"{clean}/{state.get('quote_currency','INR').upper()}",
        "timeframe": timeframe,
        "live_price": float(coin.get("price", 0) or 0),
        "candles": candles,
    }


@app.get("/api/bot-status")
def get_bot_status(device_id: str = "DEFAULT_DEVICE"):
    state = get_user_session(device_id)
    now_ts = time.time()
    sleeping = bool(state.get("sleep_until") and now_ts < state["sleep_until"])
    return {
        "status": "success",
        "is_running": bool(state.get("is_running")),
        "is_sleeping": sleeping,
        "sleep_until": state.get("sleep_until"),
        "sleep_reason": state.get("sleep_reason", ""),
        "active_broker": state.get("active_broker"),
        "market_data_exchange": get_market_data_exchange(state),
        "quote_currency": state.get("quote_currency"),
        "trade_amount": state.get("trade_amount"),
        "max_trades": state.get("max_trades", 1),
        "deal_condition": state.get("deal_condition", "ASAP"),
        "selected_coin": state.get("selected_coin", "AUTO"),
        "target_percent": state.get("target_percent", 2.5),
        "sl_percent": state.get("sl_percent", 1.5),
    }

@app.post("/api/close-trade")
async def close_trade(request: Request):
    data = await request.json()
    device_id = data.get("device_id", "DEFAULT_DEVICE")
    state = get_user_session(device_id)
    trade_id = data.get("id")

    trade = next(
        (t for t in state["active_trades"] if str(t.get("id")) == str(trade_id)),
        None
    )
    if not trade:
        return await direct_sell(request)

    try:
        broker = state.get("active_broker", "coindcx").lower()
        live_price = _find_live_price(state, trade.get("symbol"), trade.get("entry_price", 0.0))
        ok, exit_price, filled_qty, msg = _close_trade_at_market(
            state, device_id, trade, "MANUAL EXIT", live_price
        )

        if not ok:
            add_log(state, f"⚠️ MANUAL EXIT NOT CONFIRMED: {trade.get('symbol')} | {msg}")
            return {
                "status": "error",
                "message": f"{broker.upper()} Exit Not Confirmed: {msg}",
                "active": state["active_trades"]
            }

        return {
            "status": "success",
            "message": f"Exit Confirmed! PNL: {trade.get('pnl_percent', 0)}%",
            "active": state["active_trades"],
            "history": db_get_all_trades(device_id, limit=500)
        }
    except Exception as e:
        add_log(state, f"❌ CLOSE TRADE ERROR: {str(e)}")
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
        "trade_amount": state["trade_amount"],
        "max_trades": state.get("max_trades", 1),
        "deal_condition": state.get("deal_condition", "ASAP"),
        "selected_coin": state.get("selected_coin", "AUTO"),
        "target_percent": state.get("target_percent", 2.5),
        "sl_percent": state.get("sl_percent", 1.5),
        "scanner_cycle": int(state.get("scanner_cycle", 0) or 0),
        "scanner_updated_at": state.get("scanner_updated_at"),
        "scanner_reports": list(state.get("scanner_reports", []))
    }

@app.get("/api/get-trades")
def get_trades(device_id: str = "DEFAULT_DEVICE"):
    state = get_user_session(device_id)
    check_midnight_settlement(state)

    if state.get("active_trades"):
        _refresh_active_trade_pnl(state)

    history_records = db_get_all_trades(device_id, limit=500)
    # P&L is currency-specific. Never add INR and USDT/USD amounts together.
    current_currency = str(state.get("quote_currency", "INR")).upper()
    display_today_pnl = round(sum(
        float(t.get("net_pnl_val", t.get("pnl_val", 0.0)) or 0.0)
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
        "deal_condition": state.get("deal_condition", "ASAP"),
        "selected_coin": state.get("selected_coin", "AUTO"),
        "target_percent": state.get("target_percent", 2.5),
        "sl_percent": state.get("sl_percent", 1.5),
        "active_broker": state.get("active_broker", "coindcx"),
        "market_data_exchange": get_market_data_exchange(state)
    }

def _get_ai_sentiment_scores():
    return {
        "BTC": 84,
        "ETH": 79,
        "SOL": 91,
        "XRP": 52,
    }

def _select_top20_ai_news(valid_coins):
    top20 = list(valid_coins[:20])
    sentiment = _get_ai_sentiment_scores()

    if not top20:
        return None

    max_volume = max(float(c.get("volume", 0.0) or 0.0) for c in top20) or 1.0
    max_change = max(abs(float(c.get("change", 0.0) or 0.0)) for c in top20) or 1.0

    scored = []
    for idx, coin in enumerate(top20):
        base_volume = float(coin.get("volume", 0.0) or 0.0) / max_volume
        change = float(coin.get("change", 0.0) or 0.0)
        momentum = max(0.0, min(1.0, (change + max_change) / (2.0 * max_change)))
        news_score = float(sentiment.get(str(coin.get("base_coin", "")).upper(), 50)) / 100.0

        score = (0.60 * base_volume) + (0.25 * momentum) + (0.15 * news_score)
        scored.append((score, coin))

    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[0][1]


# -----------------------------------------------------------------------------
# REAL MARKET-ANALYSIS ENGINE
# -----------------------------------------------------------------------------
def _ema(values, period):
    vals = [float(v) for v in values if v is not None]
    if len(vals) < period:
        return None
    k = 2.0 / (period + 1.0)
    e = sum(vals[:period]) / period
    for v in vals[period:]:
        e = (v * k) + (e * (1.0 - k))
    return e


def _rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    gains = []
    losses = []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _macd(closes):
    if len(closes) < 35:
        return None, None, None
    macd_series = []
    for i in range(26, len(closes)):
        fast = _ema(closes[:i+1], 12)
        slow = _ema(closes[:i+1], 26)
        if fast is not None and slow is not None:
            macd_series.append(fast - slow)
    if len(macd_series) < 9:
        return None, None, None
    signal = _ema(macd_series, 9)
    if signal is None:
        return None, None, None
    return macd_series[-1], signal, macd_series[-2] if len(macd_series) > 1 else macd_series[-1]


def _bollinger(closes, period=20, mult=2.0):
    if len(closes) < period:
        return None, None, None
    w = closes[-period:]
    mid = sum(w) / period
    var = sum((x - mid) ** 2 for x in w) / period
    sd = var ** 0.5
    return mid, mid + mult * sd, mid - mult * sd


def _stochastic(highs, lows, closes, period=14, smooth=3):
    if len(closes) < period + smooth:
        return None, None
    ks = []
    for i in range(period - 1, len(closes)):
        hi = max(highs[i-period+1:i+1])
        lo = min(lows[i-period+1:i+1])
        if hi == lo:
            ks.append(50.0)
        else:
            ks.append(((closes[i] - lo) / (hi - lo)) * 100.0)
    k = ks[-1]
    d = sum(ks[-smooth:]) / smooth
    return k, d


def _supertrend_signal(highs, lows, closes, period=10, multiplier=3.0):
    if len(closes) < period + 3:
        return None
    trs = []
    for i in range(len(closes)):
        if i == 0:
            trs.append(highs[i] - lows[i])
        else:
            trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))
    atr = sum(trs[:period]) / period
    upper = lower = None
    trend = 1
    for i in range(period, len(closes)):
        atr = ((atr * (period - 1)) + trs[i]) / period
        hl2 = (highs[i] + lows[i]) / 2.0
        basic_upper = hl2 + multiplier * atr
        basic_lower = hl2 - multiplier * atr
        if upper is None:
            upper, lower = basic_upper, basic_lower
        else:
            upper = basic_upper if basic_upper < upper or closes[i-1] > upper else upper
            lower = basic_lower if basic_lower > lower or closes[i-1] < lower else lower
        if closes[i] > upper:
            trend = 1
        elif closes[i] < lower:
            trend = -1
    return trend


def _exchange_public_client(state):
    broker = get_market_data_exchange(state)
    if broker == "coindcx":
        return None, broker
    if hasattr(ccxt, broker):
        exchange_class = getattr(ccxt, broker)
        return exchange_class({'enableRateLimit': True, 'timeout': 6000}), broker
    return None, broker


def _fetch_analysis_ohlcv(base_coin, state, raw_symbol=None, timeframe="15m", limit=220):
    coin = str(base_coin or "").upper().strip()
    broker = get_market_data_exchange(state)
    quote = state.get("quote_currency", "INR").upper()
    if not coin:
        return []

    if broker == "wazirx":
        symbol = str(raw_symbol or f"{coin}{quote}").replace("/", "").replace("_", "").replace("-", "").lower()
        try:
            r = requests.get("https://api.wazirx.com/sapi/v1/klines", params={"symbol": symbol, "interval": timeframe, "limit": min(int(limit), 2000)}, timeout=8)
            r.raise_for_status()
            rows = r.json()
            out = []
            for x in rows:
                ts = int(float(x[0] or 0))
                if ts < 10**12:
                    ts *= 1000
                out.append({"time": ts, "open": float(x[1]), "high": float(x[2]), "low": float(x[3]), "close": float(x[4]), "volume": float(x[5] or 0)})
            return out
        except Exception:
            return []

    if broker == "coindcx":
        pair = raw_symbol or f"B-{coin}_{quote}"
        if not str(pair).startswith(("B-", "I-")):
            pair = f"B-{coin}_{quote}"
        try:
            r = requests.get(
                "https://api.coindcx.com/market_data/candles",
                params={"pair": pair, "interval": timeframe, "limit": min(int(limit), 500)},
                timeout=6,
            )
            r.raise_for_status()
            data = r.json()
            out = []
            for x in data:
                out.append({"time": int(float(x.get("time", 0) or 0)), "open": float(x["open"]), "high": float(x["high"]),
                            "low": float(x["low"]), "close": float(x["close"]),
                            "volume": float(x.get("volume", 0) or 0)})
            return list(reversed(out))
        except Exception:
            return []

    inst, broker = _exchange_public_client(state)
    if inst is None:
        return []
    symbol_pair = raw_symbol if raw_symbol and "/" in str(raw_symbol) else f"{coin}/{quote}"
    try:
        rows = inst.fetch_ohlcv(symbol_pair, timeframe=timeframe, limit=min(int(limit), 500))
        return [{"time": int(x[0]), "open": float(x[1]), "high": float(x[2]), "low": float(x[3]),
                 "close": float(x[4]), "volume": float(x[5] or 0)} for x in rows]
    except Exception:
        return []

def _fetch_orderbook_imbalance(base_coin, state, raw_symbol=None):
    coin = str(base_coin or "").upper().strip()
    broker = get_market_data_exchange(state)
    quote = state.get("quote_currency", "INR").upper()
    if not coin:
        return 0.0

    if broker == "coindcx":
        pair = raw_symbol or f"B-{coin}_{quote}"
        if not str(pair).startswith(("B-", "I-")):
            pair = f"B-{coin}_{quote}"
        try:
            r = requests.get("https://api.coindcx.com/market_data/orderbook",
                             params={"pair": pair, "depth": 20}, timeout=5)
            r.raise_for_status()
            d = r.json()
            bids = sum(float(v) for v in d.get("bids", {}).values())
            asks = sum(float(v) for v in d.get("asks", {}).values())
            total = bids + asks
            return ((bids - asks) / total) if total else 0.0
        except Exception:
            return 0.0

    inst, _ = _exchange_public_client(state)
    if inst is None:
        return 0.0
    symbol_pair = raw_symbol if raw_symbol and "/" in str(raw_symbol) else f"{coin}/{quote}"
    try:
        d = inst.fetch_order_book(symbol_pair, limit=20)
        bids = sum(float(x[1]) for x in d.get("bids", []))
        asks = sum(float(x[1]) for x in d.get("asks", []))
        total = bids + asks
        return ((bids - asks) / total) if total else 0.0
    except Exception:
        return 0.0

def _analyze_coin(coin, state):
    """Return a strict, explainable technical-analysis signal for one coin."""
    base = str(coin.get("base_coin", "")).upper()
    broker = get_market_data_exchange(state)
    quote = state.get("quote_currency", "INR").upper()
    raw_symbol = coin.get("raw_symbol")
    cache_key = f"{broker}|{quote}|{base}|{raw_symbol}"
    cached = _analysis_cache.get(cache_key)
    if cached and (time.time() - cached.get("ts", 0)) < 45:
        return cached.get("analysis")

    if broker == "tradingview":
        tv=coin.get("tv") or {}
        def _num(k):
            try:
                v=float(tv.get(k)); return v if math.isfinite(v) else None
            except Exception: return None
        price=float(coin.get("price",0) or 0); rsi=_num("rsi15"); macd=_num("macd"); macd_signal=_num("macd_signal")
        ema50=_num("ema50"); ema200=_num("ema200"); h1_ema50=_num("ema50_60"); vr=_num("volume_ratio") or 0.0
        k=_num("stoch_k"); d=_num("stoch_d")
        if price<=0 or rsi is None or macd is None or macd_signal is None or ema50 is None: return None
        bull=bear=0; reasons=[]
        if 50<=rsi<=68 or rsi<30: bull+=2; reasons.append(f"RSI {rsi:.1f} bullish/reversal zone")
        elif rsi>72: bear+=2; reasons.append(f"RSI {rsi:.1f} overbought")
        if macd>macd_signal: bull+=2; reasons.append("MACD bullish")
        else: bear+=2; reasons.append("MACD bearish")
        if ema200 is not None:
            if price>ema50>ema200: bull+=2; reasons.append("EMA trend bullish")
            elif price<ema50<ema200: bear+=2; reasons.append("EMA trend bearish")
        h1_bull=bool(h1_ema50 is not None and price>h1_ema50)
        if h1_bull: bull+=2; reasons.append("1H trend bullish")
        else: bear+=2; reasons.append("1H trend bearish")
        if vr>=1.5:
            if float(coin.get("change",0) or 0)>0: bull+=2; reasons.append(f"Volume spike {vr:.1f}x")
            elif float(coin.get("change",0) or 0)<0: bear+=2; reasons.append(f"Selling volume {vr:.1f}x")
        if k is not None and d is not None:
            if k>d and k<80: bull+=1
            elif k<d and k>20: bear+=1
        score=max(0,min(100,round(max(bull,bear)/11*100)))
        direction="LONG" if bull>bear else "SHORT" if bear>bull else "NEUTRAL"
        analysis={"base_coin":base,"price":price,"rsi":rsi,"macd":macd,"macd_signal":macd_signal,"macd_prev":None,"ema50":ema50,"ema200":ema200,"volume_ratio":vr,"stoch_k":k,"stoch_d":d,"supertrend":1 if h1_bull else -1,"orderbook":0.0,"bb_width":0.0,"squeeze_threshold":0.0,"bullish_points":bull,"bearish_points":bear,"score":score,"direction":direction,"reasons":reasons[-6:]}
        _analysis_cache[cache_key]={"ts":time.time(),"analysis":analysis}
        return analysis

    c15 = _fetch_analysis_ohlcv(base, state, raw_symbol, "15m", 220)
    c1h = _fetch_analysis_ohlcv(base, state, raw_symbol, "1h", 220)
    if len(c15) < 80 or len(c1h) < 80:
        return None

    closes = [x["close"] for x in c15]
    highs = [x["high"] for x in c15]
    lows = [x["low"] for x in c15]
    vols = [x["volume"] for x in c15]
    hcloses = [x["close"] for x in c1h]

    price = closes[-1]
    rsi = _rsi(closes, 14)
    macd, macd_signal, macd_prev = _macd(closes)
    ema20 = _ema(closes, 20)
    ema50 = _ema(closes, 50)
    ema200 = _ema(closes, 200)
    h1_ema50 = _ema(hcloses, 50)
    mid, bb_upper, bb_lower = _bollinger(closes, 20, 2.0)
    bb_width = ((bb_upper - bb_lower) / mid) if mid else 0.0
    bb_widths = []
    for j in range(max(20, len(closes) - 80), len(closes) + 1):
        if j <= len(closes):
            m0, u0, l0 = _bollinger(closes[:j], 20, 2.0)
            if m0:
                bb_widths.append((u0 - l0) / m0)
    squeeze_threshold = sorted(bb_widths)[max(0, int(len(bb_widths) * 0.20) - 1)] if bb_widths else 0.0
    stoch_k, stoch_d = _stochastic(highs, lows, closes, 14, 3)
    supertrend = _supertrend_signal(highs, lows, closes, 10, 3.0)
    orderbook = _fetch_orderbook_imbalance(base, state, raw_symbol)

    avg_vol = sum(vols[-21:-1]) / max(1, len(vols[-21:-1]))
    volume_ratio = vols[-1] / avg_vol if avg_vol else 0.0
    change_15 = ((closes[-1] - closes[-5]) / closes[-5]) * 100 if len(closes) >= 5 and closes[-5] else 0.0
    h1_bull = bool(h1_ema50 and hcloses[-1] > h1_ema50)

    bullish_points = 0
    bearish_points = 0
    reasons = []

    if rsi is not None:
        if 50 <= rsi <= 68:
            bullish_points += 2; reasons.append(f"RSI {rsi:.1f} healthy bullish")
        elif rsi < 30:
            bullish_points += 2; reasons.append(f"RSI {rsi:.1f} oversold reversal zone")
        elif rsi > 72:
            bearish_points += 2; reasons.append(f"RSI {rsi:.1f} overbought")

    if macd is not None and macd_signal is not None:
        if macd > macd_signal:
            bullish_points += 2; reasons.append("MACD bullish")
        else:
            bearish_points += 2; reasons.append("MACD bearish")

    if ema50 and ema200:
        if price > ema50 > ema200:
            bullish_points += 2; reasons.append("EMA trend bullish")
        elif price < ema50 < ema200:
            bearish_points += 2; reasons.append("EMA trend bearish")

    if h1_bull:
        bullish_points += 2; reasons.append("1H trend bullish")
    else:
        bearish_points += 2; reasons.append("1H trend bearish")

    if mid and bb_upper and bb_lower:
        if price > mid:
            bullish_points += 1
        else:
            bearish_points += 1

    if volume_ratio >= 1.5:
        if change_15 > 0:
            bullish_points += 2; reasons.append(f"Volume spike {volume_ratio:.1f}x")
        elif change_15 < 0:
            bearish_points += 2; reasons.append(f"Selling volume {volume_ratio:.1f}x")

    if supertrend == 1:
        bullish_points += 2; reasons.append("Supertrend bullish")
    elif supertrend == -1:
        bearish_points += 2; reasons.append("Supertrend bearish")

    if stoch_k is not None and stoch_d is not None:
        if stoch_k > stoch_d and stoch_k < 80:
            bullish_points += 1
        elif stoch_k < stoch_d and stoch_k > 20:
            bearish_points += 1

    if orderbook > 0.12:
        bullish_points += 1; reasons.append("Bid-side orderbook pressure")
    elif orderbook < -0.12:
        bearish_points += 1; reasons.append("Ask-side orderbook pressure")

    max_points = 15
    score = max(0, min(100, round((max(bullish_points, bearish_points) / max_points) * 100)))
    direction = "LONG" if bullish_points > bearish_points else "SHORT" if bearish_points > bullish_points else "NEUTRAL"

    analysis = {
        "base_coin": base, "price": price, "rsi": rsi, "macd": macd,
        "macd_signal": macd_signal, "macd_prev": macd_prev, "ema50": ema50, "ema200": ema200,
        "volume_ratio": volume_ratio, "stoch_k": stoch_k, "stoch_d": stoch_d,
        "supertrend": supertrend, "orderbook": orderbook,
        "bb_width": bb_width, "squeeze_threshold": squeeze_threshold,
        "bullish_points": bullish_points, "bearish_points": bearish_points,
        "score": score, "direction": direction, "reasons": reasons[-6:]
    }
    _analysis_cache[cache_key] = {"ts": time.time(), "analysis": analysis}
    return analysis


def _signal_passes(analysis, condition, market_mode):
    if not analysis:
        return False
    direction = analysis["direction"]
    # Spot only opens long positions. Futures can open either direction.
    if market_mode == "spot" and direction != "LONG":
        return False
    if analysis["score"] < 65:
        return False

    rsi = analysis.get("rsi")
    macd = analysis.get("macd")
    macd_signal = analysis.get("macd_signal")
    macd_prev = analysis.get("macd_prev")
    ema50 = analysis.get("ema50")
    ema200 = analysis.get("ema200")
    vr = analysis.get("volume_ratio", 0)
    st = analysis.get("supertrend")
    k = analysis.get("stoch_k")
    d = analysis.get("stoch_d")
    ob = analysis.get("orderbook", 0)

    if condition == "RSI_DIP":
        return bool(rsi is not None and rsi < 35 and direction == "LONG")
    if condition == "MACD_CROSS":
        # Strict bullish crossover: MACD is above signal now and was not above
        # signal on the previous candle. The current analyzer already confirms
        # broader trend/momentum before this condition is accepted.
        return bool(macd is not None and macd_signal is not None and macd_prev is not None and macd > macd_signal and macd_prev <= macd_signal and direction == "LONG")
    if condition == "BB_SQUEEZE":
        width = float(analysis.get("bb_width", 0.0) or 0.0)
        squeeze = float(analysis.get("squeeze_threshold", 0.0) or 0.0)
        return bool(direction == "LONG" and squeeze > 0 and width <= squeeze * 1.15 and vr >= 1.3 and analysis.get("price", 0) > analysis.get("ema50", 0))
    if condition == "VOL_BREAKOUT":
        return bool(vr >= 1.5 and direction == "LONG")
    if condition == "SUPERTREND":
        return bool(st == 1 and direction == "LONG")
    if condition == "EMA_CROSS":
        return bool(ema50 is not None and ema200 is not None and ema50 > ema200 and direction == "LONG")
    if condition == "ORDERBOOK":
        return bool(ob >= 0.12 and direction == "LONG")
    if condition == "STOCHASTIC":
        return bool(k is not None and d is not None and k > d and k < 80 and direction == "LONG")
    # TOP20_AI_NEWS and ASAP both mean: earliest strong, confirmed setup.
    return direction == "LONG" if market_mode == "spot" else direction in ("LONG", "SHORT")


def _select_analyzed_candidate(valid, state, condition, market_mode, selected_coin=None):
    pool = valid
    if selected_coin and selected_coin != "AUTO":
        clean = str(selected_coin).upper().replace("INR", "").replace("USDT", "").replace("/", "")
        pool = [c for c in valid if str(c.get("base_coin", "")).upper() == clean]
    else:
        # Always start with the highest-volume 20 coins, not an arbitrary coin.
        pool = sorted(valid, key=lambda x: float(x.get("volume", 0) or 0), reverse=True)[:20]

    best = None
    best_score = -1
    for coin in pool:
        a = _analyze_coin(coin, state)
        if not _signal_passes(a, condition, market_mode):
            continue
        if a["score"] > best_score:
            best_score = a["score"]
            best = (coin, a)
    return best


async def market_scanner_loop():
    while True:
        try:
            if SCANNER_LOCK is not None and SCANNER_LOCK.locked():
                await asyncio.sleep(0.25)
                continue
            lock_ctx = SCANNER_LOCK if SCANNER_LOCK is not None else asyncio.Lock()
            async with lock_ctx:
                for dev_id, state in list(user_sessions.items()):
                    try:
                        # 1) ALWAYS REFRESH PNL & MONITOR TARGET / SL / TRAILING STOP LOSS FOR ACTIVE TRADES
                        active = list(state.get("active_trades", []))
                        if active:
                            live_prices, live_by_base = _build_live_price_maps(state)

                            for trade in active:
                                if trade.get("_closing"):
                                    continue

                                sym = str(trade.get("symbol", "")).upper()
                                clean = sym.replace("INR", "").replace("USDT", "").replace("/", "")
                                curr_p = _resolve_trade_live_price(state, trade, live_prices, live_by_base)

                                entry = float(trade.get("entry_price", 0.0) or 0.0)
                                target_p = float(trade.get("target_price", 0.0) or 0.0)
                                sl_p = float(trade.get("sl_price", 0.0) or 0.0)

                                if curr_p > 0 and entry > 0:
                                    _update_trade_unrealized_pnl(trade, curr_p)
                                
                                    # --- TRAILING STOP LOSS (TSL) LOGIC ---
                                    is_long = trade.get("type") in ["LONG", "BUY"]
                                    sl_pct_val = float(state.get("sl_percent", 1.5)) / 100.0
                                
                                    if is_long:
                                        highest = float(trade.get("highest_price", entry) or entry)
                                        if curr_p > highest:
                                            trade["highest_price"] = curr_p
                                            # Trail SL upwards if price goes up
                                            new_sl = curr_p * (1.0 - sl_pct_val)
                                            if new_sl > sl_p:
                                                trade["sl_price"] = new_sl
                                                sl_p = new_sl
                                    else:
                                        lowest = float(trade.get("lowest_price", entry) or entry)
                                        if curr_p < lowest:
                                            trade["lowest_price"] = curr_p
                                            # Trail SL downwards if price goes down for short
                                            new_sl = curr_p * (1.0 + sl_pct_val)
                                            if new_sl < sl_p:
                                                trade["sl_price"] = new_sl
                                                sl_p = new_sl

                                # Keep the persistent active-trade record synchronized.
                                db_update_active_trade(trade, dev_id, state.get("active_broker", "paper"))

                                if curr_p <= 0 or entry <= 0 or target_p <= 0 or sl_p <= 0:
                                    continue

                                is_long = trade.get("type") in ["LONG", "BUY"]
                                if is_long:
                                    current_pct = ((curr_p - entry) / entry) * 100.0
                                    target_hit = curr_p >= target_p
                                    sl_hit = curr_p <= sl_p
                                else:
                                    current_pct = ((entry - curr_p) / entry) * 100.0
                                    target_hit = curr_p <= target_p
                                    sl_hit = curr_p >= sl_p

                                if target_hit or sl_hit:
                                    reason = "TARGET HIT" if target_hit else "SL/TSL HIT"
                                    ok, exit_price, filled_qty, msg = _close_trade_at_market(
                                        state, dev_id, trade, reason, curr_p
                                    )

                                    if ok:
                                        add_log(
                                            state,
                                            f"🎯 {reason}: {sym} | Exit {exit_price} | "
                                            f"P&L {trade.get('pnl_percent', current_pct)}%"
                                        )
                                    else:
                                        add_log(
                                            state,
                                            f"⚠️ {reason} DETECTED but EXIT NOT CONFIRMED: "
                                            f"{sym} | {msg}"
                                        )

                        # 2) NEW DEAL SCANNING
                        if not state.get("is_running"):
                            continue

                        allowed_slots = max(1, int(state.get("max_trades", 1)))
                        if len(state.get("active_trades", [])) >= allowed_slots:
                            continue

                        all_coins = fetch_active_exchange_markets(state)
                        if not all_coins:
                            continue

                        order_amount = float(state.get("trade_amount", 500.0))
                        active_symbols = {
                            str(t.get("symbol", "")).upper()
                            for t in state.get("active_trades", [])
                        }

                        valid = [
                            c for c in all_coins
                            if float(c.get("price", 0) or 0) > 0
                        ]
                        if not valid:
                            continue

                        selected = str(state.get("selected_coin", "AUTO")).upper()
                        deal_cond = str(state.get("deal_condition", "ASAP")).upper()
                        market_mode = state.get("market_mode", "spot").lower()

                        # REAL LIVE SCANNER: analyze the selected exchange's Top-100
                        # volume-ranked market universe. The browser never invents signals.
                        scan_pool = valid
                        if selected != "AUTO":
                            clean_selected = selected.replace("INR", "").replace("USDT", "").replace("/", "")
                            scan_pool = [
                                c for c in valid
                                if str(c.get("base_coin", "")).upper() == clean_selected
                            ]
                        else:
                            scan_pool = sorted(
                                valid,
                                key=lambda x: float(x.get("volume", 0) or 0),
                                reverse=True
                            )[:100]

                        state["scanner_cycle"] = int(state.get("scanner_cycle", 0) or 0) + 1
                        state["scanner_reports"] = []
                        add_log(
                            state,
                            f"🔎 SCAN START | {len(scan_pool)} coin(s) | {deal_cond} | "
                            f"{market_mode.upper()} | Top-100 volume ranking | Cycle {state['scanner_cycle']}"
                        )

                        # Evaluate every coin and publish one structured report per coin.
                        # Active positions remain visible but can never be opened twice.
                        passing_scans = []
                        for scan_idx, scan_coin in enumerate(scan_pool, 1):
                            symbol = str(scan_coin.get("base_coin") or scan_coin.get("symbol") or "?").upper()
                            position = _active_position_for_coin(state, symbol)
                            try:
                                scan_analysis = _analyze_coin(scan_coin, state)
                                if not scan_analysis:
                                    report = {
                                        "cycle": state["scanner_cycle"], "scan_index": scan_idx,
                                        "scan_total": len(scan_pool), "symbol": symbol,
                                        "market_symbol": scan_coin.get("raw_symbol") or scan_coin.get("symbol"),
                                        "price": float(scan_coin.get("price", 0) or 0),
                                        "change": float(scan_coin.get("change", 0) or 0),
                                        "volume": float(scan_coin.get("volume", 0) or 0),
                                        "position": position, "direction": "NEUTRAL",
                                        "score": 0, "rsi": None, "volume_ratio": 0.0,
                                        "status": "INSUFFICIENT DATA", "action": "WAIT",
                                        "reason": "Not enough live candles for technical analysis."
                                    }
                                    record_scanner_report(state, report)
                                    add_log(state, f"🔍 SCAN {scan_idx:03d}/{len(scan_pool):03d} | {symbol} | {position} | INSUFFICIENT DATA | WAIT")
                                    await asyncio.sleep(0)
                                    continue

                                scan_pass = _signal_passes(scan_analysis, deal_cond, market_mode)
                                # Never select an already-active symbol for a new order.
                                scan_pass_for_order = scan_pass and position == "FLAT"
                                direction = str(scan_analysis.get("direction", "NEUTRAL")).upper()
                                score = int(scan_analysis.get("score", 0) or 0)
                                action = "BUY" if direction == "LONG" and scan_pass else "SELL" if direction == "SHORT" and scan_pass and market_mode != "spot" else "WAIT"
                                status = "ACTIVE POSITION" if position != "FLAT" else ("PASS" if scan_pass else "NO TRADE")
                                rsi_val = scan_analysis.get("rsi")
                                rsi_text = f"{rsi_val:.1f}" if rsi_val is not None else "N/A"
                                vol_ratio = float(scan_analysis.get("volume_ratio", 0.0) or 0.0)
                                report = {
                                    "cycle": state["scanner_cycle"], "scan_index": scan_idx,
                                    "scan_total": len(scan_pool), "symbol": symbol,
                                    "market_symbol": scan_coin.get("raw_symbol") or scan_coin.get("symbol"),
                                    "price": float(scan_analysis.get("price", scan_coin.get("price", 0)) or 0),
                                    "change": float(scan_coin.get("change", 0) or 0),
                                    "volume": float(scan_coin.get("volume", 0) or 0),
                                    "position": position, "direction": direction, "score": score,
                                    "rsi": float(rsi_val) if rsi_val is not None else None,
                                    "volume_ratio": vol_ratio, "status": status, "action": action,
                                    "reason": "; ".join(scan_analysis.get("reasons", [])[-3:]) or "Technical conditions evaluated."
                                }
                                record_scanner_report(state, report)
                                add_log(
                                    state,
                                    f"🔍 SCAN {scan_idx:03d}/{len(scan_pool):03d} | {symbol} | "
                                    f"{position} | {direction} | Score {score}/100 | RSI {rsi_text} | "
                                    f"Vol {vol_ratio:.1f}x | {status} | {action}"
                                )
                                if scan_pass_for_order:
                                    passing_scans.append((scan_coin, scan_analysis))
                                # Yield after every completed coin so the UI can read the
                                # report immediately instead of waiting for all 100 scans.
                                await asyncio.sleep(0)
                            except Exception as scan_exc:
                                report = {
                                    "cycle": state["scanner_cycle"], "scan_index": scan_idx,
                                    "scan_total": len(scan_pool), "symbol": symbol,
                                    "market_symbol": scan_coin.get("raw_symbol") or scan_coin.get("symbol"),
                                    "price": float(scan_coin.get("price", 0) or 0),
                                    "change": float(scan_coin.get("change", 0) or 0),
                                    "volume": float(scan_coin.get("volume", 0) or 0),
                                    "position": position, "direction": "NEUTRAL", "score": 0,
                                    "rsi": None, "volume_ratio": 0.0, "status": "SCAN ERROR",
                                    "action": "WAIT", "reason": str(scan_exc)[:240]
                                }
                                record_scanner_report(state, report)
                                add_log(state, f"⚠️ SCAN {scan_idx:03d}/{len(scan_pool):03d} | {symbol} | {position} | SCAN ERROR | {scan_exc}")
                                await asyncio.sleep(0)

                        if not passing_scans:
                            add_log(
                                state,
                                f"🔎 NO TRADE: {deal_cond} | All Top-100 coins failed the confirmed market-analysis rules."
                            )
                            continue

                        passing_scans.sort(
                            key=lambda pair: float(pair[1].get("score", 0) or 0),
                            reverse=True
                        )

                        free_slots = max(
                            0,
                            allowed_slots - len(state.get("active_trades", []))
                        )
                        selected_candidates = passing_scans[:free_slots]
                        quote = state.get("quote_currency", "INR").upper()
                        target_pct = max(
                            0.001, float(state.get("target_percent", 2.5)) / 100.0
                        )
                        sl_pct = max(
                            0.001, float(state.get("sl_percent", 1.5)) / 100.0
                        )
                        broker = state.get("active_broker", "coindcx").lower()

                        for candidate_no, (target_coin, analysis) in enumerate(selected_candidates, 1):
                            try:
                                coin_sym = str(target_coin["symbol"])
                                current_p = float(target_coin["price"])
                                add_log(
                                    state,
                                    f"🧠 MARKET ANALYSIS PASS {candidate_no}/{len(selected_candidates)}: "
                                    f"{coin_sym} | {analysis['direction']} | "
                                    f"Score {analysis['score']}/100 | RSI "
                                    f"{analysis['rsi']:.1f} | Vol {analysis['volume_ratio']:.1f}x | "
                                    + "; ".join(analysis['reasons'])
                                )

                                if market_mode == "futures" and analysis.get("direction") == "SHORT":
                                    side = "sell"
                                    trade_type = "SHORT"
                                else:
                                    side = "buy"
                                    trade_type = "LONG"

                                if broker == "paper":
                                    if float(state.get("paper_balance", 500000.0) or 0.0) < order_amount:
                                        add_log(
                                            state,
                                            f"⚠️ PAPER BALANCE LIMIT: {coin_sym} skipped; "
                                            f"required {get_curr_symbol(state)}{order_amount:.2f}."
                                        )
                                        continue

                                    sim_price = current_p
                                    if sim_price <= 0:
                                        continue
                                    # Paper mode uses fractional quantities too; this
                                    # keeps the reserved amount exact for low-priced assets.
                                    calc_qty = round(order_amount / sim_price, 12)
                                    if calc_qty <= 0:
                                        add_log(
                                            state,
                                            f"⚠️ ORDER SIZE TOO SMALL: {coin_sym} at {sim_price}."
                                        )
                                        continue

                                    state["paper_balance"] = round(
                                        float(state.get("paper_balance", 500000.0))
                                        - order_amount,
                                        2,
                                    )
                                    entry_price = sim_price
                                    filled_qty = float(calc_qty)

                                elif broker == "coindcx":
                                    ok, entry_price, filled_qty, result = execute_coindcx_order(
                                        state,
                                        coin_sym,
                                        side=side,
                                        target_amount=order_amount,
                                    )
                                    if not ok or filled_qty <= 0:
                                        add_log(
                                            state,
                                            f"⚠️ ORDER NOT CONFIRMED: {coin_sym} | {result}"
                                        )
                                        continue

                                elif hasattr(ccxt, broker):
                                    ok, entry_price, filled_qty, result = execute_ccxt_order(
                                        state,
                                        coin_sym,
                                        side=side,
                                        target_amount=order_amount,
                                    )
                                    if not ok or filled_qty <= 0:
                                        add_log(
                                            state,
                                            f"⚠️ ORDER NOT CONFIRMED: {coin_sym} | {result}"
                                        )
                                        continue
                                else:
                                    add_log(
                                        state,
                                        f"⚠️ Unsupported active broker: {broker.upper()}"
                                    )
                                    continue

                                entry_price = float(entry_price)
                                filled_qty = float(filled_qty)
                                if entry_price <= 0 or filled_qty <= 0:
                                    continue

                                if trade_type == "SHORT":
                                    target_price = entry_price * (1.0 - target_pct)
                                    sl_price = entry_price * (1.0 + sl_pct)
                                else:
                                    target_price = entry_price * (1.0 + target_pct)
                                    sl_price = entry_price * (1.0 - sl_pct)

                                # Millisecond timestamps can collide when multiple
                                # trades are opened in one scanner pass. Use a UUID
                                # suffix so every active/history record is unique.
                                trade_id = f"{int(time.time() * 1000)}_{uuid.uuid4().hex[:10]}"

                                new_trade = {
                                    "id": trade_id,
                                    "symbol": coin_sym,
                                    "currency": quote,
                                    "type": trade_type,
                                    "entry_price": entry_price,
                                    "quantity": filled_qty,
                                    "amount": round(entry_price * filled_qty, 2),
                                    "reserved_amount": round(order_amount, 2) if broker == "paper" else round(entry_price * filled_qty, 2),
                                    "highest_price": entry_price,
                                    "lowest_price": entry_price,
                                    "sl_price": sl_price,
                                    "target_price": target_price,
                                    "current_price": entry_price,
                                    "current_pnl_percent": 0.0,
                                    "current_pnl_val": 0.0,
                                    "unrealized_pnl": 0.0,
                                    "unrealized_pnl_percent": 0.0,
                                    "time": get_global_time(),
                                }

                                state["active_trades"].insert(0, new_trade)
                                db_save_active_trade(new_trade, dev_id, broker)
                                save_state_to_db(dev_id, state)
                                add_log(
                                    state,
                                    f"⚡ BOT OPENED {trade_type}: {coin_sym} | "
                                    f"Entry {get_curr_symbol(state)}{entry_price} | "
                                    f"Qty {filled_qty} | Target {new_trade['target_price']} | "
                                    f"SL {new_trade['sl_price']} | "
                                    f"Slots {len(state['active_trades'])}/{allowed_slots}"
                                )

                            except Exception as trade_open_err:
                                add_log(
                                    state,
                                    f"⚠️ TRADE OPEN ERROR | {target_coin.get('symbol', '?')} | "
                                    f"{trade_open_err}"
                                )

                    except Exception as inner_err:
                        print(f"Loop error for {dev_id}: {inner_err}")

        except Exception as e:
            print(f"Scanner error: {e}")

        await asyncio.sleep(2.0)


@app.post("/api/clear-history")
async def clear_history(request: Request):
    try:
        data = await request.json()
        device_id = data.get("device_id", "DEFAULT_DEVICE")
        state = get_user_session(device_id)
        if state.get("active_trades"):
            return {"status":"error","message":"Close all active trades before clearing history."}
        conn=sqlite3.connect(DB_FILE)
        conn.execute("DELETE FROM trades WHERE device_id = ?", (device_id,))
        conn.commit(); conn.close()
        add_log(state, "🗑️ Trade history cleared for this device.")
        return {"status":"success","message":"Trade history cleared."}
    except Exception as e:
        return {"status":"error","message":str(e)}

@app.get("/api/health")
def health_check():
    return {"status":"ok","service":"hitechpro","time":get_global_time(),"scanner_task":bool(SCANNER_TASK and not SCANNER_TASK.done())}

@app.get("/")
def root():
    return {
        "status": "HiTechPro Trading Engine Live",
        "database": "SQLite Trades Active",
        "total_vip_keys": len(keys_db)
    }

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
