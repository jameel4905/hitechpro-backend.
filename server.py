import os, time, hmac, hashlib, json, requests, ccxt, uvicorn, asyncio, random
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime

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
    "active_broker": "none",
    "api_key": "",
    "secret_key": "",
    "trade_amount_usdt": 5.0,
    "trade_type": "intraday", 
    "strategy": "volume",
    "logs": ["🤖 Master AI Engine Initialized. Ready for Multi-Exchange Data."],
    "active_trades": [],   
    "trade_history": [],    
    "paper_balance": 10000.0,
    "today_pnl": 0.0, 
    "last_settlement_date": datetime.utcnow().strftime("%Y-%m-%d") 
}

DATA_FILE = "bot_data.json"

def get_global_time():
    return datetime.utcnow().isoformat() + "Z"

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
                bot_state["trade_amount_usdt"] = data.get("trade_amount_usdt", 5.0)
                bot_state["trade_type"] = data.get("trade_type", "intraday")
                bot_state["strategy"] = data.get("strategy", "volume")
                bot_state["today_pnl"] = data.get("today_pnl", 0.0)
                bot_state["last_settlement_date"] = data.get("last_settlement_date", datetime.utcnow().strftime("%Y-%m-%d"))
                bot_state["active_broker"] = data.get("active_broker", "none")
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
                "trade_amount_usdt": bot_state["trade_amount_usdt"],
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
        bot_state["paper_balance"] += bot_state["today_pnl"]
        bot_state["paper_balance"] = round(bot_state["paper_balance"], 2)
        settled_amount = bot_state["today_pnl"]
        bot_state["today_pnl"] = 0.0
        bot_state["last_settlement_date"] = current_date
        add_log(f"🏦 Midnight Settlement: ${settled_amount} moved to Wallet.")
        save_memory()

# 🌐 UNIVERSAL DATA ADAPTER
def fetch_active_exchange_markets():
    broker = bot_state.get("active_broker", "paper")
    
    if broker == "coindcx":
        try:
            res = requests.get("https://api.coindcx.com/exchange/ticker", timeout=8)
            data = res.json()
            market_list = []
            for item in data:
                m = item.get("market", "")
                if "USDT" in m:
                    clean_sym = m.replace("B-", "").replace("_", "")
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

# 🔥 REAL COINDCX BUY ORDER FUNCTION
def execute_coindcx_buy(symbol, target_usdt=5.0):
    api_key = bot_state.get("api_key", "").strip()
    secret_key = bot_state.get("secret_key", "").strip()
    if not api_key or not secret_key:
        return False, None, 0, "API Keys missing"

    try:
        clean_coin = symbol.replace("USDT", "").replace("/", "").replace("B-", "").replace("_", "").upper()
        ticker_res = requests.get("https://api.coindcx.com/exchange/ticker", timeout=8)
        tickers = ticker_res.json()
        
        target_market = f"B-{clean_coin}_USDT"
        current_price = 0.0
        for t in tickers:
            m = t.get('market', '')
            if m in [f"B-{clean_coin}_USDT", f"{clean_coin}USDT", f"B-{clean_coin}USDT"]:
                target_market = m
                current_price = float(t.get('last_price', 0.0))
                break

        if current_price <= 0:
            return False, None, 0, f"Price not found for {clean_coin}"

        # DOGE integer precision, baaki coins 2 decimals
        calc_qty = target_usdt / current_price
        quantity = int(round(calc_qty)) if clean_coin == "DOGE" else round(calc_qty, 2)
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

# 🔥 REAL SELL ENGINE (COINDCX)
def execute_coindcx_sell(symbol, quantity=0):
    api_key = bot_state.get("api_key", "").strip()
    secret_key = bot_state.get("secret_key", "").strip()
    if not api_key or not secret_key:
        return False, "API keys missing"

    try:
        clean_coin = symbol.replace("USDT", "").replace("/", "").replace("B-", "").replace("_", "").upper()
        ticker_res = requests.get("https://api.coindcx.com/exchange/ticker", timeout=8)
        tickers = ticker_res.json()
        target_market = f"B-{clean_coin}_USDT"
        for t in tickers:
            m = t.get('market', '')
            if m in [f"B-{clean_coin}_USDT", f"{clean_coin}USDT", f"B-{clean_coin}USDT"]:
                target_market = m
                break

        sell_qty = quantity
        if sell_qty <= 0:
            timeStamp = int(round(time.time() * 1000))
            bal_body = json.dumps({"timestamp": timeStamp}, separators=(',', ':'))
            bal_sig = hmac.new(secret_key.encode('utf-8'), bal_body.encode('utf-8'), hashlib.sha256).hexdigest()
            bal_headers = {'Content-Type': 'application/json', 'X-AUTH-APIKEY': api_key, 'X-AUTH-SIGNATURE': bal_sig}
            bal_res = requests.post("https://api.coindcx.com/exchange/v1/users/balances", data=bal_body, headers=bal_headers, timeout=8)
            bal_data = bal_res.json()
            if isinstance(bal_data, list):
                for item in bal_data:
                    if item.get("currency") == clean_coin:
                        sell_qty = float(item.get("balance", 0.0))
                        break

        sell_qty = int(sell_qty) if clean_coin == "DOGE" else round(sell_qty, 2)
        if sell_qty <= 0:
            return False, f"Zero {clean_coin} balance"

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
        add_log(f"📤 REAL EXIT: Sold {sell_qty} {clean_coin} on CoinDCX!")
        return True, res.json()
    except Exception as e:
        add_log(f"⚠️ CoinDCX Sell Failed: {str(e)[:40]}")
        return False, str(e)

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
            return {"status": "success", "message": "🟢 Paper Trading Synced!", "balances": {"USDT": bot_state["paper_balance"]}}
            
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

        success, price, qty, res = execute_coindcx_buy("DOGEUSDT", 5.0)
        if success:
            new_trade = {
                "id": int(time.time()),
                "symbol": "DOGEUSDT",
                "type": "LONG",
                "entry_price": price,
                "quantity": qty,
                "amount_usdt": round(qty * price, 2),
                "highest_price": price,
                "lowest_price": price,
                "sl_price": price * 0.98,
                "time": get_global_time()
            }
            bot_state["active_trades"].insert(0, new_trade)
            save_memory()
            add_log(f"🧪 REAL TEST ORDER: Bought {qty} DOGE at ${price}")
            return {"status": "success", "message": "Order Placed Successfully", "trade": {"symbol": "DOGE/USDT", "entry_price": price, "amount": qty}}
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
        if "amount" in data and float(data["amount"]) > 0:
            bot_state["trade_amount_usdt"] = float(data["amount"])
        bot_state["trade_type"] = data.get("trade_type", bot_state["trade_type"])
        bot_state["strategy"] = data.get("strategy", bot_state["strategy"])
        save_memory()
        add_log(f"🚀 BOT STARTED on {bot_state['active_broker'].upper()} | Lot Size: ${bot_state['trade_amount_usdt']}")
        return {"status": "success", "message": "Bot Started!"}
    elif action == "stop":
        bot_state["is_running"] = False
        save_memory()
        add_log("🛑 BOT STOPPED! Market scanning halted.")
        return {"status": "success", "message": "Bot Stopped!"}

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
            execute_coindcx_sell(trade_to_close.get("symbol", "DOGEUSDT"), trade_to_close.get("quantity", 0))

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
            
        pnl_usdt = (trade_to_close["amount_usdt"] * pnl_percent) / 100
        
        trade_to_close["pnl_percent"] = round(pnl_percent, 2)
        trade_to_close["pnl_usdt"] = round(pnl_usdt, 2)
        trade_to_close["exit_price"] = exit_price
        trade_to_close["close_time"] = get_global_time()
        
        if bot_state["active_broker"] == "paper":
            bot_state["today_pnl"] += trade_to_close["pnl_usdt"]
            bot_state["today_pnl"] = round(bot_state["today_pnl"], 2)

        bot_state["active_trades"].remove(trade_to_close)
        bot_state["trade_history"].insert(0, trade_to_close)
        if len(bot_state["trade_history"]) > 30: bot_state["trade_history"].pop()
        
        add_log(f"🛑 EXIT: {trade_to_close['symbol']} | P&L: {trade_to_close['pnl_percent']}%")
        save_memory()
        
        return {"status": "success", "message": f"Exit Successful! PNL: {trade_to_close['pnl_percent']}%"}
    except Exception as e:
        return {"status": "error", "message": f"API Error: {str(e)}"}

@app.get("/api/bot-logs")
def get_bot_logs():
    return {
        "status": "success", 
        "is_running": bot_state["is_running"], 
        "logs": bot_state["logs"],
        "active_broker": bot_state["active_broker"],
        "trade_amount_usdt": bot_state["trade_amount_usdt"]
    }

@app.get("/api/get-trades")
def get_trades():
    return {
        "status": "success", 
        "active": bot_state["active_trades"], 
        "history": bot_state["trade_history"],
        "paper_balance": bot_state["paper_balance"],
        "today_pnl": bot_state["today_pnl"] 
    }

# 🔄 SCANNER LOOP (AUTO REAL BUY HOOKED)
async def market_scanner_loop():
    ignore_coins = ["USDCUSDT", "FDUSDUSDT", "TUSDUSDT", "BUSDUSDT", "EURUSDT", "XUSDUSDT"]
    
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
                    pnl_usdt = (trade["amount_usdt"] * pnl_percent) / 100
                    trade["pnl_percent"] = round(pnl_percent, 2)
                    trade["pnl_usdt"] = round(pnl_usdt, 2)
                    trade["exit_price"] = exit_p
                    trade["close_time"] = get_global_time()
                    
                    if bot_state["active_broker"] == "paper":
                        bot_state["today_pnl"] += trade["pnl_usdt"]
                        bot_state["today_pnl"] = round(bot_state["today_pnl"], 2)

                    bot_state["active_trades"].remove(trade)
                    bot_state["trade_history"].insert(0, trade)
                    if len(bot_state["trade_history"]) > 30: bot_state["trade_history"].pop()
                    add_log(f"🔔 TSL HIT: {trade['symbol']} | P&L: {trade['pnl_percent']}%")
                
                save_memory()

                # Scanner Condition Check (Max 1 active trade during testing)
                if len(bot_state["active_trades"]) < 1:
                    order_amount = bot_state["trade_amount_usdt"]
                    
                    # Testing Phase ke liye Doge market prioritize
                    target_coin = next((c for c in all_coins if "DOGE" in c['symbol']), None)
                    if not target_coin and all_coins:
                        target_coin = all_coins[0]

                    if target_coin:
                        coin_sym = target_coin['symbol']
                        current_p = target_coin['price']

                        # CoinDCX Real Order Trigger
                        if bot_state["active_broker"] == "coindcx":
                            success, buy_price, buy_qty, res = execute_coindcx_buy(coin_sym, order_amount)
                            if success:
                                new_trade = {
                                    "id": int(time.time()),
                                    "symbol": coin_sym,
                                    "type": "LONG",
                                    "entry_price": buy_price,
                                    "quantity": buy_qty,
                                    "amount_usdt": round(buy_qty * buy_price, 2),
                                    "highest_price": buy_price,
                                    "lowest_price": buy_price,
                                    "sl_price": buy_price * 0.98,
                                    "time": get_global_time()
                                }
                                bot_state["active_trades"].insert(0, new_trade)
                                save_memory()
                                add_log(f"⚡ REAL BUY EXECUTED: {buy_qty} {coin_sym} at ${buy_price}")
                            else:
                                add_log(f"⚠️ Auto-Buy Failed: {str(res)[:35]}")
                        
                        elif bot_state["active_broker"] == "paper":
                            calc_qty = int(order_amount / current_p)
                            new_trade = {
                                "id": int(time.time()),
                                "symbol": coin_sym,
                                "type": "LONG",
                                "entry_price": current_p,
                                "quantity": calc_qty,
                                "amount_usdt": order_amount,
                                "highest_price": current_p,
                                "lowest_price": current_p,
                                "sl_price": current_p * 0.98,
                                "time": get_global_time()
                            }
                            bot_state["active_trades"].insert(0, new_trade)
                            save_memory()
                            add_log(f"⚡ [PAPER] BUY: {coin_sym} at ${current_p}")

                        await asyncio.sleep(5)
                        
            except Exception as e:
                add_log(f"❌ Scanner Loop Error: {str(e)[:35]}")
        
        await asyncio.sleep(3)

@app.on_event("startup")
async def startup(): 
    asyncio.create_task(market_scanner_loop())

@app.get("/")
def root(): return {"status": "HiTech AI Engine Live!", "broker": bot_state["active_broker"]}

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
