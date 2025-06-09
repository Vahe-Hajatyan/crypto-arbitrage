import os
import time
import math
import psycopg2
from binance.client import Client
from binance.exceptions import BinanceAPIException
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

SIMULATION = True               # True — simulation (no real orders/transfers), False — LIVE trading
SPREAD = 0.03                   # Spread threshold (%) to open a position
RISK_PER_TRADE = 1.0            # 1.0 = 100% of balance per trade
MIN_POSITION_SIZE = 10.0        # Minimum position size in USDT
STARTING_BALANCE = 1000.0       # Initial “fake” balance (SIMULATION)
FUTURES_FEE = 0.00045           # 0.045% futures fee
MARGIN_FEE = 0.00075            # 0.075% margin fee
MAX_TRADE_DURATION = 1800       # Maximum holding time (1 hour)

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
client = Client(API_KEY, API_SECRET)

conn = psycopg2.connect(
    dbname='arbitrage',
    user='admin',
    password='admin',
    host='localhost',
    port='5432'
)
cursor = conn.cursor()


def init_db():
    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS arbitrage_opportunities
                   (
                       id
                       SERIAL
                       PRIMARY
                       KEY,
                       timestamp
                       TIMESTAMP,
                       symbol
                       VARCHAR
                   (
                       20
                   ),
                       spot_price FLOAT,
                       futures_price FLOAT,
                       spread_percent FLOAT,
                       action VARCHAR
                   (
                       20
                   ),
                       trade_executed BOOLEAN DEFAULT FALSE,
                       profit_usdt FLOAT DEFAULT 0
                       )
                   """)
    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS open_trades
                   ( id SERIAL PRIMARY KEY,
                       symbol
                       VARCHAR
                   (
                       20
                   ) NOT NULL,
                       market_type VARCHAR
                   (
                       10
                   ) NOT NULL,
                       side VARCHAR
                   (
                       10
                   ) NOT NULL,
                       entry_price FLOAT NOT NULL,
                       quantity FLOAT NOT NULL,
                       timestamp_open TIMESTAMP NOT NULL,
                       close_price FLOAT,
                       timestamp_close TIMESTAMP,
                       profit_usdt FLOAT DEFAULT 0,
                       closed BOOLEAN DEFAULT FALSE
                       );
                   """)
    cursor.execute("""ALTER TABLE arbitrage_opportunities
        ADD COLUMN IF NOT EXISTS open_trade_id INTEGER REFERENCES open_trades(id);
                   """)
    cursor.execute("""
                   ALTER TABLE arbitrage_opportunities
                   DROP
                   CONSTRAINT IF EXISTS arbitrage_opportunities_unique;

                   ALTER TABLE arbitrage_opportunities
                       ADD CONSTRAINT arbitrage_opportunities_unique UNIQUE (symbol);
                   """)
    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS profit_tracking
                   (
                       id
                       SERIAL
                       PRIMARY
                       KEY,
                       timestamp
                       TIMESTAMP,
                       current_balance
                       FLOAT,
                       profit_usdt
                       FLOAT,
                       profit_percent
                       FLOAT,
                       daily_profit
                       FLOAT,
                       weekly_profit
                       FLOAT,
                       monthly_profit
                       FLOAT
                   )
                   """)
    cursor.execute("""
                   ALTER TABLE profit_tracking
                   DROP
                   CONSTRAINT IF EXISTS profit_tracking_unique;

                   ALTER TABLE profit_tracking
                       ADD CONSTRAINT profit_tracking_unique UNIQUE (current_balance);
                   """)

    conn.commit()

init_db()

def safe_api_call(func, *args, **kwargs):
    """Wrapper to retry API calls up to 3 times on errors."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except (BinanceAPIException, ConnectionError, TimeoutError) as e:
            print(f"[API ERROR] Attempt {attempt+1}/{max_retries}: {e}")
            time.sleep(2 ** attempt)
    return None

def get_lot_size(symbol: str, market_type: str) -> float:
    """
    Returns the minimum lot step size (stepSize) for margin or futures.
    """
    try:
        if market_type == "FUTURES":
            info = safe_api_call(client.futures_exchange_info)
            if info:
                for s in info['symbols']:
                    if s['symbol'] == symbol:
                        for f in s['filters']:
                            if f['filterType'] == 'LOT_SIZE':
                                return float(f['stepSize'])
        else:
            info = safe_api_call(client.get_symbol_info, symbol=symbol)
            if info:
                for f in info['filters']:
                    if f['filterType'] == 'LOT_SIZE':
                        return float(f['stepSize'])
    except Exception as e:
        print(f"[ERROR] get_lot_size for {symbol}: {e}")
    return 0.001

def fetch_prices(symbol: str):
    """
    Returns (spot_price, futures_price) for the given symbol.
    """
    spot_ticker = safe_api_call(client.get_symbol_ticker, symbol=symbol)
    futures_ticker = safe_api_call(client.futures_symbol_ticker, symbol=symbol)
    if not spot_ticker or not futures_ticker:
        return None, None
    try:
        spot_price = float(spot_ticker['price'])
        futures_price = float(futures_ticker['price'])
        return spot_price, futures_price
    except Exception as e:
        print(f"[ERROR] Parsing prices for {symbol}: {e}")
        return None, None

def get_real_spot_usdt_balance() -> float:
    """Free USDT balance in Spot wallet."""
    info = safe_api_call(client.get_asset_balance, asset="USDT")
    return float(info["free"]) if info and "free" in info else 0.0

def get_real_futures_usdt_balance() -> float:
    """Available (withdrawAvailable) USDT balance in Futures wallet."""
    balances = safe_api_call(client.futures_account_balance)
    if balances:
        for entry in balances:
            if entry["asset"] == "USDT":
                return float(entry["withdrawAvailable"])
    return 0.0

def get_available_balance(market_type: str) -> float:
    """
    Returns USDT balance:
    - If SIMULATION=True: from profit_tracking table (fake balance).
    - If SIMULATION=False:
        * market_type in ("SPOT","MARGIN"): real Spot USDT.
        * market_type == "FUTURES": real Futures USDT.
    """
    if SIMULATION:
        cursor.execute("SELECT current_balance FROM profit_tracking ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()
        return float(row[0]) if row else STARTING_BALANCE

    if market_type in ("SPOT", "MARGIN"):
        return get_real_spot_usdt_balance()
    elif market_type == "FUTURES":
        return get_real_futures_usdt_balance()
    return 0.0

def transfer_spot_to_isolated_margin(symbol: str, asset: str, amount: float):
    """
    Transfer USDT from Spot to Isolated Margin for a specific pair.
    """
    if SIMULATION:
        print(f"[SIM] TRANSFER Spot→IsolatedMargin | {asset}={amount:.4f} for {symbol}")
        return {"tranId": "SIM_SM"}
    try:
        return safe_api_call(
            client.transfer_spot_to_isolated_margin,
            symbol=symbol, asset=asset, amount=amount
        )
    except Exception as e:
        print(f"[ERROR] transfer_spot_to_isolated_margin for {symbol}: {e}")
        return None

def transfer_isolated_margin_to_spot(symbol: str, asset: str, amount: float):
    """
    Transfer USDT from Isolated Margin to Spot for a specific pair.
    """
    if SIMULATION:
        print(f"[SIM] TRANSFER IsolatedMargin→Spot | {asset}={amount:.4f} for {symbol}")
        return {"tranId": "SIM_MS"}
    try:
        return safe_api_call(
            client.transfer_isolated_margin_to_spot,
            symbol=symbol, asset=asset, amount=amount
        )
    except Exception as e:
        print(f"[ERROR] transfer_isolated_margin_to_spot for {symbol}: {e}")
        return None

def transfer_spot_to_futures(asset: str, amount: float):
    """
    Transfer USDT from Spot to Futures.
    """
    if SIMULATION:
        print(f"[SIM] TRANSFER Spot→Futures | {asset}={amount:.4f}")
        return {"tranId": "SIM_SF"}
    try:
        return safe_api_call(
            client.futures_account_transfer,
            asset=asset, amount=amount, type=1  # 1 = SPOT→USDT_FUTURE
        )
    except Exception as e:
        print(f"[ERROR] transfer_spot_to_futures: {e}")
        return None

def transfer_futures_to_spot(asset: str, amount: float):
    """
    Transfer USDT from Futures to Spot.
    """
    if SIMULATION:
        print(f"[SIM] TRANSFER Futures→Spot | {asset}={amount:.4f}")
        return {"tranId": "SIM_FS"}
    try:
        return safe_api_call(
            client.futures_account_transfer,
            asset=asset, amount=amount, type=2  # 2 = USDT_FUTURE→SPOT
        )
    except Exception as e:
        print(f"[ERROR] transfer_futures_to_spot: {e}")
        return None

def calculate_position_size(symbol: str, price: float, market_type: str) -> float:
    balance = get_available_balance(market_type)
    risk_amount = (balance * RISK_PER_TRADE) / 2.0

    if risk_amount < MIN_POSITION_SIZE:
        return 0.0

    quantity = risk_amount / price
    step_size = get_lot_size(symbol, market_type)
    if step_size > 0:
        quantity = math.floor(quantity / step_size) * step_size
    return round(quantity, 8)

def simulate_trade(entry_price: float, exit_price: float, quantity: float,
                   entry_fee: float, exit_fee: float, trade_type: str) -> float:
    """
    Calculates net profit (USDT) taking into account:
    - Fees (entry_fee, exit_fee).
    trade_type: "SPOT_LONG", "SPOT_SHORT", "FUTURES_LONG", "FUTURES_SHORT".
    """
    if entry_price is None or exit_price is None:
        return 0.0

    if trade_type == "SPOT_LONG":
        cost = entry_price * quantity * (1 + entry_fee)
        revenue = exit_price * quantity * (1 - exit_fee)
        return round(revenue - cost, 8)

    if trade_type == "SPOT_SHORT":
        revenue = entry_price * quantity * (1 - entry_fee)
        cost = exit_price * quantity * (1 + exit_fee)
        return round(revenue - cost, 8)

    if trade_type == "FUTURES_LONG":
        cost = entry_price * quantity * (1 + entry_fee)
        revenue = exit_price * quantity * (1 - exit_fee)
        return round(revenue - cost, 8)

    if trade_type == "FUTURES_SHORT":
        revenue = entry_price * quantity * (1 - entry_fee)
        cost = exit_price * quantity * (1 + exit_fee)
        return round(revenue - cost, 8)

    return 0.0

def create_open_trade(symbol: str, market_type: str, side: str,
                      entry_price: float, quantity: float) -> int:
    """
    Saves the open position to the DB. Returns the created record ID.
    """
    try:
        timestamp = datetime.utcnow()
        cursor.execute("""
            INSERT INTO open_trades 
            (symbol, market_type, side, entry_price, quantity, timestamp_open)
            VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
        """, (symbol, market_type, side, entry_price, quantity, timestamp))
        trade_id = cursor.fetchone()[0]
        conn.commit()
        return trade_id
    except Exception as e:
        print(f"[ERROR] create_open_trade: {e}")
        return None

def close_trade(trade_id: int):
    """
    Closes the position:
    - Sends a real order (if SIMULATION=False).
    - Calculates profit and updates DB.
    """
    try:
        cursor.execute("""
            SELECT symbol, market_type, side, entry_price, quantity 
            FROM open_trades WHERE id=%s AND closed=FALSE
        """, (trade_id,))
        row = cursor.fetchone()
        if not row:
            return
        symbol, market_type, side, entry_price, quantity = row

        spot_price, futures_price = fetch_prices(symbol)
        if spot_price is None or futures_price is None:
            return

        if market_type == "MARGIN" and side == "LONG":
            trade_type = "MARGIN_LONG"
            exit_price = spot_price
            entry_fee = MARGIN_FEE
            exit_fee = MARGIN_FEE

            if not SIMULATION:
                # 1) Sell the base asset
                safe_api_call(
                    client.create_margin_order,
                    symbol=symbol, side=Client.SIDE_SELL,
                    type=Client.ORDER_TYPE_MARKET,
                    quantity=quantity, isIsolated='TRUE'
                )

                # 2) Transfer remaining USDT back to spot
                margin_acc = safe_api_call(client.get_isolated_margin_account)
                if margin_acc:
                    for asset_info in margin_acc['assets']:
                        if asset_info['symbol'] == symbol:
                            free_usdt = float(asset_info['quoteAsset']['free'])
                            if free_usdt > 0:
                                transfer_isolated_margin_to_spot(symbol, "USDT", free_usdt)
                            break

        elif market_type == "MARGIN" and side == "SHORT":
            trade_type = "SPOT_SHORT"
            exit_price = spot_price
            entry_fee = MARGIN_FEE
            exit_fee = MARGIN_FEE
            base_asset = symbol.replace("USDT", "")

            if not SIMULATION:
                safe_api_call(
                    client.create_margin_order,
                    symbol=symbol, side=Client.SIDE_BUY,
                    type=Client.ORDER_TYPE_MARKET,
                    quantity=quantity, isIsolated='TRUE'
                )
                safe_api_call(
                    client.repay_margin_loan,
                    asset=base_asset, amount=quantity,
                    isIsolated='TRUE', symbol=symbol
                )
                margin_acc = safe_api_call(client.get_isolated_margin_account)
                if margin_acc:
                    for asset_info in margin_acc['assets']:
                        if asset_info['symbol'] == symbol:
                            free_usdt = float(asset_info['quoteAsset']['free'])
                            if free_usdt > 0:
                                transfer_isolated_margin_to_spot(symbol, "USDT", free_usdt)
                            break

        elif market_type == "FUTURES":
            trade_type = f"FUTURES_{side}"
            exit_price = futures_price
            entry_fee = FUTURES_FEE
            exit_fee = FUTURES_FEE

            if not SIMULATION:
                if side == "LONG":
                    safe_api_call(
                        client.futures_create_order,
                        symbol=symbol, side=Client.SIDE_SELL,
                        type=Client.ORDER_TYPE_MARKET, quantity=quantity
                    )
                else:
                    safe_api_call(
                        client.futures_create_order,
                        symbol=symbol, side=Client.SIDE_BUY,
                        type=Client.ORDER_TYPE_MARKET, quantity=quantity
                    )
                futures_bal = get_real_futures_usdt_balance()
                if futures_bal > 0:
                    transfer_futures_to_spot("USDT", futures_bal)
        else:
            return

        profit = simulate_trade(entry_price, exit_price, quantity,
                                entry_fee, exit_fee, trade_type)

        timestamp_close = datetime.utcnow()
        cursor.execute("""
            UPDATE open_trades
            SET close_price=%s, timestamp_close=%s, profit_usdt=%s, closed=TRUE
            WHERE id=%s
        """, (exit_price, timestamp_close, profit, trade_id))
        conn.commit()

        # Update balance (fake in SIMULATION, or just history in LIVE)
        update_profit_tracking(profit)

        print(f"[INFO] Closed trade {trade_id} ({symbol}, {market_type}, {side}) → Profit = {profit:.4f} USDT")

    except Exception as e:
        print(f"[ERROR] close_trade: {e}")

def initialize_balance():
    """
    Insert initial balance into profit_tracking:
    - If SIMULATION=True -> use static STARTING_BALANCE.
    - If SIMULATION=False -> get real Spot USDT balance from Binance.
    """
    global STARTING_BALANCE
    cursor.execute("SELECT COUNT(*) FROM profit_tracking")
    if cursor.fetchone()[0] == 0:
        if SIMULATION:
            init_balance = STARTING_BALANCE
        else:
            init_balance = get_real_spot_usdt_balance()
            STARTING_BALANCE = init_balance

        cursor.execute("""
            INSERT INTO profit_tracking
            (timestamp, current_balance, profit_usdt, profit_percent, daily_profit, weekly_profit, monthly_profit)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (datetime.utcnow(), init_balance, 0.0, 0.0, 0.0, 0.0, 0.0))
        conn.commit()
        print(f"[INIT BALANCE] STARTING_BALANCE set to {init_balance:.2f} USDT")

def update_profit_tracking(profit_change: float = 0.0):
    """
    Adds a new entry to profit_tracking:
    - new_balance = last_balance + profit_change
    - profit_usdt = new_balance - STARTING_BALANCE
    - profit_percent = profit_usdt / STARTING_BALANCE * 100
    - calculates daily/weekly/monthly profit
    """
    try:
        cursor.execute("SELECT current_balance FROM profit_tracking ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()
        last_balance = float(row[0]) if row else STARTING_BALANCE

        new_balance = max(0.0, last_balance + profit_change)
        profit_usdt = new_balance - STARTING_BALANCE
        profit_percent = (profit_usdt / STARTING_BALANCE) * 100 if STARTING_BALANCE > 0 else 0.0
        now = datetime.utcnow()

        def get_period_profit(days: int) -> float:
            period_start = now - timedelta(days=days)
            cursor.execute("""
                SELECT current_balance FROM profit_tracking
                WHERE timestamp >= %s
                ORDER BY id ASC LIMIT 1
            """, (period_start,))
            pr = cursor.fetchone()
            return (new_balance - float(pr[0])) if pr else 0.0

        daily_profit = get_period_profit(1)
        weekly_profit = get_period_profit(7)
        monthly_profit = get_period_profit(30)

        cursor.execute("""
                       INSERT INTO profit_tracking (timestamp, current_balance, profit_usdt, profit_percent,
                                                    daily_profit, weekly_profit, monthly_profit)
                       VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT
                       ON CONSTRAINT profit_tracking_unique DO
                       UPDATE SET
                           timestamp = EXCLUDED.timestamp,
                           current_balance = EXCLUDED.current_balance,
                           profit_usdt = EXCLUDED.profit_usdt,
                           profit_percent = EXCLUDED.profit_percent,
                           daily_profit = EXCLUDED.daily_profit,
                           weekly_profit = EXCLUDED.weekly_profit,
                           monthly_profit = EXCLUDED.monthly_profit
                       """, (now, new_balance, profit_usdt, profit_percent, daily_profit, weekly_profit, monthly_profit))
        conn.commit()

        print(f"[BALANCE] {now.isoformat()} | Balance: {new_balance:.2f} USDT | Profit: {profit_usdt:.2f} ({profit_percent:.2f}%)")
    except Exception as e:
        print(f"[ERROR] update_profit_tracking: {e}")

def has_open_position(symbol: str) -> bool:
    """
    Check if there is already an open position for the symbol.
    """
    cursor.execute("""
        SELECT id FROM open_trades 
        WHERE symbol=%s AND closed=FALSE 
        LIMIT 1
    """, (symbol,))
    return cursor.fetchone() is not None

def check_open_positions():
    """
    Iterate through all open trades:
    - If hold_time > MAX_TRADE_DURATION -> close.
    - If spread crosses zero in the opposite direction -> close.
    """
    try:
        cursor.execute("""
            SELECT id, symbol, timestamp_open 
            FROM open_trades 
            WHERE closed=FALSE
        """)
        open_trades = cursor.fetchall()

        for trade_id, symbol, opened_at in open_trades:
            hold_time = (datetime.utcnow() - opened_at).total_seconds()
            if hold_time > MAX_TRADE_DURATION:
                print(f"[TRIGGER] Closing trade {trade_id} (max duration reached)")
                close_trade(trade_id)
                continue

            spot_price, futures_price = fetch_prices(symbol)
            if spot_price is None or futures_price is None:
                continue

            spread = futures_price - spot_price
            spread_percent = (spread / spot_price) * 100

            cursor.execute("SELECT market_type, side FROM open_trades WHERE id=%s AND closed=FALSE", (trade_id,))
            market_type, side = cursor.fetchone()

            if market_type == "MARGIN" and side == "LONG":
                # We opened MARGIN_LONG+FUTURES_SHORT → initially spread_percent>0.
                # If spread_percent <= 0, close:
                if spread_percent <= 0:
                    print(f"[TRIGGER] Closing SPOT_LONG+FUTURES_SHORT; spread={spread_percent:.4f}%")
                    close_trade(trade_id)

            elif market_type == "MARGIN" and side == "SHORT":
                # We opened MARGIN_SHORT+FUTURES_LONG → initially spread_percent<0.
                # If spread_percent >= 0, close:
                if spread_percent >= 0:
                    print(f"[TRIGGER] Closing SPOT_SHORT+FUTURES_LONG; spread={spread_percent:.4f}%")
                    close_trade(trade_id)

    except Exception as e:
        print(f"[ERROR] check_open_positions: {e}")

def analyze_and_store(symbol: str, threshold: float):
    """
    Analyzes arbitrage opportunity for the given symbol:
    1) Checks if there is an open position (skip if there is).
    2) Gets spot and futures prices.
    3) Calculates spread_percent.
    4) If spread_percent > threshold → opens SPOT_LONG + FUTURES_SHORT.
       If spread_percent < -threshold → opens SPOT_SHORT (_MARGIN) + FUTURES_LONG.
    5) Saves arbitrage opportunity to the table.
    6) Calls check_open_positions() to close positions.
    """


    spot_price, futures_price = fetch_prices(symbol)
    if spot_price is None or futures_price is None:
        return

    spread = futures_price - spot_price
    spread_percent = (spread / spot_price) * 100

    action = "HOLD"
    if spread_percent > threshold:
        action = "BUY_MARGIN_SHORT"
    elif spread_percent < -threshold:
        action = "BUY_FUTURES"

    open_trade_id = None

    if action == "BUY_MARGIN_LONG":
        # Use isolated margin for long position
        quantity = calculate_position_size(symbol, spot_price, "MARGIN")
        if quantity > 0:
            if not SIMULATION and has_open_position(symbol):
                # 1) Transfer Spot→IsolatedMargin
                margin_required = quantity * spot_price
                transfer_spot_to_isolated_margin(symbol, "USDT", margin_required)

                # 2) Buy base asset on margin (LONG position)
                safe_api_call(
                    client.create_margin_order,
                    symbol=symbol, side=Client.SIDE_BUY,
                    type=Client.ORDER_TYPE_MARKET,
                    quantity=quantity, isIsolated='TRUE'
                )

                # 3) FUTURES SHORT
                safe_api_call(
                    client.futures_create_order,
                    symbol=symbol, side=Client.SIDE_SELL,
                    type=Client.ORDER_TYPE_MARKET, quantity=quantity
                )
            open_trade_id = create_open_trade(symbol, "MARGIN", "LONG", spot_price, quantity)
            print(f"[OPEN] Trade {open_trade_id} | MARGIN_LONG & FUTURES_SHORT | Qty={quantity:.6f}")

    elif action == "BUY_FUTURES":
        quantity = calculate_position_size(symbol, spot_price, "MARGIN")
        if quantity > 0:
            base_asset = symbol.replace("USDT", "")
            if not SIMULATION and has_open_position(symbol):
                # 1) Transfer Spot→IsolatedMargin
                margin_required = quantity * spot_price
                transfer_spot_to_isolated_margin(symbol, "USDT", margin_required)
                # 2) Borrow base_asset
                safe_api_call(
                    client.create_margin_loan,
                    symbol=symbol, asset=base_asset,
                    isIsolated='TRUE', amount=quantity
                )
                # 3) Sell borrowed base_asset
                safe_api_call(
                    client.create_margin_order,
                    symbol=symbol, side=Client.SIDE_SELL,
                    type=Client.ORDER_TYPE_MARKET,
                    quantity=quantity, isIsolated='TRUE'
                )
                # 4) FUTURES LONG
                safe_api_call(
                    client.futures_create_order,
                    symbol=symbol, side=Client.SIDE_BUY,
                    type=Client.ORDER_TYPE_MARKET, quantity=quantity
                )
            open_trade_id = create_open_trade(symbol, "MARGIN", "SHORT", spot_price, quantity)
            print(f"[OPEN] Trade {open_trade_id} | SPOT_SHORT (MARGIN) & FUTURES_LONG | Qty={quantity:.6f}")

    try:
        cursor.execute("""
            INSERT INTO arbitrage_opportunities
            (timestamp, symbol, spot_price, futures_price, spread_percent, action, trade_executed, open_trade_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol) DO UPDATE SET
                timestamp = EXCLUDED.timestamp,
                spot_price = EXCLUDED.spot_price,
                futures_price = EXCLUDED.futures_price,
                spread_percent = EXCLUDED.spread_percent,
                action = EXCLUDED.action,
                trade_executed = EXCLUDED.trade_executed,
                open_trade_id = EXCLUDED.open_trade_id
        """, (
            datetime.utcnow(), symbol, spot_price, futures_price,
            spread_percent, action, (action != "HOLD"), open_trade_id
        ))
        conn.commit()
    except Exception as e:
        print(f"[ERROR] Saving opportunity for {symbol}: {e}")

    # Close positions if needed
    check_open_positions()

def main_loop():
    initialize_balance()

    exchange_info = safe_api_call(client.get_exchange_info)
    if not exchange_info:
        print("[ERROR] Failed to fetch exchange info")
        return

    all_symbols = [
        s['symbol'] for s in exchange_info['symbols']
        if s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING'
    ]
    liquid_symbols = [
        'BTCUSDT', 'ETHUSDT', 'SOLUSDT',
        'XRPUSDT', 'ADAUSDT', 'DOGEUSDT', 'DOTUSDT',
        'AVAXUSDT', 'LINKUSDT', 'TONUSDT', 'SUIUSDT', 'TRXUSDT'
    ]
    symbols = [s for s in all_symbols if s in liquid_symbols]

    print(f"[START] Monitoring {len(symbols)} symbols: {symbols}")

    cycle = 0
    try:
        while True:
            cycle += 1
            start_time = time.time()
            print(f"\n--- Cycle #{cycle} @ {datetime.utcnow().isoformat()} ---\n")

            for sym in symbols:
                try:
                    analyze_and_store(sym, SPREAD)
                except Exception as e:
                    print(f"[ERROR] Processing {sym}: {e}")
                time.sleep(0.2)

            update_profit_tracking(0.0)

            elapsed = time.time() - start_time
            print(f"[CYCLE] Completed in {elapsed:.1f}s")
            time.sleep(1)

    except KeyboardInterrupt:
        print("[STOPPED] Bot stopped by user")
    finally:
        cursor.close()
        conn.close()
        print("[CLOSED] Database connection closed")

if __name__ == "__main__":
    main_loop()
