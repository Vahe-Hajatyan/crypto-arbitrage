import time
import psycopg2
from binance.client import Client
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

THRESHOLD = 0.2 # Arbitrage Spread in percent
STARTING_BALANCE = 1000.0
SPOT_FEE = 0.00075  # 0.075%
FUTURES_FEE = 0.00045  # 0.045%

# Initializing the Binance Client
client = Client(os.getenv("API_KEY"), os.getenv("API_SECRET"))

# Connecting to PostgreSQL
conn = psycopg2.connect(
    dbname='arbitrage',
    user='admin',
    password='admin',
    host='localhost',
    port='5432'
)
cursor = conn.cursor()

# Creating tables
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
                   profit_usd FLOAT DEFAULT 0
                   )
               """)

cursor.execute("""
              ALTER TABLE arbitrage_opportunities
                DROP CONSTRAINT IF EXISTS arbitrage_opportunities_unique;

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
                   profit_usd
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
                DROP CONSTRAINT IF EXISTS profit_tracking_unique;

              ALTER TABLE profit_tracking
                ADD CONSTRAINT profit_tracking_unique UNIQUE (profit_usd);
               """)

conn.commit()


# Balance initialization
def initialize_balance():
    cursor.execute("SELECT COUNT(*) FROM profit_tracking")
    if cursor.fetchone()[0] == 0:
        cursor.execute("""
                       INSERT INTO profit_tracking
                       (timestamp, current_balance, profit_usd, profit_percent, daily_profit, weekly_profit,
                        monthly_profit)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)
                       """, (datetime.utcnow(), STARTING_BALANCE, 0, 0, 0, 0, 0))
        conn.commit()


initialize_balance()


def fetch_prices(symbol):
    """Obtaining current prices on the spot and futures markets."""
    try:
        # Getting the latest price on the spot market
        spot_ticker = client.get_symbol_ticker(symbol=symbol)
        spot_price = float(spot_ticker['price'])

        # Getting the latest price on the futures market
        futures_ticker = client.futures_symbol_ticker(symbol=symbol)
        futures_price = float(futures_ticker['price'])

        return spot_price, futures_price
    except Exception as e:
        print(f"Error getting prices for {symbol}: {e}")
        return None, None



def get_current_balance():
    """Getting the current balance from the database"""
    cursor.execute("SELECT current_balance FROM profit_tracking ORDER BY id DESC LIMIT 1")
    return cursor.fetchone()[0]


def simulate_trade(symbol, action, spot_price, futures_price):
    """simulation of a trading operation taking into account commissions"""
    current_balance = get_current_balance()

    # Position size in base currency including commissions
    quantity = current_balance / (spot_price * (1 + SPOT_FEE))

    # Calculating net profit
    if action == 'BUY_SPOT':
        # Direct arbitrage: Long spot, Short futures
        cost_spot = spot_price * quantity * (1 + SPOT_FEE)
        revenue_futures = futures_price * quantity * (1 - FUTURES_FEE)
        net_profit = revenue_futures - cost_spot

    elif action == 'BUY_FUTURES':
        # Reverse Arbitrage: Short Spot, Long Futures
        revenue_spot = spot_price * quantity * (1 - SPOT_FEE)
        cost_futures = futures_price * quantity * (1 + FUTURES_FEE)
        net_profit = revenue_spot - cost_futures

    else:
        return 0

    return round(net_profit, 4)


def update_profit_tracking(profit_change=0):
    """Profit information update"""
    # We receive the latest balance
    cursor.execute("SELECT current_balance FROM profit_tracking ORDER BY id DESC LIMIT 1")
    last_balance = cursor.fetchone()[0]

    # Calculate a new balance with protection against negative values
    new_balance = max(10.0, last_balance + profit_change)

    # We calculate profit indicators
    profit_usd = new_balance - STARTING_BALANCE
    profit_percent = (profit_usd / STARTING_BALANCE) * 100 if STARTING_BALANCE > 0 else 0

    # Calculating periodic profit
    now = datetime.utcnow()

    # Daily profit
    daily_start = now - timedelta(days=1)
    cursor.execute("""
                   SELECT current_balance
                   FROM profit_tracking
                   WHERE timestamp >= %s
                   ORDER BY timestamp ASC
                       LIMIT 1
                   """, (daily_start,))
    daily_row = cursor.fetchone()
    daily_profit = (new_balance - daily_row[0]) if daily_row else 0

    # Weekly profit
    weekly_start = now - timedelta(weeks=1)
    cursor.execute("""
                   SELECT current_balance
                   FROM profit_tracking
                   WHERE timestamp >= %s
                   ORDER BY timestamp ASC
                       LIMIT 1
                   """, (weekly_start,))
    weekly_row = cursor.fetchone()
    weekly_profit = (new_balance - weekly_row[0]) if weekly_row else 0

    # Monthly profit
    monthly_start = now - timedelta(days=30)
    cursor.execute("""
                   SELECT current_balance
                   FROM profit_tracking
                   WHERE timestamp >= %s
                   ORDER BY timestamp ASC
                   LIMIT 1
                   """, (monthly_start,))
    monthly_row = cursor.fetchone()
    monthly_profit = (new_balance - monthly_row[0]) if monthly_row else 0


    cursor.execute("""
                   INSERT INTO profit_tracking (timestamp, current_balance, profit_usd, profit_percent, daily_profit, weekly_profit, monthly_profit)
                   VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT 
                   ON CONSTRAINT profit_tracking_unique DO
                   UPDATE SET
                       timestamp = EXCLUDED.timestamp,
                       current_balance = EXCLUDED.current_balance,
                       profit_usd = EXCLUDED.profit_usd,
                       profit_percent = EXCLUDED.profit_percent,
                       daily_profit = EXCLUDED.daily_profit,
                       weekly_profit = EXCLUDED.weekly_profit,
                       monthly_profit = EXCLUDED.monthly_profit
                   """, (now, new_balance, profit_usd, profit_percent, daily_profit, weekly_profit, monthly_profit))
    conn.commit()

    print(f"\n💹 Balance updated: {new_balance:.2f} USDT | "
          f"Profit: {profit_usd:.2f} USDT ({profit_percent:.2f}%) | "
          f"Daily: {daily_profit:.2f} | Weekly: {weekly_profit:.2f} | Monthly: {monthly_profit:.2f}\n")


def analyze_and_store(symbol, threshold):
    """Spread analysis and saving data to the database."""
    spot_price, futures_price = fetch_prices(symbol)
    if spot_price is None or futures_price is None:
        return

    spread = futures_price - spot_price
    spread_percent = (spread / spot_price) * 100
    timestamp = datetime.utcnow()

    if spread_percent > threshold:
        action = 'BUY_SPOT'
    elif spread_percent < -threshold:
        action = 'BUY_FUTURES'
    else:
        action = 'HOLD'

    print(f" {symbol} | Spread: {spread_percent:.4f}% | Action: {action}")

    # We calculate profit only for trading actions
    profit = 0
    if action != 'HOLD':
        profit = simulate_trade(symbol, action, spot_price, futures_price)

        update_profit_tracking(profit)

    cursor.execute("""
                   INSERT INTO arbitrage_opportunities (timestamp, symbol, spot_price, futures_price,
                                                        spread_percent, action, trade_executed, profit_usd)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT
                   ON CONSTRAINT arbitrage_opportunities_unique DO
                   UPDATE SET
                       spot_price = EXCLUDED.spot_price,
                       futures_price = EXCLUDED.futures_price,
                       spread_percent = EXCLUDED.spread_percent,
                       action = EXCLUDED.action,
                       trade_executed = EXCLUDED.trade_executed,
                       profit_usd = EXCLUDED.profit_usd;
                   """, (
                       timestamp, symbol, spot_price, futures_price,
                       spread_percent, action, action != 'HOLD', profit
                   ))

    conn.commit()


try:
    # Get a list of all trading pairs with USDT
    exchange_info = client.get_exchange_info()
    symbols = [s['symbol'] for s in exchange_info['symbols']
               if s['quoteAsset'] == 'USDT' and 'BUSD' not in s['symbol']]

    # filter only liquid pairs
    liquid_symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT',
                      'ADAUSDT', 'DOGEUSDT', 'DOTUSDT', 'AVAXUSDT', 'LINKUSDT']
    symbols = [s for s in symbols if s in liquid_symbols]

    print(f"Start of monitoring {len(symbols)} trading pairs")

    cycle_count = 0
    while True:
        start_time = time.time()
        cycle_count += 1
        print(f"\n🔁 Cycle #{cycle_count} has begun")

        for symbol in symbols:
            try:
                analyze_and_store(symbol, THRESHOLD)
                time.sleep(0.3)  # Delay to comply with API limits
            except Exception as e:
                print(f"Error while processing {symbol}: {str(e)}")

        # We update the balance even without transactions
        update_profit_tracking(0)

        print(f"\n🔄 Cycle #{cycle_count} Completed")
        time.sleep(1)

except KeyboardInterrupt:
    print("Completion of work.")
except Exception as e:
    print(f"Critical error: {str(e)}")
finally:
    cursor.close()
    conn.close()
    print("The database connection was closed.")


"""inch petqa anenq
1. hima es filter em anum ev mena 9 coin em tuyl talis ashxatacnel karanq toxenq saxi vra ani bayc liquidity hashvenq vor slippage ch@ lini
2. orderner@ idealakan chen ashxatum chene kara petqa binance-ov order anenq vor tenanq vonca ashxatum hima da fake-a
3. code@ refactoring petqa anenq ev normal structure sarqenq
"""
