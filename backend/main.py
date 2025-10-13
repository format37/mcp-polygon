import contextlib
import contextvars
import logging
import os
import re
from typing import Any, Optional, List, Dict
import sentry_sdk
import uvicorn
from starlette.applications import Starlette
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.routing import Mount
from mcp.server.fastmcp import FastMCP
from polygon_data_fetcher import (
    fetch_market_status
)
from mcp_service import format_csv_response
from polygon import RESTClient
from polygon_tools.news import register_polygon_news
from polygon_tools.ticker_details import register_polygon_ticker_details
from polygon_tools.price_data import register_polygon_price_data
from polygon_tools.crypto_aggregates import register_polygon_crypto_aggregates
from polygon_tools.crypto_conditions import register_polygon_crypto_conditions
from polygon_tools.crypto_daily_open_close import register_polygon_crypto_daily_open_close
from polygon_tools.crypto_exchanges import register_polygon_crypto_exchanges
from polygon_tools.crypto_grouped_daily import register_polygon_crypto_grouped_daily
from polygon_tools.crypto_previous_close import register_polygon_crypto_previous_close
from polygon_tools.crypto_snapshots import register_polygon_crypto_snapshots
from polygon_tools.crypto_last_trade import register_polygon_crypto_last_trade
from polygon_tools.market_holidays import register_polygon_market_holidays

# Python execution imports
import io
import sys
import time
import uuid
import pathlib
import traceback
import signal
from contextlib import redirect_stdout, redirect_stderr

logger = logging.getLogger(__name__)

# Initialize Sentry if DSN is provided
sentry_dsn = os.getenv("SENTRY_DSN")
if sentry_dsn:
    logger.info(f"Initializing Sentry with DSN: {sentry_dsn[:20]}... (truncated for security)")
    sentry_sdk.init(
        dsn=sentry_dsn,
        # Enable logs to be sent to Sentry
        enable_logs=True,
    )
    logger.info("Sentry initialized successfully")
else:
    logger.info("Sentry DSN not provided, running without Sentry")

MCP_TOKEN_CTX = contextvars.ContextVar("mcp_token", default=None)

# Initialize FastMCP using MCP_NAME (env) for tool name and base path
# Ensure the streamable path ends with '/'
MCP_NAME = os.getenv("MCP_NAME", "polygon")
_safe_name = re.sub(r"[^a-z0-9_-]", "-", MCP_NAME.lower()).strip("-") or "service"
BASE_PATH = f"/{_safe_name}"
STREAM_PATH = f"{BASE_PATH}/"
ENV_PREFIX = re.sub(r"[^A-Z0-9_]", "_", _safe_name.upper())
logger.info(f"Safe service name: {_safe_name}")
logger.info(f"Stream path: {STREAM_PATH}")


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning("Invalid %s=%r; using %s", name, value, default)
        return default


def _sanitize_filename(name: str) -> str:
    sanitized = re.sub(r"[^a-zA-Z0-9_.-]", "-", name).strip("-.")
    return sanitized or "script"


# Polygon API configuration
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")

# Global MCP instance - will be initialized
# mcp_instance = None
# Initialize Polygon client if API key is available
polygon_client = None
if POLYGON_API_KEY:
    try:
        logger.info(f"POLYGON_API_KEY: {POLYGON_API_KEY[:5]}... (truncated for security)")
        polygon_client = RESTClient(POLYGON_API_KEY)
        logger.info("Polygon RESTClient initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Polygon RESTClient: {e}")
        polygon_client = None
else:
    logger.warning("POLYGON_API_KEY not provided, using mock data")

mcp = FastMCP(_safe_name, streamable_http_path=STREAM_PATH, json_response=True)

# CSV storage directory (hardcoded for simplicity)
CSV_DIR = pathlib.Path("data/mcp-polygon")
CSV_DIR.mkdir(parents=True, exist_ok=True)

# Polygon MCP tools
register_polygon_news(mcp, polygon_client, CSV_DIR)
register_polygon_ticker_details(mcp, polygon_client, CSV_DIR)
register_polygon_price_data(mcp, polygon_client, CSV_DIR)
register_polygon_crypto_conditions(mcp, polygon_client, CSV_DIR)
register_polygon_crypto_daily_open_close(mcp, polygon_client, CSV_DIR)
register_polygon_crypto_exchanges(mcp, polygon_client, CSV_DIR)
register_polygon_crypto_grouped_daily(mcp, polygon_client, CSV_DIR)
register_polygon_crypto_previous_close(mcp, polygon_client, CSV_DIR)
register_polygon_crypto_snapshots(mcp, polygon_client, CSV_DIR)
register_polygon_crypto_aggregates(mcp, polygon_client, CSV_DIR)
register_polygon_crypto_last_trade(mcp, polygon_client, CSV_DIR)
register_polygon_market_holidays(mcp, polygon_client, CSV_DIR)

def _posix_time_limit(seconds: float):
    """POSIX-only wall clock timeout using signals; noop elsewhere."""
    class _TL:
        def __enter__(self_):
            self_.posix = (os.name == "posix" and hasattr(signal, "setitimer"))
            if not self_.posix:
                return
            self_.old_handler = signal.getsignal(signal.SIGALRM)
            def _raise(_sig, _frm):
                raise TimeoutError("time limit exceeded")
            signal.signal(signal.SIGALRM, _raise)
            signal.setitimer(signal.ITIMER_REAL, float(seconds))
        def __exit__(self_, exc_type, exc, tb):
            if self_.posix:
                signal.setitimer(signal.ITIMER_REAL, 0.0)
                signal.signal(signal.SIGALRM, self_.old_handler)
            return False
    return _TL()

# Add custom error handling for stream disconnections
original_logger = logging.getLogger("mcp.server.streamable_http")

class StreamErrorFilter(logging.Filter):
    def filter(self, record):
        # Suppress ClosedResourceError logs as they're expected when clients disconnect
        if "ClosedResourceError" in str(record.getMessage()):
            return False
        return True

original_logger.addFilter(StreamErrorFilter())

@mcp.tool()
def py_eval(code: str, timeout_sec: float = 5.0) -> Dict[str, Any]:
    """
    Execute Python code with pandas/numpy pre-loaded and access to CSV folder.

    Parameters:
        code (str): Python code to execute
        timeout_sec (float): Execution timeout in seconds (default: 5.0)

    Returns:
        dict: Execution results with stdout, stderr, duration_ms, and error info

    Available variables in execution environment:
        - pd: pandas library
        - np: numpy library
        - CSV_PATH: path to data/mcp-polygon folder for reading/writing CSV files
    """
    logger.info(f"py_eval invoked with {len(code)} characters of code")

    # Capture output
    buf_out, buf_err = io.StringIO(), io.StringIO()
    started = time.time()

    try:
        # Import scientific libraries in execution environment
        import pandas as pd
        import numpy as np

        # Create execution environment
        env = {
            "__builtins__": __builtins__,
            "pd": pd,
            "np": np,
            "CSV_PATH": str(CSV_DIR),
        }

        with redirect_stdout(buf_out), redirect_stderr(buf_err), _posix_time_limit(timeout_sec):
            exec(code, env, env)
        ok, error = True, None

    except TimeoutError as e:
        ok, error = False, f"Timeout: {e}"
    except Exception:
        ok, error = False, traceback.format_exc()

    duration_ms = int((time.time() - started) * 1000)

    result = {
        "ok": ok,
        "stdout": buf_out.getvalue(),
        "stderr": buf_err.getvalue(),
        "error": error,
        "duration_ms": duration_ms,
        "csv_path": str(CSV_DIR)
    }

    logger.info(f"py_eval completed: ok={ok}, duration={duration_ms}ms")
    return result


@mcp.tool()
def polygon_market_status() -> str:
    """
    Fetch current market status across all exchanges and asset classes, and save to CSV file.

    This endpoint retrieves the current trading status for various exchanges and overall financial
    markets. It provides real-time indicators of whether markets are open, closed, or operating in
    pre-market/after-hours sessions, along with timing details for the current state.

    Parameters:
        None (endpoint automatically returns current status snapshot)

    Returns:
        str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

    CSV Output Structure:
        - fetched_at (str): Local timestamp when this data was fetched in 'YYYY-MM-DD HH:MM:SS' format
        - server_time (str): API server time in RFC3339 format (e.g., "2020-11-10T17:37:37-05:00")
            Use this as the authoritative timestamp for market status
        - market (str): Overall market status across all exchanges
            Values: "open", "closed", "extended-hours", "early-hours"
        - after_hours (bool): Whether markets are in post-market hours (after normal trading)
        - early_hours (bool): Whether markets are in pre-market hours (before normal trading)
        - crypto_status (str): Status of cryptocurrency markets
            Values: "open", "closed"
            Note: Crypto markets typically trade 24/7, so usually "open"
        - fx_status (str): Status of foreign exchange (forex) markets
            Values: "open", "closed"
            Note: FX markets trade nearly 24/7 on weekdays
        - nasdaq_status (str): Status of NASDAQ exchange
            Values: "open", "closed", "extended-hours", "early-close"
        - nyse_status (str): Status of New York Stock Exchange
            Values: "open", "closed", "extended-hours", "early-close"
        - otc_status (str): Status of Over-The-Counter markets
            Values: "open", "closed"
        - index_cccy (str): Status of Cboe Streaming Market Indices Cryptocurrency ("CCCY") indices
        - index_cgi (str): Status of Cboe Global Indices ("CGI") trading hours
        - index_dow_jones (str): Status of Dow Jones indices trading hours
        - index_ftse_russell (str): Status of FTSE Russell indices trading hours
        - index_msci (str): Status of MSCI indices trading hours
        - index_mstar (str): Status of Morningstar indices trading hours
        - index_mstarc (str): Status of Morningstar Customer indices trading hours
        - index_nasdaq (str): Status of Nasdaq indices trading hours
        - index_s_and_p (str): Status of S&P indices trading hours
        - index_societe_generale (str): Status of Societe Generale indices trading hours

    Use Cases:
        - **Real-Time Monitoring**: Check if markets are currently open for trading
        - **Algorithm Scheduling**: Determine when to run trading algorithms based on market hours
        - **UI Updates**: Display current market status to users in trading applications
        - **Operational Planning**: Schedule system maintenance during market closures
        - **Trading Strategy**: Adjust crypto trading strategies based on traditional market hours
        - **Order Validation**: Prevent order submission when target markets are closed
        - **Risk Management**: Understand when gaps may occur due to market closures
        - **Crypto vs Traditional Markets**: Coordinate crypto trading with traditional market status
        - **International Trading**: Track multiple exchange statuses for global trading operations
        - **Market Hours Analysis**: Analyze patterns of market openings and closings

    Important Notes:
        - This endpoint returns a single snapshot of the current status (one row)
        - Status changes throughout the trading day (pre-market → open → after-hours → closed)
        - **Crypto markets trade 24/7** - crypto_status is typically always "open"
        - Traditional stock markets (NYSE, NASDAQ) have specific hours (typically 9:30 AM - 4:00 PM ET)
        - Extended hours trading occurs before and after regular market hours
        - Use server_time as the authoritative timestamp (not fetched_at)
        - Call this endpoint regularly to monitor real-time market status changes
        - Combine with polygon_market_holidays to understand upcoming closures

    Trading Strategy Considerations:
        - **Crypto Trading**: Crypto markets are open 24/7, but liquidity/volatility varies when traditional markets close
        - **Volatility**: Markets often experience higher volatility at open and close
        - **Liquidity**: Extended hours typically have lower volume and wider spreads
        - **Gap Risk**: Markets closed overnight can create price gaps at next open
        - **Coordination**: Some crypto traders adjust strategies based on traditional market hours

    Example Workflow:
        1. Call this tool to fetch current market status
        2. Receive CSV filename in response
        3. Use py_eval to load and analyze the CSV file
        4. Check specific market statuses (crypto_status, nyse_status, etc.)
        5. Make trading decisions based on which markets are currently active
        6. Integrate into automated systems for real-time market monitoring

    Integration with Other Tools:
        - Use with polygon_market_holidays to plan around upcoming closures
        - Combine with polygon_crypto_aggregates for 24/7 crypto market analysis
        - Coordinate with polygon_news to understand market-moving events
        - Use with trading algorithms to ensure orders are placed during market hours

    Always use py_eval tool to analyze the saved CSV file for market status checks and trading decisions.
    """
    logger.info("polygon_market_status invoked")

    try:
        # Fetch market status
        df = fetch_market_status(output_dir=None)

        if df.empty:
            return "No market status data was retrieved. The endpoint may be temporarily unavailable."

        # Always save to CSV file
        filename = f"market_status_{uuid.uuid4().hex[:8]}.csv"
        filepath = CSV_DIR / filename
        df.to_csv(filepath, index=False)
        logger.info(f"Saved market status to {filename}")

        # Return formatted response
        return format_csv_response(filepath, df)

    except Exception as e:
        logger.error(f"Error in polygon_market_status: {e}")
        return f"Error fetching market status: {str(e)}"


@mcp.resource(
    f"{_safe_name}://documentation",
    name="Polygon API Documentation",
    description="Documentation for Polygon.io financial market data API",
    mime_type="text/markdown"
)
def get_documentation_resource() -> str:
    """Expose Polygon API documentation as an MCP resource."""
    return """# Polygon API Documentation

## Available Tools

### polygon_news
Fetches market news from Polygon.io financial news aggregator.

**Parameters:**
- `start_date` (optional): Start date in 'YYYY-MM-DD' format
- `end_date` (optional): End date in 'YYYY-MM-DD' format
- `save_csv` (optional): If True, saves data to CSV file

**Returns:** Formatted table with datetime and topic columns, or CSV filename if save_csv=True.

### polygon_ticker_details
Fetch comprehensive ticker details including company info, market cap, sector, etc.

**Parameters:**
- `tickers` (required): List of ticker symbols (e.g., ['AAPL', 'MSFT'])
- `save_csv` (optional): If True, saves data to CSV file and returns filename

**Returns:** Success message with CSV filename or summary information.

### polygon_price_data
Fetch historical price data for tickers.

**Parameters:**
- `tickers` (required): List of ticker symbols
- `from_date` (optional): Start date in YYYY-MM-DD format (defaults to 30 days ago)
- `to_date` (optional): End date in YYYY-MM-DD format (defaults to today)
- `timespan` (optional): Time span (day, week, month, quarter, year)
- `multiplier` (optional): Size of the time window
- `save_csv` (optional): If True, saves data to CSV file and returns filename

**Returns:** Success message with CSV filename or summary information.

### polygon_price_metrics
Calculate price-based metrics for tickers (volatility, returns, etc.).

**Parameters:**
- `tickers` (required): List of ticker symbols
- `from_date` (optional): Start date in YYYY-MM-DD format (defaults to 30 days ago)
- `to_date` (optional): End date in YYYY-MM-DD format (defaults to today)
- `save_csv` (optional): If True, saves data to CSV file and returns filename

**Returns:** Success message with CSV filename or summary information.

---

## py_eval Tool - Python Data Analysis

Execute Python code with pandas/numpy pre-loaded and access to CSV folder.

**Parameters:**
- `code` (required): Python code to execute
- `timeout_sec` (optional): Execution timeout in seconds (default: 5.0)

**⚠️ IMPORTANT NOTES:**
- **Variable Persistence**: Each py_eval call starts fresh - variables do NOT persist between calls
- **Reload Data**: You must reload CSV files in each py_eval call that needs them
- **Temporary Files**: You can save temporary files within a py_eval session and read them back in the same call
- **Libraries**: pandas (pd), numpy (np) are pre-loaded. Standard library modules available.

### Available Variables
- `pd`: pandas library (version 2.2.3+)
- `np`: numpy library (version 1.26.4+)
- `CSV_PATH`: string path to `/work/csv/` folder containing all CSV files

### CSV File Types and Structures

#### 1. Price Data Files (`price_data_*.csv`)
**Columns:**
- `ticker` (str): Stock ticker symbol
- `timestamp` (str): Trading session timestamp 'YYYY-MM-DD HH:MM:SS'
- `open`, `high`, `low`, `close` (float): OHLC prices
- `volume` (float): Trading volume
- `vwap` (float): Volume Weighted Average Price
- `transactions` (int): Number of transactions
- `date` (str): Trading date 'YYYY-MM-DD'

#### 2. Ticker Details Files (`ticker_details_*.csv`)
**Key Columns:**
- `ticker` (str): Stock ticker symbol
- `name` (str): Company full name
- `market_cap` (float): Market capitalization in USD
- `total_employees` (int): Number of employees
- `description` (str): Company description
- `homepage_url` (str): Company website
- `sic_code` (int): Standard Industrial Classification
- `sic_description` (str): Industry sector description
- Plus 20+ additional company details columns

#### 3. Price Metrics Files (`price_metrics_*.csv`)
**Columns:**
- `ticker` (str): Stock ticker symbol
- `start_date`, `end_date` (str): Analysis period 'YYYY-MM-DD'
- `trading_days` (int): Number of trading days
- `start_price`, `end_price` (float): Period start/end prices
- `high_price`, `low_price` (float): Period high/low prices
- `total_return` (float): Total return as decimal (0.12 = 12%)
- `volatility` (float): Price volatility (std dev of daily returns)
- `avg_daily_volume`, `total_volume` (float): Volume metrics
- `price_range_ratio` (float): (High-Low)/Low ratio

#### 4. News Files (`news_*.csv`)
**Columns:**
- `datetime` (str): Publication date/time 'YYYY-MM-DD HH:MM:SS'
- `topic` (str): News headline/title

---

## Python Analysis Examples

### 1. CSV File Discovery and Loading

```python
import os

# List all CSV files
print("=== CSV FILES AVAILABLE ===")
files = [f for f in os.listdir(CSV_PATH) if f.endswith('.csv')]
print(f"Found {len(files)} CSV files:")

# Group by type
price_data_files = [f for f in files if 'price_data_' in f]
ticker_files = [f for f in files if 'ticker_details_' in f]
metrics_files = [f for f in files if 'price_metrics_' in f]
news_files = [f for f in files if 'news_' in f]

print(f"Price data: {len(price_data_files)} files")
print(f"Ticker details: {len(ticker_files)} files")
print(f"Price metrics: {len(metrics_files)} files")
print(f"News: {len(news_files)} files")

# Load the most recent files (if they exist)
if price_data_files:
    price_df = pd.read_csv(f'{CSV_PATH}/{price_data_files[-1]}')
    print(f"\nLoaded price data: {price_df.shape} - {list(price_df.columns)}")

if ticker_files:
    ticker_df = pd.read_csv(f'{CSV_PATH}/{ticker_files[-1]}')
    print(f"Loaded ticker details: {ticker_df.shape}")
```

### 2. Portfolio Performance Analysis

```python
# Reload data (required in each py_eval call)
import os
files = [f for f in os.listdir(CSV_PATH) if f.endswith('.csv')]
metrics_files = [f for f in files if 'price_metrics_' in f]
ticker_files = [f for f in files if 'ticker_details_' in f]

if metrics_files and ticker_files:
    # Load latest files
    metrics_df = pd.read_csv(f'{CSV_PATH}/{metrics_files[-1]}')
    ticker_df = pd.read_csv(f'{CSV_PATH}/{ticker_files[-1]}')

    # Risk-adjusted performance analysis
    metrics_df['risk_adj_return'] = metrics_df['total_return'] / metrics_df['volatility']

    print("=== TOP PERFORMERS (Risk-Adjusted) ===")
    top_performers = metrics_df.nlargest(5, 'risk_adj_return')
    for _, stock in top_performers.iterrows():
        print(f"{stock['ticker']}: Return={stock['total_return']*100:.1f}%, "
              f"Vol={stock['volatility']*100:.1f}%, Risk-Adj={stock['risk_adj_return']:.2f}")

    # Market cap analysis
    combined = pd.merge(metrics_df, ticker_df[['ticker', 'market_cap']], on='ticker', how='left')
    combined['market_cap_billions'] = combined['market_cap'] / 1e9

    print("\n=== LARGE CAP PERFORMANCE ===")
    large_caps = combined[combined['market_cap_billions'] > 1000]
    large_caps_sorted = large_caps.sort_values('total_return', ascending=False)
    print(large_caps_sorted[['ticker', 'market_cap_billions', 'total_return']].round(3))
```

### 3. Technical Analysis with Price Data

```python
# Load price data for technical analysis
files = [f for f in os.listdir(CSV_PATH) if f.endswith('.csv')]
price_files = [f for f in files if 'price_data_' in f]

if price_files:
    df = pd.read_csv(f'{CSV_PATH}/{price_files[-1]}')

    # Convert date column to datetime
    df['date'] = pd.to_datetime(df['date'])

    print("=== TECHNICAL ANALYSIS ===")
    for ticker in df['ticker'].unique():
        ticker_data = df[df['ticker'] == ticker].sort_values('date')

        # Calculate moving averages
        ticker_data['sma_5'] = ticker_data['close'].rolling(window=5).mean()
        ticker_data['sma_20'] = ticker_data['close'].rolling(window=20).mean()

        # Calculate daily returns
        ticker_data['daily_return'] = ticker_data['close'].pct_change()

        # Recent metrics
        recent_return = ticker_data['daily_return'].tail(5).mean() * 100
        current_price = ticker_data['close'].iloc[-1]
        sma5 = ticker_data['sma_5'].iloc[-1]
        sma20 = ticker_data['sma_20'].iloc[-1]

        print(f"{ticker}: Price=${current_price:.2f}, 5-day avg return={recent_return:.2f}%")
        if sma5 > sma20:
            print(f"  ↗️ Bullish: SMA5 (${sma5:.2f}) > SMA20 (${sma20:.2f})")
        else:
            print(f"  ↘️ Bearish: SMA5 (${sma5:.2f}) < SMA20 (${sma20:.2f})")
```

### 4. News Sentiment and Timeline Analysis

```python
# Load and analyze news data
files = [f for f in os.listdir(CSV_PATH) if f.endswith('.csv')]
news_files = [f for f in files if 'news_' in f]

if news_files:
    news_df = pd.read_csv(f'{CSV_PATH}/{news_files[-1]}')
    news_df['datetime'] = pd.to_datetime(news_df['datetime'])

    print("=== NEWS ANALYSIS ===")
    print(f"Total news articles: {len(news_df)}")

    # Daily news volume
    news_df['date'] = news_df['datetime'].dt.date
    daily_counts = news_df.groupby('date').size().sort_values(ascending=False)
    print(f"\nBusiest news days:")
    print(daily_counts.head())

    # Keyword analysis
    keywords = ['Fed', 'interest', 'earnings', 'AI', 'tech', 'market']
    print(f"\nKeyword frequency:")
    for keyword in keywords:
        count = news_df['topic'].str.contains(keyword, case=False, na=False).sum()
        print(f"  {keyword}: {count} mentions")

    # Recent headlines
    print(f"\nMost recent headlines:")
    recent_news = news_df.nlargest(5, 'datetime')
    for _, article in recent_news.iterrows():
        print(f"  {article['datetime'].strftime('%m/%d %H:%M')}: {article['topic'][:80]}...")
```

### 5. Saving and Loading Temporary Analysis Files

```python
# Create combined analysis and save as temp file for later use
files = [f for f in os.listdir(CSV_PATH) if f.endswith('.csv')]
metrics_files = [f for f in files if 'price_metrics_' in f]
ticker_files = [f for f in files if 'ticker_details_' in f]

if metrics_files and ticker_files:
    metrics_df = pd.read_csv(f'{CSV_PATH}/{metrics_files[-1]}')
    ticker_df = pd.read_csv(f'{CSV_PATH}/{ticker_files[-1]}')

    # Create comprehensive analysis
    analysis = pd.merge(metrics_df,
                       ticker_df[['ticker', 'name', 'market_cap', 'total_employees', 'sic_description']],
                       on='ticker', how='left')

    # Add calculated fields
    analysis['market_cap_billions'] = analysis['market_cap'] / 1e9
    analysis['risk_adj_return'] = analysis['total_return'] / analysis['volatility']
    analysis['return_pct'] = analysis['total_return'] * 100
    analysis['employees_per_billion_cap'] = analysis['total_employees'] / analysis['market_cap_billions']

    # Save temporary analysis file
    temp_file = f'{CSV_PATH}/temp_comprehensive_analysis.csv'
    analysis.to_csv(temp_file, index=False)
    print(f"✅ Saved comprehensive analysis to: temp_comprehensive_analysis.csv")
    print(f"   Shape: {analysis.shape}")
    print(f"   Columns: {list(analysis.columns)}")

    # Demonstrate reading it back in same session
    reloaded = pd.read_csv(temp_file)
    print(f"✅ Successfully reloaded: {reloaded.shape}")

    # Show summary
    print("\n=== COMPREHENSIVE ANALYSIS SUMMARY ===")
    summary = reloaded.nlargest(3, 'risk_adj_return')[['ticker', 'name', 'return_pct', 'market_cap_billions']]
    print(summary.round(2))
```

### 6. Error Handling and Robust File Loading

```python
# Robust file loading with error handling
def load_latest_csv(file_pattern):
    \"\"\"Load the most recent CSV file matching the pattern\"\"\"
    try:
        files = [f for f in os.listdir(CSV_PATH) if file_pattern in f and f.endswith('.csv')]
        if not files:
            print(f"❌ No files found matching pattern: {file_pattern}")
            return None

        latest_file = sorted(files)[-1]  # Get the last one alphabetically
        df = pd.read_csv(f'{CSV_PATH}/{latest_file}')
        print(f"✅ Loaded {latest_file}: {df.shape}")
        return df

    except Exception as e:
        print(f"❌ Error loading {file_pattern}: {e}")
        return None

# Use robust loading
price_data = load_latest_csv('price_data_')
ticker_details = load_latest_csv('ticker_details_')
price_metrics = load_latest_csv('price_metrics_')

# Only proceed if we have the required data
if price_metrics is not None:
    print("\n=== PORTFOLIO RISK ANALYSIS ===")

    # Volatility analysis
    high_vol = price_metrics[price_metrics['volatility'] > price_metrics['volatility'].mean()]
    low_vol = price_metrics[price_metrics['volatility'] <= price_metrics['volatility'].mean()]

    print(f"High volatility stocks ({len(high_vol)}):")
    for _, stock in high_vol.iterrows():
        print(f"  {stock['ticker']}: {stock['volatility']*100:.1f}% volatility, {stock['total_return']*100:.1f}% return")

    print(f"\\nLow volatility stocks ({len(low_vol)}):")
    for _, stock in low_vol.iterrows():
        print(f"  {stock['ticker']}: {stock['volatility']*100:.1f}% volatility, {stock['total_return']*100:.1f}% return")
```

---

## Best Practices

1. **Always reload data** - Start each py_eval with file discovery and loading
2. **Use error handling** - Check if files exist before loading
3. **Save intermediate results** - Use temporary CSV files for complex multi-step analysis
4. **Keep analysis focused** - Each py_eval call should accomplish one analytical goal
5. **Print progress** - Use print statements to show what your code is doing
6. **Combine datasets** - Merge price_metrics with ticker_details for comprehensive analysis
"""

# Build the main ASGI app with Streamable HTTP mounted
mcp_asgi = mcp.streamable_http_app()

@contextlib.asynccontextmanager
async def lifespan(_: Starlette):
    # Ensure FastMCP session manager is running, as required by Streamable HTTP
    async with mcp.session_manager.run():
        yield

async def health_check(request):
    return JSONResponse({"status": "healthy", "service": "polygon-mcp"})

app = Starlette(
    routes=[
        # Mount at root; internal app handles service path routing
        Mount("/", app=mcp_asgi),
    ],
    lifespan=lifespan,
)

# Add health endpoint before auth middleware
from starlette.routing import Route
app.routes.insert(0, Route("/health", health_check, methods=["GET"]))


class TokenAuthMiddleware(BaseHTTPMiddleware):
    """Simple token gate for all service requests under BASE_PATH.

    Accepts tokens via Authorization header: "Bearer <token>" (default and recommended).
    If env {ENV_PREFIX}_ALLOW_URL_TOKENS=true, also accepts:
      - Query parameter: ?token=<token>
      - URL path form: {BASE_PATH}/<token>/... (token is stripped before forwarding)

    Configure allowed tokens via env var {ENV_PREFIX}_TOKENS (comma-separated). If unset or empty,
    authentication is disabled (allows all) but logs a warning.
    """

    def __init__(self, app):
        super().__init__(app)
        # Prefer envs derived from MCP_NAME; fall back to legacy CBONDS_* names for backward compatibility
        raw = os.getenv(f"MCP_TOKENS", "")
        self.allowed_tokens = {t.strip() for t in raw.split(",") if t.strip()}
        self.allow_url_tokens = (
            os.getenv(f"MCP_ALLOW_URL_TOKENS", "").lower()
            in ("1", "true", "yes")
        )
        self.require_auth = (
            os.getenv(f"MCP_REQUIRE_AUTH", "").lower()
            in ("1", "true", "yes")
        )
        if not self.allowed_tokens:
            if self.require_auth:
                logger.warning(
                    "%s is not set; %s=true -> all %s requests will be rejected (401)",
                    f"MCP_TOKENS",
                    f"MCP_REQUIRE_AUTH",
                    BASE_PATH,
                )
            else:
                logger.warning(
                    "%s is not set; token auth is DISABLED for %s endpoints",
                    f"MCP_TOKENS",
                    BASE_PATH,
                )

    async def dispatch(self, request, call_next):
        # Only protect BASE_PATH path space
        path = request.url.path or "/"
        if not path.startswith(BASE_PATH):
            return await call_next(request)

        def accept(token_value: str, source: str):
            request.state.mcp_token = token_value  # stash for downstream use
            logger.info(
                "Authenticated %s %s via %s token %s",
                request.method,
                path,
                source,
                token_value,
            )
            return MCP_TOKEN_CTX.set(token_value)

        async def proceed(token_value: str, source: str):
            token_scope = accept(token_value, source)
            try:
                return await call_next(request)
            finally:
                MCP_TOKEN_CTX.reset(token_scope)

        # If auth is not required, always allow
        if not self.require_auth:
            logger.info("Auth disabled, allowing request to %s", path)
            return await call_next(request)

        # If no tokens configured but auth is required
        if not self.allowed_tokens:
            return JSONResponse({"detail": "Unauthorized"}, status_code=401, headers={"WWW-Authenticate": "Bearer"})

        # Authorization: Bearer <token>
        token = None
        auth = request.headers.get("authorization") or request.headers.get("Authorization")
        if auth and auth.lower().startswith("bearer "):
            token = auth.split(" ", 1)[1].strip()

        # Header token valid -> allow
        if token and token in self.allowed_tokens:
            return await proceed(token, "header")

        # If URL tokens are allowed, check query and path variants
        if self.allow_url_tokens:
            # 1) Query parameter ?token=...
            url_token = request.query_params.get("token")
            if url_token and url_token in self.allowed_tokens:
                return await proceed(url_token, "query")

            # 2) Path segment /<service>/<token>/...
            segs = [s for s in path.split("/") if s != ""]
            if len(segs) >= 2 and segs[0] == _safe_name:
                candidate = segs[1]
                if candidate in self.allowed_tokens:
                    # Rebuild path without the token segment
                    remainder = "/".join([_safe_name] + segs[2:])
                    new_path = "/" + (remainder + "/" if path.endswith("/") and not remainder.endswith("/") else remainder)
                    if new_path == BASE_PATH:
                        new_path = STREAM_PATH
                    request.scope["path"] = new_path
                    if "raw_path" in request.scope:
                        request.scope["raw_path"] = new_path.encode("utf-8")
                    return await proceed(candidate, "path")

        # If we reached here, reject unauthorized
        if self.allow_url_tokens:
            detail = "Unauthorized"
        else:
            detail = "Use Authorization: Bearer <token>; URL/query tokens are not allowed"
        return JSONResponse({"detail": detail}, status_code=401, headers={"WWW-Authenticate": "Bearer"})


# Install auth middleware last to wrap the full app
app.add_middleware(TokenAuthMiddleware)

def main():
    """
    Run the uvicorn server without SSL (TLS handled by reverse proxy).
    """
    PORT = int(os.getenv("PORT", "8009"))

    logger.info(f"Starting {MCP_NAME} MCP server (HTTP) on port {PORT} at {STREAM_PATH}")

    uvicorn.run(
        app=app,
        host=os.getenv("HOST", "0.0.0.0"),
        port=PORT,
        log_level=os.getenv("LOG_LEVEL", "info"),
        access_log=True,
        # Behind Caddy: respect X-Forwarded-* and use https in redirects
        proxy_headers=True,
        forwarded_allow_ips="*",
        timeout_keep_alive=120,
    )

if __name__ == "__main__":
    main()
