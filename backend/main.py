import contextlib
import contextvars
import logging
import os
import re
from datetime import datetime, timedelta
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
    fetch_ticker_details,
    fetch_price_data,
    fetch_crypto_aggregates,
    fetch_crypto_conditions,
    fetch_crypto_daily_open_close,
    fetch_crypto_exchanges,
    fetch_crypto_grouped_daily_bars,
    calculate_price_metrics,
    create_combined_analysis,
    create_output_folder
)
from polygon import RESTClient

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

# Initialize Polygon client if API key is available
polygon_client = None
if POLYGON_API_KEY:
    try:
        polygon_client = RESTClient(POLYGON_API_KEY)
        logger.info("Polygon RESTClient initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Polygon RESTClient: {e}")
        polygon_client = None
else:
    logger.warning("POLYGON_API_KEY not provided, using mock data")

mcp = FastMCP(_safe_name, streamable_http_path=STREAM_PATH, json_response=True)

# CSV storage directory
CSV_DIR = pathlib.Path("data/mcp-polygon")
CSV_DIR.mkdir(parents=True, exist_ok=True)

def _format_csv_response(filepath: pathlib.Path, df: Any) -> str:
    """
    Generate standardized response format for CSV data files.

    Args:
        filepath: Path to the saved CSV file
        df: DataFrame that was saved

    Returns:
        Formatted string with file info, schema, sample data, and Python snippet
    """
    import json
    import pandas as pd

    # Get file size
    file_size_bytes = filepath.stat().st_size
    if file_size_bytes < 1024:
        size_str = f"{file_size_bytes} bytes"
    elif file_size_bytes < 1024 * 1024:
        size_str = f"{file_size_bytes / 1024:.1f} KB"
    else:
        size_str = f"{file_size_bytes / (1024 * 1024):.1f} MB"

    # Get filename only (relative to CSV_PATH)
    filename = filepath.name

    # Infer better data types for schema
    def infer_better_type(series):
        """Infer a more descriptive data type for a pandas Series."""
        # Remove nulls for analysis
        non_null = series.dropna()

        if len(non_null) == 0:
            return "string (empty)"

        # Check current dtype first
        dtype_str = str(series.dtype)

        # If already a good type, keep it
        if 'int' in dtype_str:
            return dtype_str
        if 'float' in dtype_str:
            return dtype_str
        if 'bool' in dtype_str:
            return 'boolean'
        if 'datetime' in dtype_str:
            return 'datetime'

        # Try to infer better types for 'object' columns
        if dtype_str == 'object':
            # Try boolean
            if non_null.isin([0, 1, '0', '1', True, False, 'True', 'False', 'true', 'false']).all():
                return 'boolean'

            # Try integer
            try:
                converted = pd.to_numeric(non_null, errors='raise')
                if (converted == converted.astype(int)).all():
                    return 'integer'
            except (ValueError, TypeError):
                pass

            # Try float
            try:
                pd.to_numeric(non_null, errors='raise')
                return 'float'
            except (ValueError, TypeError):
                pass

            # Try datetime
            try:
                pd.to_datetime(non_null, errors='raise')
                return 'datetime'
            except (ValueError, TypeError):
                pass

            return 'string'

        return dtype_str

    # Build schema JSON with inferred types
    schema = {col: infer_better_type(df[col]) for col in df.columns}
    schema_json = json.dumps(schema, indent=2)

    # Generate sample data (first row) as markdown table
    if len(df) > 0:
        sample_df = df.head(1)
        # Create markdown table manually for better control
        headers = list(sample_df.columns)
        values = [str(v) for v in sample_df.iloc[0].values]

        # Truncate long values for display
        values = [v[:50] + "..." if len(v) > 50 else v for v in values]

        # Build markdown table
        header_row = "| " + " | ".join(headers) + " |"
        separator = "|" + "|".join(["-" * (len(h) + 2) for h in headers]) + "|"
        value_row = "| " + " | ".join(values) + " |"

        sample_table = f"{header_row}\n{separator}\n{value_row}"
    else:
        sample_table = "(empty dataset)"

    # Create Python snippet
    python_snippet = f"""import pandas as pd
df = pd.read_csv('data/mcp-polygon/{filename}')
print(df.info())
print(df.head())"""

    # Build final response
    response = f"""✓ Data saved to CSV

File: {filename}
Rows: {len(df)}
Size: {size_str}

Schema (JSON):
{schema_json}

Sample (first row):
{sample_table}

Python snippet to load:
```python
{python_snippet}
```"""

    return response

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


def fetch_polygon_news_data(start_date: str = "", end_date: str = "", limit: int = 1000, ticker: str = "") -> list:
    """
    Fetch news data from Polygon.io API using RESTClient.

    Args:
        start_date (optional): Start date in YYYY-MM-DD format
        end_date (optional): End date in YYYY-MM-DD format
        limit (optional): Maximum number of results to fetch. Default is 1000.
        ticker (optional): Specific stock market ticker symbol to fetch news for (e.g., 'AAPL', 'MSFT')

    Returns:
        List of news articles with published_utc and description/title fields
    """
    if not polygon_client:
        logger.warning("Polygon client not available, returning empty news list")
        return []

    try:
        # Set default dates if not provided
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

        logger.info(f"Fetching news from {start_date} to {end_date}" + (f" for ticker {ticker}" if ticker else " (general market news)"))

        # Use the news endpoint to fetch articles
        news_articles = []

        # Fetch news for the date range
        try:
            news_data = polygon_client.list_ticker_news(
                ticker=ticker if ticker else None,  # Use specific ticker or None for general market news
                published_utc_gte=start_date,
                published_utc_lte=end_date,
                order="desc",
                limit=limit,
                sort="published_utc"
            )

            for article in news_data:
                news_articles.append({
                    'published_utc': article.published_utc,
                    'title': getattr(article, 'title', ''),
                    'description': getattr(article, 'description', ''),
                    'author': getattr(article, 'author', ''),
                    'article_url': getattr(article, 'article_url', ''),
                    'tickers': getattr(article, 'tickers', [])
                })

        except Exception as e:
            logger.warning(f"Error with ticker news endpoint, trying general news: {e}")
            # Fallback to general news endpoint if ticker news fails
            try:
                # Use a more general approach
                news_iter = polygon_client.list_ticker_news(limit=limit, order="desc", sort="published_utc")
                count = 0
                for article in news_iter:
                    if count >= limit:
                        break

                    # Filter by date range if articles have published_utc
                    article_date = getattr(article, 'published_utc', '')
                    if article_date:
                        article_datetime = datetime.fromisoformat(article_date.replace('Z', '+00:00'))
                        start_datetime = datetime.fromisoformat(f"{start_date}T00:00:00+00:00")
                        end_datetime = datetime.fromisoformat(f"{end_date}T23:59:59+00:00")

                        if start_datetime <= article_datetime <= end_datetime:
                            news_articles.append({
                                'published_utc': article.published_utc,
                                'title': getattr(article, 'title', ''),
                                'description': getattr(article, 'description', ''),
                                'author': getattr(article, 'author', ''),
                                'article_url': getattr(article, 'article_url', ''),
                                'tickers': getattr(article, 'tickers', [])
                            })
                    count += 1

            except Exception as e2:
                logger.error(f"Error fetching news with fallback method: {e2}")
                return []

        logger.info(f"Successfully fetched {len(news_articles)} news articles")
        return news_articles

    except Exception as e:
        logger.error(f"Error fetching news data: {e}")
        return []

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
def polygon_news(
    start_date: str = "",
    end_date: str = "",
    ticker: str = ""
) -> str:
    """
    Fetches market news from Polygon.io and saves to CSV file.

    Parameters:
        start_date (str, optional): The start date in 'YYYY-MM-DD' format.
            Defaults to 7 days before today if not provided.
        end_date (str, optional): The end date in 'YYYY-MM-DD' format.
            Defaults to today if not provided.
        ticker (str, optional): Specific ticker symbol to fetch news for (e.g., 'AAPL').
            If not provided, fetches general market news.

    Returns:
        str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

    CSV Output Structure:
        - datetime (str): Publication date and time in 'YYYY-MM-DD HH:MM:SS' format
        - topic (str): News article title/headline

    Use this data for: News sentiment analysis, market event tracking, timeline analysis.
    Always use py_eval tool to analyze the saved CSV file.
    """
    logger.info(f"polygon_news invoked: start_date={start_date}, end_date={end_date}, ticker={ticker}")

    # Try to fetch real data from Polygon API
    news_data = fetch_polygon_news_data(start_date, end_date, limit=100, ticker=ticker)

    # Convert to format suitable for CSV
    processed_news = []
    for article in news_data:
        # Parse the published_utc datetime
        try:
            if article.get('published_utc'):
                # Handle both Z and +00:00 timezone formats
                dt_str = article['published_utc']
                if dt_str.endswith('Z'):
                    dt_str = dt_str[:-1] + '+00:00'
                dt = datetime.fromisoformat(dt_str)
                formatted_datetime = dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                formatted_datetime = "Unknown"
        except (ValueError, TypeError) as e:
            logger.warning(f"Error parsing datetime: {e}")
            formatted_datetime = str(article.get('published_utc', 'Unknown'))

        # Get title or description as the topic
        topic = article.get('title') or article.get('description', 'No title available')

        processed_news.append({
            "datetime": formatted_datetime,
            "topic": topic
        })

    # Always save to CSV file
    import pandas as pd
    df = pd.DataFrame(processed_news)
    filename = f"news_{ticker}_{str(uuid.uuid4())[:8]}.csv" if ticker else f"news_{str(uuid.uuid4())[:8]}.csv"
    filepath = CSV_DIR / filename
    df.to_csv(filepath, index=False)
    logger.info(f"polygon_news saved CSV: {filename} ({len(processed_news)} articles)")

    # Return formatted response
    return _format_csv_response(filepath, df)


@mcp.tool()
def polygon_ticker_details(
    tickers: List[str]
) -> str:
    """
    Fetch comprehensive ticker details and save to CSV file.

    Parameters:
        tickers (List[str]): List of ticker symbols (e.g., ['AAPL', 'MSFT'])

    Returns:
        str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

    CSV Output Structure:
        - ticker (str): Stock ticker symbol
        - name (str): Company full name
        - market_cap (float): Market capitalization in USD
        - share_class_shares_outstanding (float): Number of shares outstanding for this share class
        - weighted_shares_outstanding (float): Weighted average shares outstanding
        - primary_exchange (str): Primary stock exchange code (e.g., 'XNAS' for NASDAQ)
        - type (str): Security type (e.g., 'CS' for Common Stock)
        - active (bool): Whether the ticker is actively traded
        - currency_name (str): Currency denomination (e.g., 'usd')
        - cik (str): SEC Central Index Key
        - composite_figi (str): Financial Instrument Global Identifier
        - share_class_figi (str): Share class specific FIGI
        - locale (str): Market locale (e.g., 'us')
        - description (str): Detailed company description
        - homepage_url (str): Company website URL
        - total_employees (int): Number of employees
        - list_date (str): IPO/listing date in YYYY-MM-DD format
        - logo_url (str): URL to company logo image
        - icon_url (str): URL to company icon image
        - sic_code (int): Standard Industrial Classification code
        - sic_description (str): SIC code description
        - ticker_root (str): Root ticker symbol
        - phone_number (str): Company phone number
        - address_1 (str): Primary address
        - city (str): Company headquarters city
        - state (str): Company headquarters state
        - postal_code (str): ZIP/postal code

    Use this data for: Company research, fundamental analysis, market cap screening, sector analysis.
    Always use py_eval tool to analyze the saved CSV file.
    """
    logger.info(f"polygon_ticker_details invoked with {len(tickers)} tickers")

    try:
        # Fetch ticker details (no output_dir, we'll save separately)
        df = fetch_ticker_details(tickers, output_dir=None)

        if df.empty:
            return "No ticker details were retrieved"

        # Always save to CSV file
        filename = f"ticker_details_{uuid.uuid4().hex[:8]}.csv"
        filepath = CSV_DIR / filename
        df.to_csv(filepath, index=False)
        logger.info(f"Saved ticker details to {filename} ({len(df)} records)")

        # Return formatted response
        return _format_csv_response(filepath, df)

    except Exception as e:
        logger.error(f"Error in polygon_ticker_details: {e}")
        return f"Error fetching ticker details: {str(e)}"


@mcp.tool()
def polygon_price_data(
    tickers: List[str],
    from_date: str = "",
    to_date: str = "",
    timespan: str = "day",
    multiplier: int = 1
) -> str:
    """
    Fetch historical price data and save to CSV file.

    Parameters:
        tickers (List[str]): List of ticker symbols
        from_date (str): Start date in YYYY-MM-DD format (defaults to 30 days ago)
        to_date (str): End date in YYYY-MM-DD format (defaults to today)
        timespan (str): Time span (day, week, month, quarter, year)
        multiplier (int): Size of the time window

    Returns:
        str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

    CSV Output Structure:
        - ticker (str): Stock ticker symbol
        - timestamp (str): Trading session timestamp in 'YYYY-MM-DD HH:MM:SS' format
        - open (float): Opening price for the period
        - high (float): Highest price during the period
        - low (float): Lowest price during the period
        - close (float): Closing price for the period
        - volume (float): Total trading volume for the period
        - vwap (float): Volume Weighted Average Price
        - transactions (int): Number of transactions during the period
        - date (str): Trading date in 'YYYY-MM-DD' format

    Use this data for: Technical analysis, price trend analysis, volume analysis, OHLC charting, volatility calculations.
    Always use py_eval tool to analyze the saved CSV file.
    """
    logger.info(f"polygon_price_data invoked with {len(tickers)} tickers, dates {from_date} to {to_date}")

    try:
        # Set default dates if not provided
        from_date_param = from_date if from_date else None
        to_date_param = to_date if to_date else None

        # Fetch price data (no output_dir, we'll save separately)
        df = fetch_price_data(
            tickers=tickers,
            from_date=from_date_param,
            to_date=to_date_param,
            timespan=timespan,
            multiplier=multiplier,
            output_dir=None
        )

        if df.empty:
            return "No price data were retrieved"

        # Always save to CSV file
        filename = f"price_data_{uuid.uuid4().hex[:8]}.csv"
        filepath = CSV_DIR / filename
        df.to_csv(filepath, index=False)
        logger.info(f"Saved price data to {filename} ({len(df)} records)")

        # Return formatted response
        return _format_csv_response(filepath, df)

    except Exception as e:
        logger.error(f"Error in polygon_price_data: {e}")
        return f"Error fetching price data: {str(e)}"


@mcp.tool()
def polygon_price_metrics(
    tickers: List[str],
    from_date: str = "",
    to_date: str = ""
) -> str:
    """
    Calculate price-based metrics and save to CSV file.

    Parameters:
        tickers (List[str]): List of ticker symbols
        from_date (str): Start date in YYYY-MM-DD format (defaults to 30 days ago)
        to_date (str): End date in YYYY-MM-DD format (defaults to today)

    Returns:
        str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

    CSV Output Structure:
        - ticker (str): Stock ticker symbol
        - start_date (str): Analysis period start date in 'YYYY-MM-DD' format
        - end_date (str): Analysis period end date in 'YYYY-MM-DD' format
        - trading_days (int): Number of trading days in the period
        - start_price (float): Opening price on start date
        - end_price (float): Closing price on end date
        - high_price (float): Highest price during the period
        - low_price (float): Lowest price during the period
        - total_return (float): Total return as decimal (e.g., 0.12 = 12% gain)
        - volatility (float): Price volatility (standard deviation of daily returns)
        - avg_daily_volume (float): Average daily trading volume
        - total_volume (float): Total volume traded during period
        - price_range_ratio (float): (High - Low) / Low ratio indicating price range

    Use this data for: Risk analysis, performance comparison, volatility assessment, return calculations, portfolio optimization.
    Always use py_eval tool to analyze the saved CSV file.
    """
    logger.info(f"polygon_price_metrics invoked with {len(tickers)} tickers")

    try:
        # Set default dates if not provided
        from_date_param = from_date if from_date else None
        to_date_param = to_date if to_date else None

        # First fetch price data
        price_df = fetch_price_data(
            tickers=tickers,
            from_date=from_date_param,
            to_date=to_date_param
        )

        if price_df.empty:
            return "No price data available for metrics calculation"

        # Calculate metrics
        metrics_df = calculate_price_metrics(price_df)

        if metrics_df.empty:
            return "No metrics could be calculated"

        # Always save to CSV file
        filename = f"price_metrics_{uuid.uuid4().hex[:8]}.csv"
        filepath = CSV_DIR / filename
        metrics_df.to_csv(filepath, index=False)
        logger.info(f"Saved price metrics to {filename} ({len(metrics_df)} records)")

        # Return formatted response
        return _format_csv_response(filepath, metrics_df)

    except Exception as e:
        logger.error(f"Error in polygon_price_metrics: {e}")
        return f"Error calculating price metrics: {str(e)}"


@mcp.tool()
def polygon_crypto_aggregates(
    ticker: str,
    from_date: str = "",
    to_date: str = "",
    timespan: str = "hour",
    multiplier: int = 1,
    limit: int = 50000
) -> str:
    """
    Fetch crypto aggregate bars (OHLCV data) for cryptocurrency trading analysis and save to CSV file.

    Parameters:
        ticker (str, required): Cryptocurrency ticker symbol in format X:BASEUSD.
            Examples: "X:BTCUSD" (Bitcoin/USD), "X:ETHUSD" (Ethereum/USD),
                     "X:SOLUSD" (Solana/USD), "X:ADAUSD" (Cardano/USD),
                     "X:DOGEUSD" (Dogecoin/USD), "X:MATICUSD" (Polygon/USD).
            The 'X:' prefix indicates the crypto exchange aggregation.

        from_date (str, optional): Start date in 'YYYY-MM-DD' format.
            Defaults to 7 days before today if not provided.
            Example: "2024-01-01"

        to_date (str, optional): End date in 'YYYY-MM-DD' format.
            Defaults to today if not provided.
            Example: "2024-01-31"

        timespan (str, optional): Size of the time window for each aggregate bar.
            Options: "minute", "hour", "day", "week", "month"
            Default: "hour"
            - "minute": Use for high-frequency trading, scalping strategies
            - "hour": Recommended for intraday trading decisions (default)
            - "day": Use for swing trading, daily trend analysis
            - "week"/"month": Use for long-term position analysis

        multiplier (int, optional): Multiplier for the timespan to create custom intervals.
            Range: 1-999
            Default: 1
            Examples:
            - multiplier=5, timespan="minute" → 5-minute bars
            - multiplier=4, timespan="hour" → 4-hour bars (popular in crypto)
            - multiplier=1, timespan="day" → daily bars

        limit (int, optional): Maximum number of aggregate bars to retrieve.
            Range: 1-50000
            Default: 50000
            Note: Crypto markets trade 24/7, so you'll get more bars per day than stocks.
                  For minute bars over 7 days: ~10,080 bars (7 * 24 * 60)
                  For hour bars over 7 days: ~168 bars (7 * 24)

    Returns:
        str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

    CSV Output Structure:
        - ticker (str): Cryptocurrency ticker symbol (e.g., "X:BTCUSD")
        - timestamp (str): Trading bar timestamp in 'YYYY-MM-DD HH:MM:SS' format (24/7 coverage)
        - open (float): Opening price for the period in USD
        - high (float): Highest price during the period in USD
        - low (float): Lowest price during the period in USD
        - close (float): Closing price for the period in USD
        - volume (float): Total trading volume for the period (in base currency units)
        - vwap (float): Volume Weighted Average Price - critical for crypto trading strategies
        - transactions (int): Number of individual trades aggregated into this bar
        - date (str): Trading date in 'YYYY-MM-DD' format

    Use Cases:
        - Technical Analysis: Calculate indicators (RSI, MACD, Bollinger Bands) using OHLC data
        - Trading Strategies: Identify support/resistance, trend patterns, breakouts
        - Backtesting: Test trading algorithms with historical crypto data
        - Market Research: Analyze crypto volatility, volume patterns, price correlation
        - Risk Management: Calculate drawdowns, volatility metrics (ATR, standard deviation)
        - VWAP Strategies: Use volume-weighted prices for institutional-style execution
        - 24/7 Market Analysis: Crypto markets never close - analyze weekend/overnight patterns

    Important Notes:
        - Crypto markets trade 24 hours a day, 7 days a week (no market hours)
        - High volatility: Price swings can be significant, especially for smaller cap coins
        - VWAP is particularly important in crypto due to thin liquidity in some periods
        - Transaction count helps identify high-activity periods vs. low liquidity periods

    Always use py_eval tool to analyze the saved CSV file for trading decisions.
    """
    logger.info(f"polygon_crypto_aggregates invoked: ticker={ticker}, from_date={from_date}, to_date={to_date}")
    logger.info(f"Parameters: timespan={timespan}, multiplier={multiplier}, limit={limit}")

    try:
        # Set default dates if not provided
        from_date_param = from_date if from_date else None
        to_date_param = to_date if to_date else None

        # Fetch crypto aggregates
        df = fetch_crypto_aggregates(
            ticker=ticker,
            from_date=from_date_param,
            to_date=to_date_param,
            timespan=timespan,
            multiplier=multiplier,
            limit=limit,
            output_dir=None
        )

        if df.empty:
            return f"No crypto aggregate data were retrieved for {ticker}. Please check the ticker format (should be X:BASEUSD, e.g., X:BTCUSD) and date range."

        # Always save to CSV file
        # Clean ticker for filename (replace : with _)
        clean_ticker = ticker.replace(':', '_')
        filename = f"crypto_aggs_{clean_ticker}_{uuid.uuid4().hex[:8]}.csv"
        filepath = CSV_DIR / filename
        df.to_csv(filepath, index=False)
        logger.info(f"Saved crypto aggregates to {filename} ({len(df)} records)")

        # Return formatted response
        return _format_csv_response(filepath, df)

    except Exception as e:
        logger.error(f"Error in polygon_crypto_aggregates: {e}")
        return f"Error fetching crypto aggregates: {str(e)}"


@mcp.tool()
def polygon_crypto_conditions(
    limit: int = 100,
    data_type: str = "",
    condition_id: int = 0,
    sip: str = "",
    sort: str = "",
    order: str = ""
) -> str:
    """
    Fetch cryptocurrency condition codes for interpreting trade and quote data, and save to CSV file.

    Condition codes are reference metadata that explain special circumstances associated with crypto trades
    and quotes. They help you understand trade flags, filter data appropriately, and correctly interpret
    trading activity. Essential for accurate data analysis and algorithmic trading decisions.

    Parameters:
        limit (int, optional): Maximum number of condition codes to retrieve.
            Range: 1-1000
            Default: 100
            Recommendation: Use 1000 to get comprehensive reference data on first call.

        data_type (str, optional): Filter condition codes by the type of data they apply to.
            Options: "trade", "quote"
            Default: "" (returns all data types)
            Examples:
            - "trade": Only conditions that apply to trade data
            - "quote": Only conditions that apply to quote/bid-ask data
            Use this to focus on conditions relevant to your analysis type.

        condition_id (int, optional): Retrieve a specific condition code by its ID.
            Default: 0 (not used)
            Use when you need details about a specific condition you encountered in trade data.
            Example: condition_id=1 returns details for condition ID 1

        sip (str, optional): Filter by SIP (Systematic Internalization Provider) mapping.
            Default: "" (not used)
            If a condition has a mapping for the specified SIP, it will be included.
            Advanced parameter for exchange-specific condition interpretation.

        sort (str, optional): Field to sort results by.
            Options: "id", "name", "type"
            Default: "" (API default sorting)
            Examples:
            - "name": Sort alphabetically by condition name
            - "id": Sort numerically by condition ID

        order (str, optional): Sort order for results.
            Options: "asc" (ascending), "desc" (descending)
            Default: "" (API default order)
            Combine with sort parameter for specific ordering.

    Returns:
        str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

    CSV Output Structure:
        - id (int): Unique identifier for this condition code in Polygon.io system
        - name (str): Full descriptive name of the condition (e.g., "Average Price Trade")
        - abbreviation (str): Commonly-used short code for this condition (e.g., "AP")
        - asset_class (str): Asset type this applies to (always "crypto" for this endpoint)
        - description (str): Detailed explanation of what this condition means for trades/quotes
        - type (str): Category of condition. Values include:
            * "sale_condition": Conditions affecting trade/sale records
            * "quote_condition": Conditions affecting quote/bid-ask records
            * "sip_generated_flag": Flags generated by SIP aggregation
            * "financial_status_indicator": Financial status markers
            * "short_sale_restriction_indicator": Short sale restrictions
            * "settlement_condition": Settlement-related conditions
            * "market_condition": Market state conditions
            * "trade_thru_exempt": Trade-through exemption markers
        - legacy (bool): Whether this condition is deprecated (True) or active (False)
            Legacy conditions may appear in historical data but are no longer used
        - exchange (int): Optional exchange ID if condition is exchange-specific
            If present, this condition only applies to data from that exchange
        - data_types (JSON string): Array of data types this condition applies to
            Example: ["trade"], ["quote"], or ["trade", "quote"]
            Parse with json.loads() in Python
        - sip_mapping (JSON string): Maps condition codes from individual exchanges to Polygon.io code
            Example: {"CTA": "B", "UTP": "W"} means CTA exchange uses "B", UTP uses "W"
            Parse with json.loads() in Python for exchange-specific interpretation
        - update_rules (JSON string): Rules for how this condition affects aggregated data
            Contains nested objects showing if condition updates high/low/open/close/volume
            Example: {"consolidated": {"updates_high_low": false, "updates_volume": true}}
            Critical for understanding how conditions affect OHLCV bars
            Parse with json.loads() in Python

    Use Cases:
        - **Data Interpretation**: Understand what condition codes in your trade data mean
        - **Filtering**: Identify which conditions to include/exclude for clean analysis
        - **Aggregation Rules**: Learn how conditions affect OHLCV calculations
        - **Exchange Mapping**: Map raw exchange condition codes to standardized Polygon codes
        - **Compliance**: Ensure trading algorithms handle special conditions correctly
        - **Algorithm Development**: Build filters for valid/invalid trade conditions
        - **Historical Analysis**: Identify legacy conditions when analyzing old data

    Reference Data Notes:
        - This is metadata/reference data, not time-series trading data
        - Condition codes are relatively static; fetch once and cache for lookup
        - Use returned data as a lookup table when processing trade/quote data
        - Check 'legacy' field to exclude deprecated conditions from new algorithms

    Integration Example:
        1. Call this tool once to get all crypto conditions (limit=1000)
        2. Use py_eval to load CSV and create a lookup dictionary
        3. When analyzing trade data, cross-reference condition codes with this reference
        4. Apply update_rules to correctly calculate aggregates

    Always use py_eval tool to analyze the saved CSV file for condition code lookups.
    """
    logger.info(f"polygon_crypto_conditions invoked: limit={limit}, data_type={data_type}")

    try:
        # Handle optional parameters (convert empty strings to None)
        data_type_param = data_type if data_type else None
        condition_id_param = condition_id if condition_id != 0 else None
        sip_param = sip if sip else None
        sort_param = sort if sort else None
        order_param = order if order else None

        # Fetch crypto conditions
        df = fetch_crypto_conditions(
            data_type=data_type_param,
            condition_id=condition_id_param,
            sip=sip_param,
            limit=limit,
            sort=sort_param,
            order=order_param,
            output_dir=None
        )

        if df.empty:
            return "No crypto condition codes were retrieved. Please check your filter parameters."

        # Always save to CSV file
        filename = f"crypto_conditions_{uuid.uuid4().hex[:8]}.csv"
        filepath = CSV_DIR / filename
        df.to_csv(filepath, index=False)
        logger.info(f"Saved crypto conditions to {filename} ({len(df)} records)")

        # Return formatted response
        return _format_csv_response(filepath, df)

    except Exception as e:
        logger.error(f"Error in polygon_crypto_conditions: {e}")
        return f"Error fetching crypto conditions: {str(e)}"


@mcp.tool()
def polygon_crypto_daily_open_close(
    ticker: str,
    date: str,
    adjusted: bool = True
) -> str:
    """
    Fetch daily open/close data for a specific cryptocurrency pair on a specific date and save to CSV file.

    This endpoint retrieves the opening and closing trades for a crypto pair, providing essential daily
    pricing details for performance evaluation, historical analysis, and trading activity insights.

    Parameters:
        ticker (str, required): Cryptocurrency ticker symbol in format X:BASEUSD.
            The 'X:' prefix indicates crypto exchange aggregation.
            Examples:
            - "X:BTCUSD" (Bitcoin/USD)
            - "X:ETHUSD" (Ethereum/USD)
            - "X:SOLUSD" (Solana/USD)
            - "X:ADAUSD" (Cardano/USD)
            - "X:DOGEUSD" (Dogecoin/USD)
            - "X:MATICUSD" (Polygon/USD)
            - "X:AVAXUSD" (Avalanche/USD)
            - "X:LINKUSD" (Chainlink/USD)

        date (str, required): The specific date for which to fetch open/close data.
            Format: YYYY-MM-DD
            Examples: "2024-01-15", "2023-12-31"
            Note: Must be a past date (cannot fetch future dates)

        adjusted (bool, optional): Whether results are adjusted for splits.
            Default: True
            - True: Results are adjusted for any splits (recommended for analysis)
            - False: Raw, unadjusted results

    Returns:
        str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

    CSV Output Structure:
        - ticker (str): Cryptocurrency ticker symbol (e.g., "X:BTCUSD")
        - date (str): The requested date in 'YYYY-MM-DD' format
        - symbol (str): The symbol pair evaluated (e.g., "BTC-USD")
        - open (float): Opening price for the day in USD
        - close (float): Closing price for the day in USD
        - high (float): Highest price during the day in USD
        - low (float): Lowest price during the day in USD
        - volume (float): Total trading volume for the day (in base currency units)
        - after_hours (float): After hours trading price (if available)
        - pre_market (float): Pre-market trading price (if available)
        - is_utc (bool): Whether timestamps are in UTC timezone
        - num_opening_trades (int): Number of trades that constitute the opening price
        - opening_volume_total (float): Total volume of all opening trades
        - first_opening_trade_price (float): Price of the first opening trade
        - num_closing_trades (int): Number of trades that constitute the closing price
        - closing_volume_total (float): Total volume of all closing trades
        - last_closing_trade_price (float): Price of the last closing trade

    Use Cases:
        - **Daily Performance Analysis**: Compare open vs. close to determine daily gains/losses
        - **Historical Data Collection**: Build datasets for backtesting trading strategies
        - **Portfolio Tracking**: Monitor daily changes in crypto holdings
        - **Volatility Assessment**: Calculate daily price ranges (high - low) for risk analysis
        - **Trading Pattern Analysis**: Identify opening/closing trade patterns and volumes
        - **Price Discovery**: Understand how markets establish opening and closing prices
        - **Risk Management**: Analyze daily drawdowns and price movements
        - **Algorithm Development**: Train models on historical daily OHLC data

    Important Notes:
        - Crypto markets trade 24/7, so "open" and "close" refer to UTC day boundaries
        - Opening trades occur at the start of the UTC day (00:00:00 UTC)
        - Closing trades occur at the end of the UTC day (23:59:59 UTC)
        - Multiple trades may comprise the opening/closing prices
        - Trade counts and volumes provide liquidity insights
        - Data is typically available 1-2 days after the trading day
        - Use this for single-day snapshots; use polygon_crypto_aggregates for time series

    Example Workflow:
        1. Call this tool to fetch daily data for a specific date
        2. Receive CSV filename in response
        3. Use py_eval to load and analyze the CSV file
        4. Calculate metrics like daily return, volatility, trade analysis

    Always use py_eval tool to analyze the saved CSV file for trading decisions and insights.
    """
    logger.info(f"polygon_crypto_daily_open_close invoked: ticker={ticker}, date={date}, adjusted={adjusted}")

    try:
        # Fetch crypto daily open/close data
        df = fetch_crypto_daily_open_close(
            ticker=ticker,
            date=date,
            adjusted=adjusted,
            output_dir=None
        )

        if df.empty:
            return f"No daily open/close data were retrieved for {ticker} on {date}. Please check the ticker format (should be X:BASEUSD, e.g., X:BTCUSD) and ensure the date is valid and in the past."

        # Always save to CSV file
        # Clean ticker for filename (replace : with _)
        clean_ticker = ticker.replace(':', '_')
        clean_date = date.replace('-', '')
        filename = f"crypto_daily_open_close_{clean_ticker}_{clean_date}_{uuid.uuid4().hex[:8]}.csv"
        filepath = CSV_DIR / filename
        df.to_csv(filepath, index=False)
        logger.info(f"Saved crypto daily open/close to {filename} ({len(df)} records)")

        # Return formatted response
        return _format_csv_response(filepath, df)

    except Exception as e:
        logger.error(f"Error in polygon_crypto_daily_open_close: {e}")
        return f"Error fetching crypto daily open/close: {str(e)}"


@mcp.tool()
def polygon_crypto_exchanges(
    locale: str = ""
) -> str:
    """
    Fetch cryptocurrency exchange reference data and save to CSV file.

    This endpoint retrieves a list of known crypto exchanges including their identifiers,
    names, market types, and other relevant attributes. This reference information helps map
    exchange codes, understand market coverage, integrate exchange details into applications,
    and ensure regulatory compliance.

    Parameters:
        locale (str, optional): Filter exchanges by geographical location.
            Options: "us" (United States), "global" (international)
            Default: "" (returns all locales)
            Examples:
            - "us": Only exchanges operating in the United States
            - "global": Only international/global exchanges
            - "" (empty): All exchanges regardless of locale

    Returns:
        str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

    CSV Output Structure:
        - id (int): Unique identifier for this exchange in Polygon.io system
        - name (str): Full official name of the exchange
            Examples: "Binance", "Coinbase", "Kraken", "Bitfinex"
        - acronym (str): Commonly used abbreviation for the exchange
            Examples: "BNCE", "GDAX", "KRKN"
        - asset_class (str): Type of assets traded (always "crypto" for this endpoint)
        - locale (str): Geographical location identifier
            Values: "us" (United States) or "global" (international)
        - mic (str): Market Identifier Code following ISO 10383 standard
            Used for regulatory reporting and market identification
        - operating_mic (str): MIC of the entity that operates this exchange
            May differ from 'mic' for exchanges with multiple operating entities
        - participant_id (str): ID used by SIP (Securities Information Processor) to represent this exchange
            Used in data feeds and market data aggregation
        - type (str): Exchange classification type
            Values: "exchange", "TRF" (Trade Reporting Facility), "SIP" (Securities Information Processor)
        - url (str): Official website URL for the exchange (if available)
            Example: "https://www.binance.com"

    Use Cases:
        - **Exchange Mapping**: Create lookup tables to map exchange codes to full names
        - **Market Coverage Analysis**: Understand which exchanges are available in your data
        - **Application Development**: Display exchange options in trading applications
        - **Data Integration**: Integrate exchange details when processing trade/quote data
        - **Regulatory Compliance**: Use MIC codes for regulatory reporting requirements
        - **Exchange Selection**: Identify exchanges by locale for geographic-specific analysis
        - **Trading Strategy**: Filter data by specific exchanges for venue-specific strategies

    Reference Data Notes:
        - This is metadata/reference data, not time-series trading data
        - Exchange lists are relatively static; fetch once and cache for lookups
        - Use returned data as a lookup table when processing crypto trades/quotes
        - MIC codes are standard identifiers used across financial systems
        - Some exchanges may have multiple MIC codes for different services

    Integration Example:
        1. Call this tool once to get all crypto exchanges
        2. Use py_eval to load CSV and create exchange lookup dictionary
        3. When analyzing trade data, cross-reference exchange IDs with this reference
        4. Filter data by specific exchanges based on your trading strategy

    Important Notes:
        - This endpoint returns exchange metadata, not trading data
        - Exchange information helps interpret where trades occurred
        - Use 'mic' field for regulatory reporting and compliance
        - 'participant_id' helps identify exchange in market data feeds

    Always use py_eval tool to analyze the saved CSV file for exchange lookups and filtering.
    """
    logger.info(f"polygon_crypto_exchanges invoked" + (f": locale={locale}" if locale else ""))

    try:
        # Handle optional parameter (convert empty string to None)
        locale_param = locale if locale else None

        # Fetch crypto exchanges
        df = fetch_crypto_exchanges(
            locale=locale_param,
            output_dir=None
        )

        if df.empty:
            return "No crypto exchanges were retrieved. Please check your parameters."

        # Always save to CSV file
        filename = f"crypto_exchanges_{uuid.uuid4().hex[:8]}.csv"
        filepath = CSV_DIR / filename
        df.to_csv(filepath, index=False)
        logger.info(f"Saved crypto exchanges to {filename} ({len(df)} records)")

        # Return formatted response
        return _format_csv_response(filepath, df)

    except Exception as e:
        logger.error(f"Error in polygon_crypto_exchanges: {e}")
        return f"Error fetching crypto exchanges: {str(e)}"


@mcp.tool()
def polygon_crypto_grouped_daily_bars(
    date: str,
    adjusted: bool = True
) -> str:
    """
    Fetch daily OHLC data for ALL cryptocurrency pairs on a specific date and save to CSV file.

    This endpoint retrieves grouped daily aggregate bars for every crypto ticker traded on the
    specified date, providing a comprehensive market-wide snapshot in a single API call. Perfect
    for analyzing overall market conditions, identifying trending cryptocurrencies, comparing
    performance across the entire crypto market, and bulk data processing for research.

    Parameters:
        date (str, required): The date for which to fetch grouped daily bars.
            Format: YYYY-MM-DD
            Examples: "2024-01-15", "2023-12-31", "2024-03-20"
            Note: Must be a past date; data typically available 1-2 days after trading day
            Recommendation: Use recent dates (within last 30 days) for most current data

        adjusted (bool, optional): Whether results are adjusted for splits.
            Default: True
            - True: Results are adjusted for any splits (recommended for analysis)
            - False: Raw, unadjusted results
            Note: Most crypto pairs don't have splits, but this ensures consistency

    Returns:
        str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

    CSV Output Structure:
        - ticker (str): Cryptocurrency ticker symbol in X:BASEUSD format
            Examples: "X:BTCUSD" (Bitcoin), "X:ETHUSD" (Ethereum), "X:SOLUSD" (Solana)
        - date (str): The requested date in 'YYYY-MM-DD' format
        - open (float): Opening price for the day in USD
        - high (float): Highest price during the day in USD
        - low (float): Lowest price during the day in USD
        - close (float): Closing price for the day in USD
        - volume (float): Total trading volume for the day (in base currency units)
        - vwap (float): Volume Weighted Average Price - critical for institutional trading
        - transactions (int): Total number of individual trades during the day
        - timestamp (datetime): Timestamp for the daily bar in 'YYYY-MM-DD HH:MM:SS' format

    Use Cases:
        - **Market-Wide Analysis**: Get complete crypto market snapshot in one call
        - **Top Movers Identification**: Identify highest volume or highest volatility coins
        - **Performance Comparison**: Compare returns across all crypto pairs simultaneously
        - **Market Screening**: Filter cryptocurrencies by volume, price change, or other metrics
        - **Research & Backtesting**: Build historical datasets for algorithm development
        - **Portfolio Analysis**: Analyze portfolio holdings against market performance
        - **Trend Detection**: Identify market-wide trends and sector rotation
        - **Liquidity Analysis**: Find most liquid trading pairs using volume and transaction data
        - **Risk Assessment**: Calculate market-wide volatility and correlation metrics
        - **Daily Reports**: Generate automated daily market summary reports

    Important Notes:
        - This endpoint returns data for ALL crypto pairs in a single call (typically 500+ pairs)
        - Much more efficient than calling individual ticker endpoints repeatedly
        - Crypto markets trade 24/7, so "daily" refers to UTC day boundaries
        - Daily bars aggregate all trades from 00:00:00 UTC to 23:59:59 UTC
        - Data includes both major pairs (BTC, ETH) and smaller altcoins
        - VWAP is particularly important for large orders and institutional execution
        - Transaction count helps identify high-activity vs. low-liquidity pairs
        - Perfect for daily analysis workflows and market monitoring systems

    Example Workflow:
        1. Call this tool to fetch all crypto pairs for a specific date
        2. Receive CSV filename in response
        3. Use py_eval to load and analyze the CSV file
        4. Calculate metrics: sort by volume, identify gainers/losers, compute market stats
        5. Generate insights: "Top 10 by volume", "Biggest daily movers", "Market volatility"

    Performance Tips:
        - Fetches 500+ crypto pairs in one API call (very efficient)
        - Use for daily snapshots rather than intraday analysis
        - For time-series analysis of specific pairs, use polygon_crypto_aggregates instead
        - For individual pair deep-dive, use polygon_crypto_daily_open_close

    Always use py_eval tool to analyze the saved CSV file for comprehensive market insights.
    """
    logger.info(f"polygon_crypto_grouped_daily_bars invoked: date={date}, adjusted={adjusted}")

    try:
        # Fetch crypto grouped daily bars
        df = fetch_crypto_grouped_daily_bars(
            date=date,
            adjusted=adjusted,
            output_dir=None
        )

        if df.empty:
            return f"No crypto grouped daily data were retrieved for {date}. Please ensure the date is valid (YYYY-MM-DD format) and is in the past. Data is typically available 1-2 days after the trading day."

        # Always save to CSV file
        # Clean date for filename (remove dashes)
        clean_date = date.replace('-', '')
        filename = f"crypto_grouped_daily_{clean_date}_{uuid.uuid4().hex[:8]}.csv"
        filepath = CSV_DIR / filename
        df.to_csv(filepath, index=False)
        logger.info(f"Saved crypto grouped daily bars to {filename} ({len(df)} crypto pairs)")

        # Return formatted response
        return _format_csv_response(filepath, df)

    except Exception as e:
        logger.error(f"Error in polygon_crypto_grouped_daily_bars: {e}")
        return f"Error fetching crypto grouped daily bars: {str(e)}"


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
