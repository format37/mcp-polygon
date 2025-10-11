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
        ticker (optional): Specific ticker symbol to fetch news for

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

# @mcp.tool()
# def py_eval(code: str, timeout_sec: float = 5.0) -> Dict[str, Any]:
#     """
#     Execute Python code with pandas/numpy pre-loaded and access to CSV folder.

#     Parameters:
#         code (str): Python code to execute
#         timeout_sec (float): Execution timeout in seconds (default: 5.0)

#     Returns:
#         dict: Execution results with stdout, stderr, duration_ms, and error info

#     Available variables in execution environment:
#         - pd: pandas library
#         - np: numpy library
#         - CSV_PATH: path to data/mcp-polygon folder for reading/writing CSV files
#     """
#     logger.info(f"py_eval invoked with {len(code)} characters of code")

#     # Capture output
#     buf_out, buf_err = io.StringIO(), io.StringIO()
#     started = time.time()

#     try:
#         # Import scientific libraries in execution environment
#         import pandas as pd
#         import numpy as np

#         # Create execution environment
#         env = {
#             "__builtins__": __builtins__,
#             "pd": pd,
#             "np": np,
#             "CSV_PATH": str(CSV_DIR),
#         }

#         with redirect_stdout(buf_out), redirect_stderr(buf_err), _posix_time_limit(timeout_sec):
#             exec(code, env, env)
#         ok, error = True, None

#     except TimeoutError as e:
#         ok, error = False, f"Timeout: {e}"
#     except Exception:
#         ok, error = False, traceback.format_exc()

#     duration_ms = int((time.time() - started) * 1000)

#     result = {
#         "ok": ok,
#         "stdout": buf_out.getvalue(),
#         "stderr": buf_err.getvalue(),
#         "error": error,
#         "duration_ms": duration_ms,
#         "csv_path": str(CSV_DIR)
#     }

#     logger.info(f"py_eval completed: ok={ok}, duration={duration_ms}ms")
#     return result

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
