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
CSV_DIR = pathlib.Path("/work/csv")
CSV_DIR.mkdir(parents=True, exist_ok=True)

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


def fetch_polygon_news_data(start_date: str = "", end_date: str = "", limit: int = 1000) -> list:
    """
    Fetch news data from Polygon.io API using RESTClient.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        limit: Maximum number of results to fetch

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

        logger.info(f"Fetching news from {start_date} to {end_date}")

        # Use the news endpoint to fetch articles
        news_articles = []

        # Fetch news for the date range
        try:
            news_data = polygon_client.list_ticker_news(
                ticker=None,  # Get general market news
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
        - CSV_PATH: path to /work/csv folder for reading/writing CSV files
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
    save_csv: bool = False
) -> str:
    """
    Fetches market news from Polygon.io financial news aggregator within a specified date range.

    Parameters:
        start_date (str, optional): The start date in 'YYYY-MM-DD' format.
            Defaults to 7 days before today if not provided.
        end_date (str, optional): The end date in 'YYYY-MM-DD' format.
            Defaults to today if not provided.
        save_csv (bool, optional): If True, saves data to CSV file and returns filename.

    Returns:
        str: A formatted table of news with datetime and topic, or CSV filename if save_csv=True.

    CSV Output Structure (when save_csv=True):
        - datetime (str): Publication date and time in 'YYYY-MM-DD HH:MM:SS' format
        - topic (str): News article title/headline, truncated to 200 characters if longer

    Use this data for: News sentiment analysis, market event tracking, timeline analysis
    """
    logger.info(f"polygon_news invoked: start_date={start_date}, end_date={end_date}, save_csv={save_csv}")

    # Try to fetch real data from Polygon API
    news_data = fetch_polygon_news_data(start_date, end_date, limit=100)

    # If no real data available, fall back to mock data
    if not news_data:
        logger.warning("No real news data available, using mock data")
        today = datetime.now()
        news_data = [
            {
                "published_utc": (today - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "title": "Federal Reserve announces interest rate decision",
                "description": "Federal Reserve announces interest rate decision"
            },
            {
                "published_utc": (today - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "title": "Tech stocks surge on AI developments",
                "description": "Tech stocks surge on AI developments"
            },
            {
                "published_utc": today.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "title": "Energy sector shows strong quarterly earnings",
                "description": "Energy sector shows strong quarterly earnings"
            }
        ]

    # Convert to format suitable for CSV/table display
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
            "topic": topic[:200] + "..." if len(topic) > 200 else topic  # Truncate long topics
        })

    if save_csv:
        # Save to CSV file
        import pandas as pd
        df = pd.DataFrame(processed_news)
        filename = f"news_{str(uuid.uuid4())[:8]}.csv"
        filepath = CSV_DIR / filename
        df.to_csv(filepath, index=False)
        logger.info(f"polygon_news saved CSV: {filename}")
        return f"News data saved to: {filename} ({len(processed_news)} articles)"
    else:
        # Format as markdown table
        result = "| Datetime | Topic |\n"
        result += "|----------|-------|\n"
        for news in processed_news:
            # Escape pipe characters in the topic to prevent markdown table issues
            topic_escaped = news['topic'].replace('|', '&#124;')
            result += f"| {news['datetime']} | {topic_escaped} |\n"

        logger.info(f"polygon_news successful: returned {len(processed_news)} news items")
        return result


@mcp.tool()
def polygon_ticker_details(
    tickers: List[str],
    save_csv: bool = True
) -> str:
    """
    Fetch comprehensive ticker details including company info, market cap, sector, etc.

    Parameters:
        tickers (List[str]): List of ticker symbols (e.g., ['AAPL', 'MSFT'])
        save_csv (bool): If True, saves data to CSV file and returns filename

    Returns:
        str: Success message with CSV filename or error message

    CSV Output Structure (when save_csv=True):
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

    Use this data for: Company research, fundamental analysis, market cap screening, sector analysis
    """
    logger.info(f"polygon_ticker_details invoked with {len(tickers)} tickers")

    try:
        # Create output directory in CSV folder
        output_dir = CSV_DIR / f"ticker_details_{uuid.uuid4().hex[:8]}"
        output_dir.mkdir(exist_ok=True)

        # Fetch ticker details
        df = fetch_ticker_details(tickers, output_dir if save_csv else None)

        if df.empty:
            return "No ticker details were retrieved"

        if save_csv:
            filename = f"ticker_details_{uuid.uuid4().hex[:8]}.csv"
            filepath = CSV_DIR / filename
            df.to_csv(filepath, index=False)
            logger.info(f"Saved ticker details to {filename}")
            return f"Ticker details saved to: {filename} ({len(df)} records)"
        else:
            # Return summary info
            return f"Retrieved ticker details for {len(df)} tickers"

    except Exception as e:
        logger.error(f"Error in polygon_ticker_details: {e}")
        return f"Error fetching ticker details: {str(e)}"


@mcp.tool()
def polygon_price_data(
    tickers: List[str],
    from_date: str = "",
    to_date: str = "",
    timespan: str = "day",
    multiplier: int = 1,
    save_csv: bool = True
) -> str:
    """
    Fetch historical price data for tickers.

    Parameters:
        tickers (List[str]): List of ticker symbols
        from_date (str): Start date in YYYY-MM-DD format (defaults to 30 days ago)
        to_date (str): End date in YYYY-MM-DD format (defaults to today)
        timespan (str): Time span (day, week, month, quarter, year)
        multiplier (int): Size of the time window
        save_csv (bool): If True, saves data to CSV file and returns filename

    Returns:
        str: Success message with CSV filename or error message

    CSV Output Structure (when save_csv=True):
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

    Use this data for: Technical analysis, price trend analysis, volume analysis, OHLC charting, volatility calculations
    """
    logger.info(f"polygon_price_data invoked with {len(tickers)} tickers, dates {from_date} to {to_date}")

    try:
        # Set default dates if not provided
        from_date_param = from_date if from_date else None
        to_date_param = to_date if to_date else None

        # Create output directory in CSV folder
        output_dir = CSV_DIR / f"price_data_{uuid.uuid4().hex[:8]}"
        output_dir.mkdir(exist_ok=True)

        # Fetch price data
        df = fetch_price_data(
            tickers=tickers,
            from_date=from_date_param,
            to_date=to_date_param,
            timespan=timespan,
            multiplier=multiplier,
            output_dir=output_dir if save_csv else None
        )

        if df.empty:
            return "No price data were retrieved"

        if save_csv:
            filename = f"price_data_{uuid.uuid4().hex[:8]}.csv"
            filepath = CSV_DIR / filename
            df.to_csv(filepath, index=False)
            logger.info(f"Saved price data to {filename}")
            return f"Price data saved to: {filename} ({len(df)} records)"
        else:
            # Return summary info
            return f"Retrieved price data: {len(df)} records for {len(df['ticker'].unique())} tickers"

    except Exception as e:
        logger.error(f"Error in polygon_price_data: {e}")
        return f"Error fetching price data: {str(e)}"


@mcp.tool()
def polygon_price_metrics(
    tickers: List[str],
    from_date: str = "",
    to_date: str = "",
    save_csv: bool = True
) -> str:
    """
    Calculate price-based metrics for tickers (requires fetching price data first).

    Parameters:
        tickers (List[str]): List of ticker symbols
        from_date (str): Start date in YYYY-MM-DD format (defaults to 30 days ago)
        to_date (str): End date in YYYY-MM-DD format (defaults to today)
        save_csv (bool): If True, saves data to CSV file and returns filename

    Returns:
        str: Success message with CSV filename or error message

    CSV Output Structure (when save_csv=True):
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

    Use this data for: Risk analysis, performance comparison, volatility assessment, return calculations, portfolio optimization
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

        if save_csv:
            filename = f"price_metrics_{uuid.uuid4().hex[:8]}.csv"
            filepath = CSV_DIR / filename
            metrics_df.to_csv(filepath, index=False)
            logger.info(f"Saved price metrics to {filename}")
            return f"Price metrics saved to: {filename} ({len(metrics_df)} records)"
        else:
            # Return summary info
            return f"Calculated price metrics for {len(metrics_df)} tickers"

    except Exception as e:
        logger.error(f"Error in polygon_price_metrics: {e}")
        return f"Error calculating price metrics: {str(e)}"


# @mcp.tool()
# def polygon_combined_analysis(
#     tickers: List[str],
#     from_date: str = "",
#     to_date: str = "",
#     save_csv: bool = True
# ) -> str:
#     """
#     Create a combined analysis with ticker details and price metrics.

#     Parameters:
#         tickers (List[str]): List of ticker symbols
#         from_date (str): Start date for price data in YYYY-MM-DD format (defaults to 30 days ago)
#         to_date (str): End date for price data in YYYY-MM-DD format (defaults to today)
#         save_csv (bool): If True, saves data to CSV file and returns filename

#     Returns:
#         str: Success message with CSV filename or error message
#     """
#     logger.info(f"polygon_combined_analysis invoked with {len(tickers)} tickers")

#     try:
#         # Set default dates if not provided
#         from_date_param = from_date if from_date else None
#         to_date_param = to_date if to_date else None

#         # Create output directory in CSV folder
#         output_dir = CSV_DIR / f"combined_analysis_{uuid.uuid4().hex[:8]}"
#         output_dir.mkdir(exist_ok=True)

#         # Create combined analysis
#         combined_df = create_combined_analysis(
#             tickers=tickers,
#             from_date=from_date_param,
#             to_date=to_date_param,
#             output_dir=output_dir if save_csv else None
#         )

#         if combined_df.empty:
#             return "No data available for combined analysis"

#         if save_csv:
#             filename = f"combined_analysis_{uuid.uuid4().hex[:8]}.csv"
#             filepath = CSV_DIR / filename
#             combined_df.to_csv(filepath, index=False)
#             logger.info(f"Saved combined analysis to {filename}")
#             return f"Combined analysis saved to: {filename} ({len(combined_df)} records)"
#         else:
#             # Return summary info
#             return f"Combined analysis completed for {len(combined_df)} tickers"

#     except Exception as e:
#         logger.error(f"Error in polygon_combined_analysis: {e}")
#         return f"Error creating combined analysis: {str(e)}"



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

### polygon_combined_analysis
Create a combined analysis with ticker details and price metrics.

**Parameters:**
- `tickers` (required): List of ticker symbols
- `from_date` (optional): Start date for price data in YYYY-MM-DD format (defaults to 30 days ago)
- `to_date` (optional): End date for price data in YYYY-MM-DD format (defaults to today)
- `save_csv` (optional): If True, saves data to CSV file and returns filename

**Returns:** Success message with CSV filename or summary information.

### py_eval
Execute Python code with pandas/numpy pre-loaded and access to CSV folder.

**Parameters:**
- `code` (required): Python code to execute
- `timeout_sec` (optional): Execution timeout in seconds (default: 5.0)

**Available variables:** pd (pandas), np (numpy), CSV_PATH (path to CSV folder)
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
