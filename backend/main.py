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
    """
    logger.info(f"polygon_news invoked: start_date={start_date}, end_date={end_date}, save_csv={save_csv}")

    # Mock data for testing
    today = datetime.now()
    mock_news = [
        {
            "datetime": (today - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S"),
            "topic": "Federal Reserve announces interest rate decision"
        },
        {
            "datetime": (today - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
            "topic": "Tech stocks surge on AI developments"
        },
        {
            "datetime": today.strftime("%Y-%m-%d %H:%M:%S"),
            "topic": "Energy sector shows strong quarterly earnings"
        }
    ]

    if save_csv:
        # Save to CSV file
        import pandas as pd
        df = pd.DataFrame(mock_news)
        filename = f"{str(uuid.uuid4())[:8]}.csv"
        filepath = CSV_DIR / filename
        df.to_csv(filepath, index=False)
        logger.info(f"polygon_news saved CSV: {filename}")
        return f"News data saved to: {filename}"
    else:
        # Format as markdown table
        result = "| Datetime | Topic |\n"
        result += "|----------|-------|\n"
        for news in mock_news:
            result += f"| {news['datetime']} | {news['topic']} |\n"

        logger.info(f"polygon_news successful: returned {len(mock_news)} news items")
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
