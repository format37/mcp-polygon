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
    fetch_crypto_grouped_daily_bars,
    fetch_crypto_previous_close,
    fetch_crypto_snapshots,
    fetch_crypto_last_trade,
    fetch_market_holidays,
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
# register_polygon_crypto_aggregates(mcp, polygon_client, CSV_DIR) # Disabled temporarily due to paid plan requirements

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
        return format_csv_response(filepath, df)

    except Exception as e:
        logger.error(f"Error in polygon_crypto_grouped_daily_bars: {e}")
        return f"Error fetching crypto grouped daily bars: {str(e)}"


@mcp.tool()
def polygon_crypto_previous_close(
    ticker: str,
    adjusted: bool = True
) -> str:
    """
    Fetch the previous trading day's OHLC data for a cryptocurrency pair and save to CSV file.

    This endpoint retrieves the previous day's open, high, low, and close (OHLC) data for a
    specified crypto pair, providing key pricing metrics including volume to help assess recent
    performance and inform trading strategies. Perfect for baseline comparisons, technical analysis,
    market research, and daily reporting workflows.

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
            - "X:DOTUSD" (Polkadot/USD)
            - "X:UNIUSD" (Uniswap/USD)

        adjusted (bool, optional): Whether results are adjusted for splits.
            Default: True
            - True: Results are adjusted for any splits (recommended for analysis)
            - False: Raw, unadjusted results
            Note: Most crypto pairs don't have splits, but this ensures consistency

    Returns:
        str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

    CSV Output Structure:
        - ticker (str): Cryptocurrency ticker symbol (e.g., "X:BTCUSD")
        - timestamp (datetime): Previous day's timestamp in 'YYYY-MM-DD HH:MM:SS' format
        - open (float): Opening price for the previous day in USD
        - high (float): Highest price during the previous day in USD
        - low (float): Lowest price during the previous day in USD
        - close (float): Closing price for the previous day in USD
        - volume (float): Total trading volume for the previous day (in base currency units)
        - vwap (float): Volume Weighted Average Price - critical for institutional trading
        - transactions (int): Total number of individual trades during the previous day
        - date (str): Previous trading date in 'YYYY-MM-DD' format

    Use Cases:
        - **Baseline Comparison**: Compare current prices against previous day's close for daily gains/losses
        - **Technical Analysis**: Use previous day's OHLC data for indicator calculations (RSI, moving averages)
        - **Daily Reporting**: Generate automated daily performance reports and summaries
        - **Market Research**: Analyze daily price patterns and volatility across different crypto pairs
        - **Trading Strategies**: Establish support/resistance levels based on previous day's range
        - **Performance Tracking**: Monitor daily changes in crypto holdings and portfolios
        - **Gap Analysis**: Identify price gaps between previous close and current open
        - **Volatility Assessment**: Calculate daily price ranges (high - low) for risk analysis
        - **Algorithm Development**: Use previous day data as input for trading algorithms
        - **Price Discovery**: Understand how markets established previous day's prices

    Important Notes:
        - Returns data for the most recent completed trading day (previous day)
        - Crypto markets trade 24/7, so "previous day" refers to previous UTC day
        - Daily boundaries are UTC-based: 00:00:00 UTC to 23:59:59 UTC
        - This is a single-day snapshot; use polygon_crypto_aggregates for time series
        - VWAP is particularly important in crypto due to varying liquidity
        - Transaction count helps identify high-activity vs. low-liquidity days
        - Data is typically available within 1-2 hours after the UTC day closes
        - Perfect for daily analysis workflows and morning market reviews

    Example Workflow:
        1. Call this tool to fetch previous day's OHLC for a crypto pair
        2. Receive CSV filename in response
        3. Use py_eval to load and analyze the CSV file
        4. Calculate daily metrics: return percentage, volatility, price range
        5. Compare against historical averages or other crypto pairs
        6. Generate insights: "Bitcoin closed up 3.2% yesterday at $45,123"

    Performance Tips:
        - Very fast endpoint (single day lookup, no date range needed)
        - Ideal for daily morning reports and market summaries
        - For historical analysis over multiple days, use polygon_crypto_aggregates instead
        - Combine with polygon_crypto_grouped_daily_bars to compare across all crypto pairs
        - Use with polygon_market_status to understand when traditional markets were open

    Always use py_eval tool to analyze the saved CSV file for trading decisions and insights.
    """
    logger.info(f"polygon_crypto_previous_close invoked: ticker={ticker}, adjusted={adjusted}")

    try:
        # Fetch crypto previous close data
        df = fetch_crypto_previous_close(
            ticker=ticker,
            adjusted=adjusted,
            output_dir=None
        )

        if df.empty:
            return f"No previous close data were retrieved for {ticker}. Please check the ticker format (should be X:BASEUSD, e.g., X:BTCUSD) and ensure it's a valid cryptocurrency pair with available data."

        # Always save to CSV file
        # Clean ticker for filename (replace : with _)
        clean_ticker = ticker.replace(':', '_')
        filename = f"crypto_prev_close_{clean_ticker}_{uuid.uuid4().hex[:8]}.csv"
        filepath = CSV_DIR / filename
        df.to_csv(filepath, index=False)
        logger.info(f"Saved crypto previous close to {filename} ({len(df)} record)")

        # Return formatted response
        return format_csv_response(filepath, df)

    except Exception as e:
        logger.error(f"Error in polygon_crypto_previous_close: {e}")
        return f"Error fetching crypto previous close: {str(e)}"


# @mcp.tool() # Disabled due to special subscription plan requirement
def polygon_crypto_snapshots(
    tickers: str = ""
) -> str:
    """
    Fetch comprehensive snapshot data for cryptocurrency tickers and save to CSV file.

    This endpoint retrieves a full market snapshot consolidating key information like pricing,
    volume, and trade activity in a single API call. Snapshot data is cleared daily at 12:00 AM
    EST and begins repopulating as exchanges report new data (can start as early as 4:00 AM EST).
    By accessing all tickers at once or filtering specific ones, users can efficiently monitor
    market conditions, perform comparative analysis, and power trading applications that require
    complete, current market information.

    Parameters:
        tickers (str, optional): Comma-separated list of cryptocurrency ticker symbols to filter.
            Format: Crypto tickers in X:BASEUSD format
            Examples:
            - "" (empty string, default): Returns ALL available crypto tickers (500+ pairs)
            - "X:BTCUSD": Returns only Bitcoin/USD snapshot
            - "X:BTCUSD,X:ETHUSD,X:SOLUSD": Returns snapshots for Bitcoin, Ethereum, and Solana
            - "X:ADAUSD,X:DOGEUSD,X:MATICUSD": Returns snapshots for Cardano, Dogecoin, and Polygon

            Common Major Crypto Tickers:
            - X:BTCUSD (Bitcoin), X:ETHUSD (Ethereum), X:SOLUSD (Solana)
            - X:ADAUSD (Cardano), X:DOGEUSD (Dogecoin), X:MATICUSD (Polygon/MATIC)
            - X:AVAXUSD (Avalanche), X:LINKUSD (Chainlink), X:DOTUSD (Polkadot)
            - X:UNIUSD (Uniswap), X:ATOMUSD (Cosmos), X:XLMUSD (Stellar)

            Note: The 'X:' prefix indicates crypto exchange aggregation across multiple venues

    Returns:
        str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

    CSV Output Structure:
        Core Identification & Timestamps:
        - ticker (str): Cryptocurrency ticker symbol in X:BASEUSD format (e.g., "X:BTCUSD")
        - updated (datetime): Last update timestamp in 'YYYY-MM-DD HH:MM:SS' format
        - todays_change (float): Absolute price change from previous close (in USD)
        - todays_change_perc (float): Percentage price change from previous close

        Current Day (Today) Data:
        - day_open (float): Today's opening price in USD
        - day_high (float): Today's highest price in USD
        - day_low (float): Today's lowest price in USD
        - day_close (float): Today's most recent close/current price in USD
        - day_volume (float): Today's total trading volume (in base currency units)
        - day_vwap (float): Today's Volume Weighted Average Price - critical for execution analysis

        Previous Day Data (for comparison):
        - prev_day_open (float): Previous day's opening price in USD
        - prev_day_high (float): Previous day's highest price in USD
        - prev_day_low (float): Previous day's lowest price in USD
        - prev_day_close (float): Previous day's closing price in USD
        - prev_day_volume (float): Previous day's total trading volume
        - prev_day_vwap (float): Previous day's Volume Weighted Average Price

        Last Trade Information:
        - last_trade_price (float): Price of the most recent trade in USD
        - last_trade_size (float): Size of the most recent trade (in base currency)
        - last_trade_exchange (int): Exchange ID where the last trade occurred
        - last_trade_timestamp (datetime): Timestamp of last trade in 'YYYY-MM-DD HH:MM:SS' format

        Latest Minute Bar Data:
        - min_open (float): Latest minute's opening price
        - min_high (float): Latest minute's highest price
        - min_low (float): Latest minute's lowest price
        - min_close (float): Latest minute's closing price
        - min_volume (float): Latest minute's trading volume
        - min_vwap (float): Latest minute's Volume Weighted Average Price
        - min_timestamp (datetime): Minute bar timestamp in 'YYYY-MM-DD HH:MM:SS' format

    Use Cases:
        - **Market Overview**: Get complete crypto market snapshot in one call, see all assets at once
        - **Screening & Discovery**: Filter and identify top movers, high volume, or volatile cryptocurrencies
        - **Portfolio Monitoring**: Track multiple crypto holdings simultaneously with real-time updates
        - **Comparative Analysis**: Compare performance metrics across different crypto assets
        - **Trading Signals**: Identify entry/exit opportunities based on price changes and volume
        - **Risk Assessment**: Monitor volatility through daily ranges and volume patterns
        - **Market Sentiment**: Gauge overall market direction and strength through aggregate data
        - **Automated Trading**: Feed real-time snapshot data into trading algorithms and bots
        - **Research & Analysis**: Collect comprehensive market data for backtesting and strategy development
        - **Dashboard & Visualization**: Power live crypto market dashboards and heat maps

    Important Notes:
        - Snapshot data resets daily at 12:00 AM EST (midnight Eastern Time)
        - Data begins repopulating from 4:00 AM EST as exchanges report new trades
        - Crypto markets trade 24/7, so snapshots are always updating (unlike stocks)
        - Returns ALL crypto pairs when no tickers specified (typically 500+ pairs)
        - Filtering specific tickers significantly reduces response time and size
        - VWAP is crucial for evaluating execution quality and institutional trades
        - Today's change fields compare current price against previous day's close
        - Exchange IDs can be mapped using the polygon_crypto_exchanges tool
        - Minute bar data provides the most recent price action (last 60 seconds)
        - Last trade data shows the absolute latest transaction in the market

    Example Workflow:
        1. Call this tool with specific tickers or leave empty for full market snapshot
        2. Receive CSV filename in response
        3. Use py_eval to load and analyze the CSV file with pandas
        4. Calculate metrics: sort by change %, identify volume leaders, detect trends
        5. Generate insights: "BTC up 3.2%, ETH down 1.5%, SOL highest volume"

    Performance & Best Practices:
        - For quick checks of 3-5 cryptos, specify tickers to minimize response time
        - For full market scans, use empty tickers to get complete dataset
        - Combine with polygon_crypto_aggregates for historical time series analysis
        - Use with polygon_crypto_exchanges to map exchange IDs to exchange names
        - Poll this endpoint regularly (e.g., every 1-5 minutes) for near-real-time monitoring
        - Compare today's metrics vs. previous day to identify significant movements
        - Use VWAP to assess if current prices are favorable for large orders

    Integration with Other Tools:
        - Use with polygon_crypto_aggregates for detailed historical price charts
        - Combine with polygon_news to understand price movements from market events
        - Use with polygon_crypto_exchanges to identify which exchanges are most active
        - Integrate with py_eval for advanced statistical analysis and visualization

    Always use py_eval tool to analyze the saved CSV file for comprehensive market insights.
    """
    logger.info(f"polygon_crypto_snapshots invoked: tickers={tickers if tickers else '(all)'}")

    try:
        # Parse tickers parameter
        ticker_list = None
        if tickers and tickers.strip():
            # Split by comma and clean whitespace
            ticker_list = [t.strip() for t in tickers.split(',') if t.strip()]
            logger.info(f"Filtering for {len(ticker_list)} specific tickers")

        # Fetch crypto snapshots
        df = fetch_crypto_snapshots(
            tickers=ticker_list,
            output_dir=None
        )

        if df.empty:
            if ticker_list:
                return f"No crypto snapshot data were retrieved for the specified tickers: {', '.join(ticker_list)}.\n\nPossible causes:\n1. This endpoint may require a paid/premium Polygon.io plan\n2. The ticker format might be incorrect (should be X:BASEUSD, e.g., X:BTCUSD)\n3. The tickers might not be valid cryptocurrency pairs\n\nAlternative: Use polygon_crypto_aggregates for historical data or polygon_crypto_grouped_daily_bars for daily snapshots."
            else:
                return "No crypto snapshot data were retrieved.\n\nPossible causes:\n1. This endpoint may require a paid/premium Polygon.io plan (similar to polygon_crypto_last_trade)\n2. The endpoint may be temporarily unavailable\n3. There may be no active crypto tickers at this time\n\nAlternative: Use polygon_crypto_aggregates for historical data or polygon_crypto_grouped_daily_bars for daily snapshots."

        # Always save to CSV file
        if ticker_list and len(ticker_list) <= 5:
            # Include tickers in filename if 5 or fewer
            tickers_str = "_".join([t.replace(':', '_') for t in ticker_list])
            filename = f"crypto_snapshots_{tickers_str}_{uuid.uuid4().hex[:8]}.csv"
        else:
            # Use generic name for all tickers or many tickers
            filename = f"crypto_snapshots_{uuid.uuid4().hex[:8]}.csv"

        filepath = CSV_DIR / filename
        df.to_csv(filepath, index=False)
        logger.info(f"Saved crypto snapshots to {filename} ({len(df)} crypto pairs)")

        # Return formatted response
        return format_csv_response(filepath, df)

    except Exception as e:
        logger.error(f"Error in polygon_crypto_snapshots: {e}")
        return f"Error fetching crypto snapshots: {str(e)}"


# @mcp.tool() # Disabled since too expensive plan requirements
def polygon_crypto_last_trade(
    from_symbol: str,
    to_symbol: str = "USD"
) -> str:
    """
    Fetch the most recent trade for a specified cryptocurrency pair and save to CSV file.

    This endpoint retrieves the latest trade details for a crypto pair, including price, size,
    timestamp, exchange, and conditions. Provides up-to-date market information for real-time
    monitoring, rapid decision-making, and integration into crypto trading or analytics tools.

    Parameters:
        from_symbol (str, required): The base cryptocurrency symbol.
            Examples: "BTC" (Bitcoin), "ETH" (Ethereum), "SOL" (Solana), "ADA" (Cardano),
                     "DOGE" (Dogecoin), "MATIC" (Polygon), "AVAX" (Avalanche), "LINK" (Chainlink),
                     "DOT" (Polkadot), "UNI" (Uniswap), "ATOM" (Cosmos), "XLM" (Stellar)
            Note: Use the symbol only (e.g., "BTC"), not the full ticker format (not "X:BTCUSD")

        to_symbol (str, optional): The quote currency symbol.
            Common options: "USD" (US Dollar), "USDT" (Tether), "USDC" (USD Coin), "EUR" (Euro)
            Default: "USD"
            Note: Most liquid pairs use USD or USDT as the quote currency

    Returns:
        str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

    CSV Output Structure:
        - symbol (str): The symbol pair evaluated (e.g., "BTC-USD")
        - price (float): The price of the most recent trade in quote currency
            This is the actual dollar value per whole unit of the cryptocurrency
            Example: 45000.50 means $45,000.50 per 1 BTC
        - size (float): The size of the trade (volume in base currency units)
            Example: 0.015 means 0.015 BTC was traded
        - exchange (int): The exchange ID where this trade occurred
            Use polygon_crypto_exchanges tool to map exchange IDs to exchange names
            Example: 4 = Coinbase, 6 = Kraken (see exchanges reference)
        - timestamp (int): Unix millisecond timestamp when the trade occurred
            Example: 1605560885027 represents the exact moment of the trade
        - timestamp_readable (str): Human-readable timestamp in 'YYYY-MM-DD HH:MM:SS.mmm' format
            Example: "2024-01-15 14:23:45.123"
            Includes milliseconds for precise timing analysis
        - conditions (JSON string): Array of condition codes that apply to this trade
            Example: "[1]" indicates condition code 1 (see polygon_crypto_conditions for details)
            Parse with json.loads() in Python to get the array
            Conditions explain special circumstances (e.g., averaged price, off-exchange)
        - request_id (str): Unique identifier for this API request (for debugging/tracking)
        - status (str): Status of the API response (typically "success" or "error")

    Use Cases:
        - **Real-Time Price Monitoring**: Get the absolute latest trade price for a crypto pair
        - **Trading Execution Analysis**: Verify execution prices against last trade data
        - **Market Data Validation**: Compare your data feeds against Polygon's last trade
        - **Price Alerts**: Build alert systems that trigger on the most recent trade price
        - **Order Book Analysis**: Understand the last executed price vs. current bid/ask
        - **Latency Testing**: Measure data feed delays by comparing timestamps
        - **Slippage Analysis**: Calculate slippage by comparing order price vs. last trade
        - **Market Making**: Monitor last trade to adjust quotes and spreads
        - **Arbitrage Detection**: Compare last trade prices across different sources
        - **Algorithm Development**: Use real-time trades to train and validate trading models

    Important Notes:
        - Returns the single most recent trade only (not historical trades)
        - Crypto markets trade 24/7, so this data is always fresh (no market hours)
        - Trade timestamp is extremely precise (millisecond resolution)
        - Exchange ID maps to specific crypto exchanges (use polygon_crypto_exchanges to decode)
        - Condition codes provide context about the trade (use polygon_crypto_conditions to decode)
        - Price represents the actual executed price at that moment
        - Size is denominated in the base currency (e.g., for BTC-USD, size is in BTC)
        - This is snapshot data; for time-series analysis use polygon_crypto_aggregates

    Example Workflow:
        1. Call this tool to fetch the last trade for BTC/USD
        2. Receive CSV filename in response
        3. Use py_eval to load and analyze the CSV file
        4. Extract price, size, timestamp for trading decisions
        5. Cross-reference exchange ID with polygon_crypto_exchanges for venue details
        6. Parse conditions array to understand trade circumstances

    Performance Tips:
        - Very fast endpoint (single trade lookup)
        - Perfect for real-time dashboards and monitoring
        - For historical trade data, use polygon_crypto_aggregates instead
        - Combine with polygon_crypto_conditions to understand trade flags
        - Use polygon_crypto_exchanges to map exchange IDs to exchange names

    Always use py_eval tool to analyze the saved CSV file for trading decisions and real-time insights.
    """
    logger.info(f"polygon_crypto_last_trade invoked: from_symbol={from_symbol}, to_symbol={to_symbol}")

    try:
        # Fetch crypto last trade
        df = fetch_crypto_last_trade(
            from_symbol=from_symbol,
            to_symbol=to_symbol,
            output_dir=None
        )

        if df.empty:
            return f"No last trade data were retrieved for {from_symbol}/{to_symbol}. Please check the symbol format (e.g., 'BTC', 'ETH') and ensure it's a valid cryptocurrency pair."

        # Always save to CSV file
        # Clean symbols for filename
        filename = f"crypto_last_trade_{from_symbol}_{to_symbol}_{uuid.uuid4().hex[:8]}.csv"
        filepath = CSV_DIR / filename
        df.to_csv(filepath, index=False)
        logger.info(f"Saved crypto last trade to {filename}")

        # Return formatted response
        return format_csv_response(filepath, df)

    except Exception as e:
        logger.error(f"Error in polygon_crypto_last_trade: {e}")
        return f"Error fetching crypto last trade: {str(e)}"


@mcp.tool()
def polygon_market_holidays() -> str:
    """
    Fetch upcoming market holidays and their corresponding open/close times, and save to CSV file.

    This endpoint retrieves forward-looking market holiday information across all exchanges.
    Use this data to plan ahead for trading activities, system maintenance, operational planning,
    and notifying users about upcoming market closures or early closes.

    Parameters:
        None (endpoint automatically returns upcoming holidays)

    Returns:
        str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

    CSV Output Structure:
        - date (str): Holiday date in 'YYYY-MM-DD' format
            Example: "2024-11-28" (Thanksgiving)
        - exchange (str): Exchange identifier affected by this holiday
            Examples: "NYSE" (New York Stock Exchange), "NASDAQ", "OTC" (Over-The-Counter)
            Note: Same holiday may appear multiple times for different exchanges
        - name (str): Holiday name
            Examples: "Thanksgiving", "Christmas Day", "New Year's Day", "Independence Day"
        - status (str): Market status for this day
            Values:
            * "closed": Market is completely closed for the entire day
            * "early-close": Market closes earlier than usual
        - open (str): Opening time in ISO 8601 format (YYYY-MM-DDTHH:MM:SS.sssZ)
            Only present for "early-close" days when market opens normally
            Example: "2024-11-29T14:30:00.000Z" (9:30 AM EST)
            Note: null/empty for "closed" status days
        - close (str): Closing time in ISO 8601 format (YYYY-MM-DDTHH:MM:SS.sssZ)
            Present for "early-close" days showing the early closing time
            Example: "2024-11-29T18:00:00.000Z" (1:00 PM EST)
            Note: null/empty for "closed" status days

    Use Cases:
        - **Trading Schedule Adjustments**: Plan trading strategies around market closures
        - **System Maintenance**: Schedule system updates during market holidays
        - **Operational Planning**: Adjust staffing and operations for holiday schedules
        - **User Notifications**: Alert users about upcoming market closures and early closes
        - **Algorithm Scheduling**: Pause or adjust automated trading during holidays
        - **Calendar Integration**: Build integrated holiday calendars for trading platforms
        - **Risk Management**: Anticipate reduced liquidity before/after holidays
        - **Order Management**: Avoid placing orders that won't execute during closures
        - **Portfolio Rebalancing**: Plan rebalancing activities around market availability
        - **Crypto Trading**: Understand when traditional markets are closed (crypto trades 24/7)

    Important Notes:
        - This endpoint is **forward-looking only** - shows future holidays, not historical
        - Same holiday date may appear multiple times (once per exchange)
        - Crypto markets trade 24/7 but this data helps coordinate with traditional markets
        - Early-close days have reduced trading hours (check open/close times)
        - Times are typically in UTC/ISO format - convert to your local timezone as needed
        - Data covers major U.S. exchanges (NYSE, NASDAQ, OTC)
        - Plan ahead: Use this to anticipate market gaps in your trading strategies

    Trading Strategy Considerations:
        - Markets often have reduced volume and volatility before holidays
        - Gap risk increases over extended closures (e.g., 3-day weekends)
        - Early-close days may have compressed trading activity
        - Consider closing risky positions before extended holiday closures
        - Crypto markets continue during traditional market holidays

    Example Workflow:
        1. Call this tool to fetch upcoming market holidays
        2. Receive CSV filename in response
        3. Use py_eval to load and analyze the CSV file
        4. Filter by specific exchanges or date ranges
        5. Identify next closure date for scheduling purposes
        6. Alert systems or adjust trading algorithms accordingly

    Always use py_eval tool to analyze the saved CSV file for comprehensive holiday planning.
    """
    logger.info("polygon_market_holidays invoked")

    try:
        # Fetch market holidays
        df = fetch_market_holidays(output_dir=None)

        if df.empty:
            return "No upcoming market holidays were retrieved. The endpoint may be temporarily unavailable or there are no upcoming holidays in the system."

        # Always save to CSV file
        filename = f"market_holidays_{uuid.uuid4().hex[:8]}.csv"
        filepath = CSV_DIR / filename
        df.to_csv(filepath, index=False)
        logger.info(f"Saved market holidays to {filename} ({len(df)} records)")

        # Return formatted response
        return format_csv_response(filepath, df)

    except Exception as e:
        logger.error(f"Error in polygon_market_holidays: {e}")
        return f"Error fetching market holidays: {str(e)}"


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
