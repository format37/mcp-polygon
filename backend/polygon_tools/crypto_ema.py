import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import uuid
from mcp_service import format_csv_response
import pandas as pd
from polygon import RESTClient

logger = logging.getLogger(__name__)


def fetch_crypto_ema(
    polygon_client: RESTClient,
    ticker: str,
    timespan: str = "day",
    window: int = 50,
    series_type: str = "close",
    expand_underlying: bool = False,
    order: str = "desc",
    limit: int = 120,
    timestamp_gte: Optional[str] = None,
    timestamp_lte: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch Exponential Moving Average (EMA) indicator data for a cryptocurrency pair.

    Args:
        polygon_client: Initialized Polygon RESTClient
        ticker: Crypto ticker symbol in format X:BASEUSD (e.g., "X:BTCUSD", "X:ETHUSD")
        timespan: Size of the aggregate time window (minute, hour, day, week, month)
        window: The window size for EMA calculation (e.g., 50 for 50-period EMA)
        series_type: Price type to use for calculation (close, open, high, low)
        expand_underlying: Whether to include underlying aggregates in response
        order: Order of results by timestamp (asc or desc)
        limit: Maximum number of results (default: 120, max: 5000)
        timestamp_gte: Start of timestamp range in YYYY-MM-DD format
        timestamp_lte: End of timestamp range in YYYY-MM-DD format

    Returns:
        DataFrame with EMA values containing timestamp and EMA value columns

    Note:
        The EMA places greater weight on recent prices, making it more responsive to
        price changes compared to Simple Moving Average (SMA).
    """
    logger.info(f"Fetching EMA for {ticker}")
    logger.info(f"Parameters: timespan={timespan}, window={window}, series_type={series_type}, limit={limit}")

    records = []
    try:
        # Build kwargs for the API call
        api_kwargs = {
            'ticker': ticker,
            'timespan': timespan,
            'window': window,
            'series_type': series_type,
            'expand_underlying': expand_underlying,
            'order': order,
            'limit': min(limit, 5000),  # Enforce max limit
        }

        # Add timestamp range filters if provided
        if timestamp_gte:
            api_kwargs['timestamp_gte'] = timestamp_gte
        if timestamp_lte:
            api_kwargs['timestamp_lte'] = timestamp_lte

        # Call Polygon API
        ema_response = polygon_client.get_ema(**api_kwargs)

        # Extract values from response
        if hasattr(ema_response, 'values') and ema_response.values:
            for value_item in ema_response.values:
                # Convert timestamp from milliseconds to datetime
                timestamp_ms = getattr(value_item, 'timestamp', None)
                if timestamp_ms:
                    timestamp_dt = pd.to_datetime(timestamp_ms, unit='ms')
                else:
                    timestamp_dt = None

                record = {
                    'timestamp': timestamp_dt,
                    'ema_value': getattr(value_item, 'value', None),
                }
                records.append(record)

    except Exception as e:
        logger.error(f"Error fetching EMA for {ticker}: {e}")
        raise

    df = pd.DataFrame(records)

    # Add additional columns for context
    if not df.empty:
        df['ticker'] = ticker
        df['window'] = window
        df['series_type'] = series_type
        df['timespan'] = timespan
        # Reorder columns
        df = df[['ticker', 'timestamp', 'ema_value', 'window', 'series_type', 'timespan']]

    logger.info(f"Successfully fetched {len(df)} EMA data points for {ticker}")

    return df


def register_polygon_crypto_ema(local_mcp_instance, local_polygon_client, csv_dir):
    """Register the polygon_crypto_ema tool"""
    @local_mcp_instance.tool()
    def polygon_crypto_ema(
        ticker: str,
        timespan: str = "day",
        window: int = 50,
        series_type: str = "close",
        limit: int = 120,
        timestamp_gte: str = "",
        timestamp_lte: str = ""
    ) -> str:
        """
        Fetch Exponential Moving Average (EMA) technical indicator for crypto trading analysis and save to CSV.

        The EMA places greater weight on recent prices, enabling quicker trend detection and more responsive
        signals compared to Simple Moving Average. Essential for identifying trends, crossover signals,
        and dynamic support/resistance levels in crypto trading.

        Parameters:
            ticker (str, required): Cryptocurrency ticker symbol in format X:BASEUSD.
                Examples: "X:BTCUSD" (Bitcoin/USD), "X:ETHUSD" (Ethereum/USD),
                         "X:SOLUSD" (Solana/USD), "X:ADAUSD" (Cardano/USD),
                         "X:DOGEUSD" (Dogecoin/USD), "X:MATICUSD" (Polygon/USD).
                The 'X:' prefix indicates the crypto exchange aggregation.

            timespan (str, optional): Size of the aggregate time window for each data point.
                Options: "minute", "hour", "day", "week", "month"
                Default: "day"
                - "minute": Use for scalping, high-frequency strategies
                - "hour": Recommended for intraday trend detection
                - "day": Most popular for swing trading and trend analysis (default)
                - "week"/"month": Use for long-term trend identification

            window (int, optional): The window size for EMA calculation (number of periods).
                Range: 1-999
                Default: 50
                Common values:
                - 9-12: Very short-term, highly responsive to price changes
                - 20-26: Short-term trends, popular for day trading
                - 50: Medium-term trend, widely used (default)
                - 100: Longer-term trend confirmation
                - 200: Long-term trend, major support/resistance level
                Note: Larger windows = smoother line, slower response to price changes

            series_type (str, optional): The price type used to calculate the EMA.
                Options: "close", "open", "high", "low"
                Default: "close"
                - "close": Most common, represents end-of-period price (recommended)
                - "high": Use for resistance level analysis
                - "low": Use for support level analysis
                - "open": Less common, for specialized strategies

            limit (int, optional): Maximum number of EMA data points to retrieve.
                Range: 1-5000
                Default: 120
                Examples:
                - For daily data: 120 = ~4 months of trading history
                - For hourly data: 120 = 5 days of 24/7 crypto trading
                - For minute data: 120 = 2 hours of data

            timestamp_gte (str, optional): Start of timestamp range in 'YYYY-MM-DD' format.
                Filters results to include only data points on or after this date.
                Example: "2024-01-01"

            timestamp_lte (str, optional): End of timestamp range in 'YYYY-MM-DD' format.
                Filters results to include only data points on or before this date.
                Example: "2024-12-31"

        Returns:
            str: Formatted response with CSV file info, schema, sample data, and Python snippet to load the file.

        CSV Output Structure:
            - ticker (str): Cryptocurrency ticker symbol (e.g., "X:BTCUSD")
            - timestamp (datetime): Timestamp for the EMA calculation in 'YYYY-MM-DD HH:MM:SS' format
            - ema_value (float): The calculated EMA value at this timestamp
            - window (int): The window size used for this EMA calculation
            - series_type (str): The price type used (close, open, high, low)
            - timespan (str): The timespan of the underlying data (day, hour, etc.)

        Use Cases:
            - Trend Identification: EMA direction shows market trend (rising = uptrend, falling = downtrend)
            - EMA Crossover Signals: When fast EMA (e.g., 12) crosses slow EMA (e.g., 26), generates buy/sell signals
              * Golden Cross: Fast EMA crosses above slow EMA = Bullish signal
              * Death Cross: Fast EMA crosses below slow EMA = Bearish signal
            - Dynamic Support/Resistance: EMAs act as dynamic support in uptrends, resistance in downtrends
            - Trading Strategy Adjustment: Steepness of EMA slope indicates trend strength and volatility
            - Multi-Timeframe Analysis: Compare EMAs across different timespans for confluence
            - Entry/Exit Points: Price bouncing off EMA can signal good entry/exit opportunities
            - Trend Confirmation: Use multiple EMAs (e.g., 50, 100, 200) for stronger trend confirmation

        Common EMA Trading Strategies:
            1. EMA Ribbon: Plot multiple EMAs (e.g., 5, 8, 13, 21) to visualize trend strength
            2. MACD: Uses 12-EMA and 26-EMA for momentum trading
            3. Price-EMA Crossover: Buy when price crosses above EMA, sell when it crosses below
            4. 3-EMA Strategy: Use 5, 13, and 62-period EMAs for entry/exit signals

        Important Notes:
            - EMAs are lagging indicators - they follow price action, don't predict it
            - More responsive than SMA but can generate more false signals in choppy markets
            - Works best in trending markets, less reliable in sideways/ranging markets
            - Crypto markets are 24/7, so EMA calculations include all weekend/overnight price action
            - Combine with other indicators (RSI, volume, support/resistance) for better accuracy

        Always use py_eval tool to analyze the saved CSV file and calculate trading signals.

        Example Usage:
            # Get 50-day EMA for Bitcoin (most popular configuration)
            polygon_crypto_ema(ticker="X:BTCUSD", timespan="day", window=50, series_type="close", limit=120)

            # Get 12-hour EMA for Ethereum (for intraday trading)
            polygon_crypto_ema(ticker="X:ETHUSD", timespan="hour", window=12, series_type="close", limit=168)

            # Get 200-day EMA for long-term trend (major support/resistance)
            polygon_crypto_ema(ticker="X:BTCUSD", timespan="day", window=200, series_type="close", limit=250)
        """
        logger.info(f"polygon_crypto_ema invoked: ticker={ticker}, timespan={timespan}, window={window}")
        logger.info(f"Parameters: series_type={series_type}, limit={limit}")

        try:
            # Convert empty strings to None
            timestamp_gte_param = timestamp_gte if timestamp_gte else None
            timestamp_lte_param = timestamp_lte if timestamp_lte else None

            # Fetch EMA data
            df = fetch_crypto_ema(
                polygon_client=local_polygon_client,
                ticker=ticker,
                timespan=timespan,
                window=window,
                series_type=series_type,
                expand_underlying=False,
                order="desc",
                limit=limit,
                timestamp_gte=timestamp_gte_param,
                timestamp_lte=timestamp_lte_param,
            )

            if df.empty:
                return f"No EMA data were retrieved for {ticker}. Please check the ticker format (should be X:BASEUSD, e.g., X:BTCUSD) and parameters."

            # Generate unique filename
            clean_ticker = ticker.replace(':', '_')
            filename = f"crypto_ema_{clean_ticker}_w{window}_{timespan}_{uuid.uuid4().hex[:8]}.csv"
            filepath = csv_dir / filename

            # Save to CSV
            df.to_csv(filepath, index=False)
            logger.info(f"Saved EMA data to {filename} ({len(df)} records)")

            # Return formatted response
            return format_csv_response(filepath, df)

        except Exception as e:
            logger.error(f"Error in polygon_crypto_ema: {e}")
            return f"Error fetching EMA data: {str(e)}"
