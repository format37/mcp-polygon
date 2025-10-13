import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import uuid
from mcp_service import format_csv_response
import pandas as pd
from polygon import RESTClient

logger = logging.getLogger(__name__)


def fetch_crypto_rsi(
    polygon_client: RESTClient,
    ticker: str,
    timespan: str = "day",
    window: int = 14,
    series_type: str = "close",
    expand_underlying: bool = False,
    order: str = "desc",
    limit: int = 120,
    timestamp_gte: Optional[str] = None,
    timestamp_lte: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch Relative Strength Index (RSI) indicator data for a cryptocurrency pair.

    Args:
        polygon_client: Initialized Polygon RESTClient
        ticker: Crypto ticker symbol in format X:BASEUSD (e.g., "X:BTCUSD", "X:ETHUSD")
        timespan: Size of the aggregate time window (minute, hour, day, week, month)
        window: The window size for RSI calculation (e.g., 14 for 14-period RSI)
        series_type: Price type to use for calculation (close, open, high, low)
        expand_underlying: Whether to include underlying aggregates in response
        order: Order of results by timestamp (asc or desc)
        limit: Maximum number of results (default: 120, max: 5000)
        timestamp_gte: Start of timestamp range in YYYY-MM-DD format
        timestamp_lte: End of timestamp range in YYYY-MM-DD format

    Returns:
        DataFrame with RSI values containing timestamp and RSI value columns

    Note:
        RSI measures the speed and magnitude of price changes, oscillating between 0 and 100
        to help identify overbought (>70) or oversold (<30) conditions.
    """
    logger.info(f"Fetching RSI for {ticker}")
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
        rsi_response = polygon_client.get_rsi(**api_kwargs)

        # Extract values from response
        if hasattr(rsi_response, 'values') and rsi_response.values:
            for value_item in rsi_response.values:
                # Convert timestamp from milliseconds to datetime
                timestamp_ms = getattr(value_item, 'timestamp', None)
                if timestamp_ms:
                    timestamp_dt = pd.to_datetime(timestamp_ms, unit='ms')
                else:
                    timestamp_dt = None

                record = {
                    'timestamp': timestamp_dt,
                    'rsi_value': getattr(value_item, 'value', None),
                }
                records.append(record)

    except Exception as e:
        logger.error(f"Error fetching RSI for {ticker}: {e}")
        raise

    df = pd.DataFrame(records)

    # Add additional columns for context
    if not df.empty:
        df['ticker'] = ticker
        df['window'] = window
        df['series_type'] = series_type
        df['timespan'] = timespan
        # Reorder columns
        df = df[['ticker', 'timestamp', 'rsi_value', 'window', 'series_type', 'timespan']]

    logger.info(f"Successfully fetched {len(df)} RSI data points for {ticker}")

    return df


def register_polygon_crypto_rsi(local_mcp_instance, local_polygon_client, csv_dir):
    """Register the polygon_crypto_rsi tool"""
    @local_mcp_instance.tool()
    def polygon_crypto_rsi(
        ticker: str,
        timespan: str = "day",
        window: int = 14,
        series_type: str = "close",
        limit: int = 120,
        timestamp_gte: str = "",
        timestamp_lte: str = ""
    ) -> str:
        """
        Fetch Relative Strength Index (RSI) technical indicator for crypto trading analysis and save to CSV.

        RSI measures the speed and magnitude of price changes, oscillating between 0 and 100 to identify
        overbought or oversold conditions. It is one of the most popular momentum indicators for timing
        entry and exit points in crypto trading.

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
                - "hour": Recommended for intraday momentum detection
                - "day": Most popular for swing trading and trend analysis (default)
                - "week"/"month": Use for long-term momentum identification

            window (int, optional): The window size for RSI calculation (number of periods).
                Range: 1-999
                Default: 14 (standard RSI period)
                Common values:
                - 9: More sensitive, generates more signals (good for short-term trading)
                - 14: Standard RSI period, most widely used (default)
                - 21: Less sensitive, fewer false signals (better for volatile markets)
                - 25: Smoother RSI, for longer-term trend confirmation
                Note: Smaller windows = more responsive but more false signals

            series_type (str, optional): The price type used to calculate the RSI.
                Options: "close", "open", "high", "low"
                Default: "close"
                - "close": Most common, represents end-of-period price (recommended)
                - "high": Use for resistance breakout analysis
                - "low": Use for support breakdown analysis
                - "open": Less common, for specialized strategies

            limit (int, optional): Maximum number of RSI data points to retrieve.
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
            - timestamp (datetime): Timestamp for the RSI calculation in 'YYYY-MM-DD HH:MM:SS' format
            - rsi_value (float): The calculated RSI value at this timestamp (0-100)
            - window (int): The window size used for this RSI calculation
            - series_type (str): The price type used (close, open, high, low)
            - timespan (str): The timespan of the underlying data (day, hour, etc.)

        RSI Interpretation:
            - RSI > 70: Overbought condition - potential sell signal, price may decline
            - RSI 50-70: Bullish momentum zone - uptrend in progress
            - RSI 30-50: Bearish momentum zone - downtrend in progress
            - RSI < 30: Oversold condition - potential buy signal, price may rise
            - RSI = 50: Neutral zone - no clear momentum direction

        Use Cases:
            1. Overbought/Oversold Detection:
               - Buy signals when RSI < 30 (oversold) and starts rising
               - Sell signals when RSI > 70 (overbought) and starts falling
               - Note: In strong trends, RSI can stay >70 (uptrend) or <30 (downtrend) for extended periods

            2. Divergence Analysis (powerful reversal signals):
               - Bullish Divergence: Price makes lower low, but RSI makes higher low → Buy signal
               - Bearish Divergence: Price makes higher high, but RSI makes lower high → Sell signal
               - Hidden Divergence: Indicates trend continuation

            3. RSI Trendline Breaks:
               - Draw trendlines on RSI itself
               - Breaks above RSI resistance = bullish signal
               - Breaks below RSI support = bearish signal

            4. Failure Swings (reversal patterns):
               - Bullish Failure Swing: RSI drops below 30, bounces above 30, pulls back but stays above 30, then breaks above previous high
               - Bearish Failure Swing: RSI rises above 70, drops below 70, bounces but stays below 70, then breaks below previous low

            5. 50-Level Crossovers:
               - RSI crossing above 50 = momentum shift to bullish
               - RSI crossing below 50 = momentum shift to bearish

            6. Trend Confirmation:
               - In uptrend: RSI typically bounces off 40-50 level (not oversold at 30)
               - In downtrend: RSI typically fails near 50-60 level (not overbought at 70)

        Common RSI Trading Strategies:
            1. Basic Overbought/Oversold: Buy when RSI < 30, sell when RSI > 70
            2. RSI + Support/Resistance: Combine RSI signals with price S/R levels
            3. RSI Divergence + Price Action: Wait for divergence + candlestick confirmation
            4. RSI + Moving Averages: Use RSI for timing entries in MA-confirmed trends
            5. RSI Range Trading: In sideways markets, buy at RSI 30, sell at RSI 70
            6. Multi-Timeframe RSI: Check RSI on higher timeframe to confirm direction

        Advanced RSI Concepts:
            - Positive/Negative Reversals: RSI makes new high/low but price doesn't = strong signal
            - RSI Bands: Consider 80/20 or 90/10 for very strong trends (not just 70/30)
            - RSI Smoothing: Some traders use RSI of RSI for additional smoothing
            - Crypto Adjustments: Consider using wider bands (75/25 or 80/20) due to crypto volatility

        Important Notes:
            - RSI is a leading indicator but can generate false signals in ranging markets
            - Works best when combined with trend indicators (EMAs) and volume analysis
            - In strong trending markets, overbought/oversold signals may be premature
            - 14-period RSI is standard but can be adjusted based on asset volatility
            - Crypto markets are 24/7, so RSI calculations include all weekend/overnight price action
            - RSI is more reliable on higher timeframes (daily) than lower timeframes (1-min)
            - Always wait for confirmation (price action, volume) before acting on RSI signals
            - False signals are common - use stop-losses and risk management

        Always use py_eval tool to analyze the saved CSV file and calculate trading signals.

        Example Usage:
            # Get standard 14-period RSI for Bitcoin (most popular configuration)
            polygon_crypto_rsi(ticker="X:BTCUSD", timespan="day", window=14, series_type="close", limit=120)

            # Get 9-period RSI for Ethereum (more sensitive for day trading)
            polygon_crypto_rsi(ticker="X:ETHUSD", timespan="hour", window=9, series_type="close", limit=168)

            # Get 21-period RSI for less volatile signals
            polygon_crypto_rsi(ticker="X:SOLUSD", timespan="day", window=21, series_type="close", limit=100)
        """
        logger.info(f"polygon_crypto_rsi invoked: ticker={ticker}, timespan={timespan}, window={window}")
        logger.info(f"Parameters: series_type={series_type}, limit={limit}")

        try:
            # Convert empty strings to None
            timestamp_gte_param = timestamp_gte if timestamp_gte else None
            timestamp_lte_param = timestamp_lte if timestamp_lte else None

            # Fetch RSI data
            df = fetch_crypto_rsi(
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
                return f"No RSI data were retrieved for {ticker}. Please check the ticker format (should be X:BASEUSD, e.g., X:BTCUSD) and parameters."

            # Generate unique filename
            clean_ticker = ticker.replace(':', '_')
            filename = f"crypto_rsi_{clean_ticker}_w{window}_{timespan}_{uuid.uuid4().hex[:8]}.csv"
            filepath = csv_dir / filename

            # Save to CSV
            df.to_csv(filepath, index=False)
            logger.info(f"Saved RSI data to {filename} ({len(df)} records)")

            # Return formatted response
            return format_csv_response(filepath, df)

        except Exception as e:
            logger.error(f"Error in polygon_crypto_rsi: {e}")
            return f"Error fetching RSI data: {str(e)}"
