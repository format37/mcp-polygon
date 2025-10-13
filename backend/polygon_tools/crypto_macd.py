import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import uuid
from mcp_service import format_csv_response
import pandas as pd
from polygon import RESTClient

logger = logging.getLogger(__name__)


def fetch_crypto_macd(
    polygon_client: RESTClient,
    ticker: str,
    timespan: str = "day",
    short_window: int = 12,
    long_window: int = 26,
    signal_window: int = 9,
    series_type: str = "close",
    expand_underlying: bool = False,
    order: str = "desc",
    limit: int = 120,
    timestamp_gte: Optional[str] = None,
    timestamp_lte: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch Moving Average Convergence/Divergence (MACD) indicator data for a cryptocurrency pair.

    Args:
        polygon_client: Initialized Polygon RESTClient
        ticker: Crypto ticker symbol in format X:BASEUSD (e.g., "X:BTCUSD", "X:ETHUSD")
        timespan: Size of the aggregate time window (minute, hour, day, week, month)
        short_window: Short window size for MACD calculation (default: 12)
        long_window: Long window size for MACD calculation (default: 26)
        signal_window: Window size for signal line calculation (default: 9)
        series_type: Price type to use for calculation (close, open, high, low)
        expand_underlying: Whether to include underlying aggregates in response
        order: Order of results by timestamp (asc or desc)
        limit: Maximum number of results (default: 120, max: 5000)
        timestamp_gte: Start of timestamp range in YYYY-MM-DD format
        timestamp_lte: End of timestamp range in YYYY-MM-DD format

    Returns:
        DataFrame with MACD values containing timestamp, MACD line, signal line, and histogram columns

    Note:
        MACD is a momentum indicator showing the relationship between two moving averages.
        The MACD line is calculated by subtracting the long-period EMA from the short-period EMA.
    """
    logger.info(f"Fetching MACD for {ticker}")
    logger.info(f"Parameters: timespan={timespan}, windows={short_window}/{long_window}/{signal_window}, series_type={series_type}, limit={limit}")

    records = []
    try:
        # Build kwargs for the API call
        api_kwargs = {
            'ticker': ticker,
            'timespan': timespan,
            'short_window': short_window,
            'long_window': long_window,
            'signal_window': signal_window,
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
        macd_response = polygon_client.get_macd(**api_kwargs)

        # Extract values from response
        if hasattr(macd_response, 'values') and macd_response.values:
            for value_item in macd_response.values:
                # Convert timestamp from milliseconds to datetime
                timestamp_ms = getattr(value_item, 'timestamp', None)
                if timestamp_ms:
                    timestamp_dt = pd.to_datetime(timestamp_ms, unit='ms')
                else:
                    timestamp_dt = None

                record = {
                    'timestamp': timestamp_dt,
                    'macd_value': getattr(value_item, 'value', None),
                    'signal': getattr(value_item, 'signal', None),
                    'histogram': getattr(value_item, 'histogram', None),
                }
                records.append(record)

    except Exception as e:
        logger.error(f"Error fetching MACD for {ticker}: {e}")
        raise

    df = pd.DataFrame(records)

    # Add additional columns for context
    if not df.empty:
        df['ticker'] = ticker
        df['short_window'] = short_window
        df['long_window'] = long_window
        df['signal_window'] = signal_window
        df['series_type'] = series_type
        df['timespan'] = timespan
        # Reorder columns
        df = df[['ticker', 'timestamp', 'macd_value', 'signal', 'histogram',
                 'short_window', 'long_window', 'signal_window', 'series_type', 'timespan']]

    logger.info(f"Successfully fetched {len(df)} MACD data points for {ticker}")

    return df


def register_polygon_crypto_macd(local_mcp_instance, local_polygon_client, csv_dir):
    """Register the polygon_crypto_macd tool"""
    @local_mcp_instance.tool()
    def polygon_crypto_macd(
        ticker: str,
        timespan: str = "day",
        short_window: int = 12,
        long_window: int = 26,
        signal_window: int = 9,
        series_type: str = "close",
        limit: int = 120,
        timestamp_gte: str = "",
        timestamp_lte: str = ""
    ) -> str:
        """
        Fetch Moving Average Convergence/Divergence (MACD) technical indicator for crypto trading and save to CSV.

        MACD is one of the most popular momentum indicators that shows the relationship between two exponential
        moving averages (EMAs). It helps identify trend direction, momentum strength, and potential reversal points.
        The indicator consists of three components: MACD line, Signal line, and Histogram.

        Parameters:
            ticker (str, required): Cryptocurrency ticker symbol in format X:BASEUSD.
                Examples: "X:BTCUSD" (Bitcoin/USD), "X:ETHUSD" (Ethereum/USD),
                         "X:SOLUSD" (Solana/USD), "X:ADAUSD" (Cardano/USD),
                         "X:DOGEUSD" (Dogecoin/USD), "X:MATICUSD" (Polygon/USD).
                The 'X:' prefix indicates the crypto exchange aggregation.

            timespan (str, optional): Size of the aggregate time window for each data point.
                Options: "minute", "hour", "day", "week", "month"
                Default: "day"
                - "minute": Use for scalping, very short-term trading
                - "hour": Recommended for intraday trading strategies
                - "day": Most popular for swing trading and trend analysis (default)
                - "week"/"month": Use for long-term trend identification

            short_window (int, optional): The short (fast) EMA period for MACD calculation.
                Range: 1-999
                Default: 12 (industry standard)
                - Smaller values (e.g., 8-10): More responsive, generates more signals
                - Standard value 12: Balanced responsiveness
                - Larger values (e.g., 15-20): Smoother, fewer false signals

            long_window (int, optional): The long (slow) EMA period for MACD calculation.
                Range: 1-999
                Default: 26 (industry standard)
                - Must be greater than short_window
                - Standard value 26: Widely used across markets
                - Adjust proportionally with short_window (e.g., 12/26, 8/17, 5/35)

            signal_window (int, optional): The EMA period for the signal line.
                Range: 1-999
                Default: 9 (industry standard)
                - The signal line is an EMA of the MACD line
                - Standard value 9: Most commonly used
                - Smaller values: More responsive crossover signals
                - Larger values: Smoother, more reliable signals

            series_type (str, optional): The price type used to calculate MACD.
                Options: "close", "open", "high", "low"
                Default: "close"
                - "close": Most common, represents end-of-period price (recommended)
                - "high": Use for analyzing resistance levels
                - "low": Use for analyzing support levels
                - "open": Less common, for specialized strategies

            limit (int, optional): Maximum number of MACD data points to retrieve.
                Range: 1-5000
                Default: 120
                Examples:
                - For daily data: 120 = ~4 months of trading history
                - For hourly data: 120 = 5 days of 24/7 crypto trading
                - For minute data: 120 = 2 hours of data
                Note: Need sufficient data for MACD calculation (at least long_window + signal_window periods)

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
            - timestamp (datetime): Timestamp for the MACD calculation in 'YYYY-MM-DD HH:MM:SS' format
            - macd_value (float): The MACD line value (short_EMA - long_EMA)
            - signal (float): The signal line value (EMA of MACD line)
            - histogram (float): The MACD histogram (macd_value - signal)
            - short_window (int): Short EMA window used (e.g., 12)
            - long_window (int): Long EMA window used (e.g., 26)
            - signal_window (int): Signal line window used (e.g., 9)
            - series_type (str): Price type used (close, open, high, low)
            - timespan (str): Timespan of the underlying data (day, hour, etc.)

        Understanding MACD Components:
            1. MACD Line (macd_value): Difference between fast and slow EMAs
               - Above zero: Short-term average is above long-term (bullish)
               - Below zero: Short-term average is below long-term (bearish)
               - Rising: Increasing bullish momentum
               - Falling: Increasing bearish momentum

            2. Signal Line (signal): 9-period EMA of MACD line
               - Acts as a trigger line for buy/sell signals
               - Smoother than MACD line, reduces false signals

            3. Histogram (histogram): Visual representation of MACD - Signal
               - Positive (above zero): MACD is above signal line (bullish)
               - Negative (below zero): MACD is below signal line (bearish)
               - Growing bars: Momentum is strengthening
               - Shrinking bars: Momentum is weakening
               - Zero-line cross: Potential trend change

        Trading Signals & Use Cases:
            1. **Crossover Signals** (Most Common):
               - Bullish Crossover: MACD crosses above signal line → Buy signal
               - Bearish Crossover: MACD crosses below signal line → Sell signal
               - More reliable when crossing in the direction of the larger trend

            2. **Zero Line Crossover**:
               - MACD crosses above zero → Bullish trend confirmation
               - MACD crosses below zero → Bearish trend confirmation
               - Indicates shift in momentum direction

            3. **Divergence Trading** (Advanced):
               - Bullish Divergence: Price makes lower lows, but MACD makes higher lows
                 → Potential bullish reversal
               - Bearish Divergence: Price makes higher highs, but MACD makes lower highs
                 → Potential bearish reversal
               - Strong reversal indicator when combined with support/resistance

            4. **Histogram Analysis**:
               - Histogram expanding: Momentum strengthening
               - Histogram contracting: Momentum weakening (potential reversal warning)
               - Histogram crossing zero: Early crossover signal

            5. **Overbought/Oversold Conditions**:
               - Extreme MACD values (relative to historical range) suggest potential reversal
               - Combine with RSI for confirmation

            6. **Trend Strength**:
               - Large MACD values indicate strong trends
               - MACD hovering near zero suggests consolidation/sideways market

        Common MACD Trading Strategies:
            1. **Classic MACD Strategy**: Trade crossovers in direction of overall trend
            2. **MACD + RSI Combo**: Confirm MACD signals with RSI overbought/oversold
            3. **MACD + Support/Resistance**: Take MACD signals near key price levels
            4. **Divergence Trading**: Trade divergences for reversal opportunities
            5. **Histogram Peak Trading**: Enter when histogram reaches extreme and starts reversing
            6. **Zero Line Bounce**: Buy when MACD bounces off zero in uptrend (or vice versa)

        Parameter Optimization for Crypto:
            - Standard (12, 26, 9): Works well for most crypto pairs, widely used
            - Fast (8, 17, 9): More responsive for volatile crypto markets
            - Slow (19, 39, 9): Reduces false signals in choppy markets
            - Weekly (5, 35, 5): For longer-term position trading

        Important Notes:
            - MACD is a lagging indicator - confirms trends but doesn't predict them
            - Works best in trending markets; less reliable in ranging/sideways markets
            - Crypto markets are 24/7, so calculations include all weekend/holiday data
            - False signals are common in low-volume or highly volatile conditions
            - Always use stop-losses; MACD alone is not sufficient for risk management
            - Combine with price action, volume, and support/resistance for best results
            - Requires sufficient historical data (at least long_window + signal_window periods)

        Always use py_eval tool to analyze the saved CSV file for crossover detection and signal generation.

        Example Usage:
            # Standard MACD for Bitcoin (most popular configuration)
            polygon_crypto_macd(ticker="X:BTCUSD", timespan="day", short_window=12,
                              long_window=26, signal_window=9, limit=120)

            # Fast MACD for intraday Ethereum trading
            polygon_crypto_macd(ticker="X:ETHUSD", timespan="hour", short_window=8,
                              long_window=17, signal_window=9, limit=200)

            # Weekly MACD for long-term trend analysis
            polygon_crypto_macd(ticker="X:BTCUSD", timespan="day", short_window=5,
                              long_window=35, signal_window=5, limit=250)

            # MACD with specific date range for backtesting
            polygon_crypto_macd(ticker="X:SOLUSD", timespan="day", timestamp_gte="2024-01-01",
                              timestamp_lte="2024-12-31", limit=500)
        """
        logger.info(f"polygon_crypto_macd invoked: ticker={ticker}, timespan={timespan}")
        logger.info(f"Parameters: windows={short_window}/{long_window}/{signal_window}, series_type={series_type}, limit={limit}")

        try:
            # Convert empty strings to None
            timestamp_gte_param = timestamp_gte if timestamp_gte else None
            timestamp_lte_param = timestamp_lte if timestamp_lte else None

            # Fetch MACD data
            df = fetch_crypto_macd(
                polygon_client=local_polygon_client,
                ticker=ticker,
                timespan=timespan,
                short_window=short_window,
                long_window=long_window,
                signal_window=signal_window,
                series_type=series_type,
                expand_underlying=False,
                order="desc",
                limit=limit,
                timestamp_gte=timestamp_gte_param,
                timestamp_lte=timestamp_lte_param,
            )

            if df.empty:
                return f"No MACD data were retrieved for {ticker}. Please check the ticker format (should be X:BASEUSD, e.g., X:BTCUSD) and parameters."

            # Generate unique filename
            clean_ticker = ticker.replace(':', '_')
            filename = f"crypto_macd_{clean_ticker}_{short_window}-{long_window}-{signal_window}_{timespan}_{uuid.uuid4().hex[:8]}.csv"
            filepath = csv_dir / filename

            # Save to CSV
            df.to_csv(filepath, index=False)
            logger.info(f"Saved MACD data to {filename} ({len(df)} records)")

            # Return formatted response
            return format_csv_response(filepath, df)

        except Exception as e:
            logger.error(f"Error in polygon_crypto_macd: {e}")
            return f"Error fetching MACD data: {str(e)}"
