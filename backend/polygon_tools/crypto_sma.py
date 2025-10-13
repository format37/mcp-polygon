import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import uuid
from mcp_service import format_csv_response
import pandas as pd
from polygon import RESTClient

logger = logging.getLogger(__name__)


def fetch_crypto_sma(
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
    Fetch Simple Moving Average (SMA) indicator data for a cryptocurrency pair.

    Args:
        polygon_client: Initialized Polygon RESTClient
        ticker: Crypto ticker symbol in format X:BASEUSD (e.g., "X:BTCUSD", "X:ETHUSD")
        timespan: Size of the aggregate time window (minute, hour, day, week, month)
        window: The window size for SMA calculation (e.g., 50 for 50-period SMA)
        series_type: Price type to use for calculation (close, open, high, low)
        expand_underlying: Whether to include underlying aggregates in response
        order: Order of results by timestamp (asc or desc)
        limit: Maximum number of results (default: 120, max: 5000)
        timestamp_gte: Start of timestamp range in YYYY-MM-DD format
        timestamp_lte: End of timestamp range in YYYY-MM-DD format

    Returns:
        DataFrame with SMA values containing timestamp and SMA value columns

    Note:
        The SMA calculates the average price across a set number of periods with equal weight
        for all prices, smoothing price fluctuations to reveal underlying trends.
    """
    logger.info(f"Fetching SMA for {ticker}")
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
        sma_response = polygon_client.get_sma(**api_kwargs)

        # Extract values from response
        if hasattr(sma_response, 'values') and sma_response.values:
            for value_item in sma_response.values:
                # Convert timestamp from milliseconds to datetime
                timestamp_ms = getattr(value_item, 'timestamp', None)
                if timestamp_ms:
                    timestamp_dt = pd.to_datetime(timestamp_ms, unit='ms')
                else:
                    timestamp_dt = None

                record = {
                    'timestamp': timestamp_dt,
                    'sma_value': getattr(value_item, 'value', None),
                }
                records.append(record)

    except Exception as e:
        logger.error(f"Error fetching SMA for {ticker}: {e}")
        raise

    df = pd.DataFrame(records)

    # Add additional columns for context
    if not df.empty:
        df['ticker'] = ticker
        df['window'] = window
        df['series_type'] = series_type
        df['timespan'] = timespan
        # Reorder columns
        df = df[['ticker', 'timestamp', 'sma_value', 'window', 'series_type', 'timespan']]

    logger.info(f"Successfully fetched {len(df)} SMA data points for {ticker}")

    return df


def register_polygon_crypto_sma(local_mcp_instance, local_polygon_client, csv_dir):
    """Register the polygon_crypto_sma tool"""
    @local_mcp_instance.tool()
    def polygon_crypto_sma(
        ticker: str,
        timespan: str = "day",
        window: int = 50,
        series_type: str = "close",
        limit: int = 120,
        timestamp_gte: str = "",
        timestamp_lte: str = ""
    ) -> str:
        """
        Fetch Simple Moving Average (SMA) technical indicator for crypto trading analysis and save to CSV.

        The SMA calculates the average price across a set number of periods, giving equal weight to all prices
        in the window. This smooths price fluctuations to reveal underlying trends and provides more stable
        signals than EMA, though with slower response to price changes. Essential for identifying long-term
        trends, major support/resistance levels, and classic crossover trading strategies.

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

            window (int, optional): The window size for SMA calculation (number of periods).
                Range: 1-999
                Default: 50
                Common values:
                - 10-20: Short-term trends, quick signals but more noise
                - 50: Medium-term trend, widely used (default)
                - 100: Longer-term trend confirmation
                - 200: Long-term trend, most important support/resistance level
                Note: Larger windows = smoother line, slower response to price changes

            series_type (str, optional): The price type used to calculate the SMA.
                Options: "close", "open", "high", "low"
                Default: "close"
                - "close": Most common, represents end-of-period price (recommended)
                - "high": Use for resistance level analysis
                - "low": Use for support level analysis
                - "open": Less common, for specialized strategies

            limit (int, optional): Maximum number of SMA data points to retrieve.
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
            - timestamp (datetime): Timestamp for the SMA calculation in 'YYYY-MM-DD HH:MM:SS' format
            - sma_value (float): The calculated SMA value at this timestamp
            - window (int): The window size used for this SMA calculation
            - series_type (str): The price type used (close, open, high, low)
            - timespan (str): The timespan of the underlying data (day, hour, etc.)

        SMA vs EMA Comparison:
            - SMA: Equal weight to all prices in the window
                * Pros: More stable, smoother signals, less false signals, better for long-term trends
                * Cons: Slower to react to price changes, may lag behind current market conditions
            - EMA: Exponentially weighted, emphasizes recent prices
                * Pros: Faster response to price changes, better for short-term trading
                * Cons: More volatile, more false signals, can overreact to noise

        Use Cases:
            1. Trend Identification:
               - Price above SMA = Uptrend, potential buy signals
               - Price below SMA = Downtrend, potential sell signals
               - SMA slope: Rising = bullish trend, Falling = bearish trend

            2. Golden Cross & Death Cross (most famous SMA signals):
               - Golden Cross: 50-SMA crosses above 200-SMA = Strong bullish signal
               - Death Cross: 50-SMA crosses below 200-SMA = Strong bearish signal
               - These are major long-term trend reversal signals

            3. Dynamic Support/Resistance Levels:
               - 50-SMA often acts as support in uptrends, resistance in downtrends
               - 200-SMA is considered a major support/resistance level
               - Price bouncing off SMA can signal continuation of trend

            4. SMA Crossover Strategies:
               - Fast SMA (e.g., 20) crossing above slow SMA (e.g., 50) = Buy signal
               - Fast SMA crossing below slow SMA = Sell signal
               - Triple SMA: Use 3 SMAs (e.g., 20, 50, 200) for confirmation

            5. Price-SMA Crossover:
               - Price crossing above SMA = Buy signal (trend reversal to uptrend)
               - Price crossing below SMA = Sell signal (trend reversal to downtrend)
               - Wait for price to close above/below SMA for confirmation

            6. SMA Ribbon/Fan:
               - Plot multiple SMAs (e.g., 20, 50, 100, 200) together
               - SMAs aligned and expanding = Strong trend
               - SMAs converging = Potential trend change or consolidation

        Common SMA Trading Strategies:
            1. 50-200 Day Golden Cross/Death Cross: Most popular long-term strategy
            2. 20-50 Day Crossover: Medium-term trend following
            3. Price Above/Below 200-SMA: Simple trend filter (only buy if above, only sell if below)
            4. SMA Bounce Strategy: Buy when price bounces off rising SMA
            5. SMA Break Strategy: Buy/sell when price breaks through SMA with volume confirmation

        Standard SMA Periods:
            - 10-day SMA: Very short-term, sensitive to recent price action
            - 20-day SMA: Short-term trend, approximately one trading month
            - 50-day SMA: Medium-term trend, widely watched by traders
            - 100-day SMA: Intermediate-term trend
            - 200-day SMA: Long-term trend, most important MA for institutional investors
            - 200-week SMA: Bitcoin's historical bottom support level

        Important Notes:
            - SMA is a lagging indicator - it confirms trends but doesn't predict them
            - More reliable than EMA in choppy/volatile markets (fewer false signals)
            - Works best in trending markets, less useful in sideways/ranging markets
            - Longer SMA periods (200-day) are more respected by market participants
            - Crypto markets are 24/7, so SMA calculations include all weekend/overnight price action
            - The 200-day SMA is considered the "line in the sand" between bull and bear markets
            - Golden/Death crosses are lagging but highly reliable for major trend changes
            - Always wait for confirmation (candle close, volume) before acting on SMA signals
            - False breakouts are common - use additional indicators for confirmation
            - SMA crossovers can have significant lag - trend may be well underway before signal

        Combining SMA with Other Indicators:
            - SMA + RSI: Use SMA for trend, RSI for overbought/oversold timing
            - SMA + Volume: Confirm SMA crossovers with volume spikes
            - SMA + Support/Resistance: Use SMA as dynamic S/R along with horizontal levels
            - SMA + MACD: SMA for trend, MACD for momentum confirmation

        Always use py_eval tool to analyze the saved CSV file and calculate trading signals.

        Example Usage:
            # Get 50-day SMA for Bitcoin (medium-term trend)
            polygon_crypto_sma(ticker="X:BTCUSD", timespan="day", window=50, series_type="close", limit=120)

            # Get 200-day SMA for Bitcoin (long-term trend, bull/bear market indicator)
            polygon_crypto_sma(ticker="X:BTCUSD", timespan="day", window=200, series_type="close", limit=250)

            # Get 20-day SMA for Ethereum (short-term trend)
            polygon_crypto_sma(ticker="X:ETHUSD", timespan="day", window=20, series_type="close", limit=90)

            # For Golden Cross/Death Cross analysis, call twice:
            # polygon_crypto_sma(ticker="X:BTCUSD", window=50, limit=100) and
            # polygon_crypto_sma(ticker="X:BTCUSD", window=200, limit=250)
        """
        logger.info(f"polygon_crypto_sma invoked: ticker={ticker}, timespan={timespan}, window={window}")
        logger.info(f"Parameters: series_type={series_type}, limit={limit}")

        try:
            # Convert empty strings to None
            timestamp_gte_param = timestamp_gte if timestamp_gte else None
            timestamp_lte_param = timestamp_lte if timestamp_lte else None

            # Fetch SMA data
            df = fetch_crypto_sma(
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
                return f"No SMA data were retrieved for {ticker}. Please check the ticker format (should be X:BASEUSD, e.g., X:BTCUSD) and parameters."

            # Generate unique filename
            clean_ticker = ticker.replace(':', '_')
            filename = f"crypto_sma_{clean_ticker}_w{window}_{timespan}_{uuid.uuid4().hex[:8]}.csv"
            filepath = csv_dir / filename

            # Save to CSV
            df.to_csv(filepath, index=False)
            logger.info(f"Saved SMA data to {filename} ({len(df)} records)")

            # Return formatted response
            return format_csv_response(filepath, df)

        except Exception as e:
            logger.error(f"Error in polygon_crypto_sma: {e}")
            return f"Error fetching SMA data: {str(e)}"
