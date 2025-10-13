import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import uuid
from mcp_service import format_csv_response
import pandas as pd
from polygon import RESTClient

logger = logging.getLogger(__name__)


def fetch_crypto_aggregates(polygon_client: RESTClient,
                           ticker: str,
                           from_date: Optional[str] = None,
                           to_date: Optional[str] = None,
                           timespan: str = "hour",
                           multiplier: int = 1,
                           limit: int = 50000,
                           output_dir: Optional[Path] = None) -> pd.DataFrame:
    """
    Fetch historical crypto aggregate bars (OHLCV data) for a cryptocurrency pair.

    Args:
        polygon_client: Initialized Polygon RESTClient
        ticker: Crypto ticker symbol in format X:BASEUSD (e.g., "X:BTCUSD", "X:ETHUSD")
        from_date: Start date in YYYY-MM-DD format (defaults to 7 days ago)
        to_date: End date in YYYY-MM-DD format (defaults to today)
        timespan: Time span - minute, hour, day, week, month (default: hour)
        multiplier: Size of the time window (e.g., 5 for 5-hour bars, default: 1)
        limit: Maximum number of bars to fetch (default: 50000, max: 50000)
        output_dir: Optional output directory to save CSV file

    Returns:
        DataFrame with crypto aggregate bars containing OHLCV data

    Note:
        Crypto markets are 24/7, so timestamps span all days and hours.
        The ticker must be in format X:BASEUSD (e.g., X:BTCUSD for Bitcoin/USD).
    """
    if from_date is None:
        # Use dates 30-37 days ago to ensure data availability (API may have delays)
        from_date = (datetime.now() - timedelta(days=37)).strftime('%Y-%m-%d')
    if to_date is None:
        # End date 30 days ago to ensure data is available
        to_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

    logger.info(f"Fetching crypto aggregates for {ticker} from {from_date} to {to_date}")
    logger.info(f"Parameters: timespan={timespan}, multiplier={multiplier}, limit={limit}")

    records = []
    try:
        # Get crypto price aggregates
        aggs = polygon_client.list_aggs(
            ticker=ticker,
            multiplier=multiplier,
            timespan=timespan,
            from_=from_date,
            to=to_date,
            adjusted=True,
            limit=limit,
            sort="asc"
        )

        for bar in aggs:
            record = {
                'ticker': ticker,
                'timestamp': bar.timestamp,
                'open': bar.open,
                'high': bar.high,
                'low': bar.low,
                'close': bar.close,
                'volume': bar.volume,
                'vwap': getattr(bar, 'vwap', None),
                'transactions': getattr(bar, 'transactions', None),
            }
            records.append(record)

    except Exception as e:
        logger.error(f"Error fetching crypto aggregates for {ticker}: {e}")
        raise

    df = pd.DataFrame(records)

    # Convert timestamp to datetime
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['date'] = df['timestamp'].dt.date

    logger.info(f"Successfully fetched {len(df)} crypto aggregate bars for {ticker}")

    # Save to CSV if output directory is provided
    if output_dir and not df.empty:
        csv_file = output_dir / f"crypto_aggs_{ticker.replace(':', '_')}.csv"
        df.to_csv(csv_file, index=False)
        logger.info(f"Saved crypto aggregates to {csv_file}")

    return df


def register_polygon_crypto_aggregates(local_mcp_instance, local_polygon_client, csv_dir):
    """Register the polygon_crypto_aggregates tool"""
    @local_mcp_instance.tool()
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
                polygon_client=local_polygon_client,
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
            filepath = csv_dir / filename
            df.to_csv(filepath, index=False)
            logger.info(f"Saved crypto aggregates to {filename} ({len(df)} records)")

            # Return formatted response
            return format_csv_response(filepath, df)

        except Exception as e:
            logger.error(f"Error in polygon_crypto_aggregates: {e}")
            return f"Error fetching crypto aggregates: {str(e)}"
