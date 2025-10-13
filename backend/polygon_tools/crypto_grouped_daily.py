import logging
import uuid
import pandas as pd
from typing import Optional
from pathlib import Path
from polygon import RESTClient
from mcp_service import format_csv_response

logger = logging.getLogger(__name__)


def fetch_crypto_grouped_daily_bars(
    polygon_client: RESTClient,
    date: str,
    adjusted: bool = True,
) -> pd.DataFrame:
    """
    Fetch grouped daily OHLC data for ALL cryptocurrency pairs on a specific date.

    This endpoint retrieves daily aggregate bars for all crypto tickers traded on a given date,
    providing a comprehensive market-wide snapshot in a single API call. Ideal for analyzing
    overall market conditions, identifying trending coins, and bulk data processing.

    Args:
        polygon_client: Initialized Polygon RESTClient
        date: Date in YYYY-MM-DD format (e.g., "2024-01-15")
        adjusted: Whether or not results are adjusted for splits (default: True)

    Returns:
        DataFrame with daily OHLC data for all crypto pairs on the specified date

    Note:
        This endpoint returns data for ALL crypto pairs in one call, making it extremely
        efficient for market-wide analysis. Perfect for daily market snapshots and research.
    """
    logger.info(f"Fetching crypto grouped daily bars for {date}")

    try:
        # Get grouped daily aggregates for all crypto pairs
        result = polygon_client.get_grouped_daily_aggs(
            date=date,
            locale="global",
            market_type="crypto",
            adjusted=adjusted
        )

        # Process results into records
        records = []
        for bar in result:
            # The ticker field might be 'T' or 'ticker' or 'symbol' depending on the API version
            ticker = getattr(bar, 'ticker', None) or getattr(bar, 'T', None) or getattr(bar, 'symbol', None)

            record = {
                'ticker': ticker,
                'date': date,
                'open': bar.open,
                'high': bar.high,
                'low': bar.low,
                'close': bar.close,
                'volume': bar.volume,
                'vwap': getattr(bar, 'vwap', None),
                'transactions': getattr(bar, 'transactions', None),
                'timestamp': bar.timestamp,
            }
            records.append(record)

        df = pd.DataFrame(records)

        # Convert timestamp to datetime if present
        if not df.empty and 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

        logger.info(f"Successfully fetched {len(df)} crypto pairs for {date}")

        return df

    except Exception as e:
        logger.error(f"Error fetching crypto grouped daily bars for {date}: {e}")
        return pd.DataFrame()


def register_polygon_crypto_grouped_daily(local_mcp_instance, local_polygon_client, csv_dir):
    """Register the polygon_crypto_grouped_daily_bars tool"""
    @local_mcp_instance.tool()
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
                polygon_client=local_polygon_client,
                date=date,
                adjusted=adjusted,
            )

            if df.empty:
                return f"No crypto grouped daily data were retrieved for {date}. Please ensure the date is valid (YYYY-MM-DD format) and is in the past. Data is typically available 1-2 days after the trading day."

            # Always save to CSV file
            # Clean date for filename (remove dashes)
            clean_date = date.replace('-', '')
            filename = f"crypto_grouped_daily_{clean_date}_{uuid.uuid4().hex[:8]}.csv"
            filepath = csv_dir / filename
            df.to_csv(filepath, index=False)
            logger.info(f"Saved crypto grouped daily bars to {filename} ({len(df)} crypto pairs)")

            # Return formatted response
            return format_csv_response(filepath, df)

        except Exception as e:
            logger.error(f"Error in polygon_crypto_grouped_daily_bars: {e}")
            return f"Error fetching crypto grouped daily bars: {str(e)}"
