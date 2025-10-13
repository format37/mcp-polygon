import logging
from pathlib import Path
from typing import Optional
import uuid
from mcp_service import format_csv_response
import pandas as pd
from polygon import RESTClient

logger = logging.getLogger(__name__)


def fetch_crypto_previous_close(
    polygon_client: RESTClient,
    ticker: str,
    adjusted: bool = True,
    output_dir: Optional[Path] = None
) -> pd.DataFrame:
    """
    Fetch previous trading day's OHLC data for a cryptocurrency pair.

    This endpoint retrieves the previous day's open, high, low, and close (OHLC) data
    for a specified crypto pair, providing key pricing metrics to assess recent performance
    and inform trading strategies.

    Args:
        polygon_client: Initialized Polygon RESTClient
        ticker: Crypto ticker symbol in format X:BASEUSD (e.g., "X:BTCUSD", "X:ETHUSD")
        adjusted: Whether results are adjusted for splits (default: True)
        output_dir: Optional output directory to save CSV file

    Returns:
        DataFrame with previous day's OHLC data including volume and VWAP

    Note:
        This endpoint provides the most recent completed trading day's data,
        useful for baseline comparisons, technical analysis, and daily reporting.
    """
    logger.info(f"Fetching crypto previous close for {ticker}")

    try:
        # Get previous close aggregate
        result = polygon_client.get_previous_close_agg(ticker)

        # Extract data from results array (typically contains one item)
        results = getattr(result, 'results', None)
        if not results or len(results) == 0:
            logger.error(f"No previous close data found for {ticker}")
            return pd.DataFrame()

        # Get the first (and typically only) result
        bar = results[0]

        # Extract OHLCV data
        record = {
            'ticker': getattr(bar, 'T', ticker),
            'timestamp': getattr(bar, 't', None),
            'open': getattr(bar, 'o', None),
            'high': getattr(bar, 'h', None),
            'low': getattr(bar, 'l', None),
            'close': getattr(bar, 'c', None),
            'volume': getattr(bar, 'v', None),
            'vwap': getattr(bar, 'vw', None),
            'transactions': getattr(bar, 'n', None),
        }

        df = pd.DataFrame([record])

        # Convert timestamp to datetime if present
        if not df.empty and 'timestamp' in df.columns and df['timestamp'].iloc[0]:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df['date'] = df['timestamp'].dt.date

        logger.info(f"Successfully fetched previous close for {ticker}: close=${record['close']}, volume={record['volume']}")

        # Save to CSV if output directory is provided
        if output_dir and not df.empty:
            csv_file = output_dir / f"crypto_prev_close_{ticker.replace(':', '_')}.csv"
            df.to_csv(csv_file, index=False)
            logger.info(f"Saved crypto previous close to {csv_file}")

        return df

    except Exception as e:
        logger.error(f"Error fetching crypto previous close for {ticker}: {e}")
        raise


def register_polygon_crypto_previous_close(local_mcp_instance, local_polygon_client, csv_dir):
    """Register the polygon_crypto_previous_close tool"""
    @local_mcp_instance.tool()
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
                polygon_client=local_polygon_client,
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
            filepath = csv_dir / filename
            df.to_csv(filepath, index=False)
            logger.info(f"Saved crypto previous close to {filename} ({len(df)} record)")

            # Return formatted response
            return format_csv_response(filepath, df)

        except Exception as e:
            logger.error(f"Error in polygon_crypto_previous_close: {e}")
            return f"Error fetching crypto previous close: {str(e)}"
