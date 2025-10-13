import logging
import uuid
import pandas as pd
from typing import Optional
from pathlib import Path
from polygon import RESTClient
from mcp_service import format_csv_response

logger = logging.getLogger(__name__)


def fetch_crypto_snapshot_ticker(
    polygon_client: RESTClient,
    ticker: str,
) -> pd.DataFrame:
    """
    Fetch single ticker snapshot data for a specific cryptocurrency.

    This endpoint retrieves the most recent market data snapshot for a single ticker,
    consolidating the latest trade and aggregated data (minute, day, and previous day)
    for the specified ticker. Snapshot data is cleared at 12:00 AM EST and begins
    updating as exchanges report new information.

    Args:
        polygon_client: Initialized Polygon RESTClient
        ticker: Cryptocurrency ticker symbol in X:BASEUSD format (e.g., "X:BTCUSD")

    Returns:
        DataFrame with single row containing comprehensive snapshot data

    Note:
        This endpoint is optimized for focused monitoring of a single ticker.
        For multiple tickers, use polygon_crypto_snapshots instead.
    """
    logger.info(f"Fetching crypto snapshot for ticker: {ticker}")

    try:
        # Get snapshot for single ticker
        # API call: GET /v2/snapshot/locale/global/markets/crypto/tickers/{ticker}
        snapshot = polygon_client.get_snapshot_ticker("crypto", ticker)

        if not snapshot:
            logger.warning(f"No snapshot data returned for ticker: {ticker}")
            return pd.DataFrame()

        # Extract ticker object from response
        ticker_data = getattr(snapshot, 'ticker', None)
        if not ticker_data:
            logger.error(f"Response missing 'ticker' object for {ticker}")
            return pd.DataFrame()

        # Initialize record with basic info
        record = {
            'ticker': getattr(ticker_data, 'ticker', ticker),
            'updated': getattr(ticker_data, 'updated', None),
            'todays_change': getattr(ticker_data, 'todaysChange', None),
            'todays_change_perc': getattr(ticker_data, 'todaysChangePerc', None),
        }

        # Extract day (current day) data
        day = getattr(ticker_data, 'day', None)
        if day:
            record['day_open'] = getattr(day, 'o', None)
            record['day_high'] = getattr(day, 'h', None)
            record['day_low'] = getattr(day, 'l', None)
            record['day_close'] = getattr(day, 'c', None)
            record['day_volume'] = getattr(day, 'v', None)
            record['day_vwap'] = getattr(day, 'vw', None)
        else:
            record['day_open'] = None
            record['day_high'] = None
            record['day_low'] = None
            record['day_close'] = None
            record['day_volume'] = None
            record['day_vwap'] = None

        # Extract prev_day (previous day) data
        prev_day = getattr(ticker_data, 'prevDay', None)
        if prev_day:
            record['prev_day_open'] = getattr(prev_day, 'o', None)
            record['prev_day_high'] = getattr(prev_day, 'h', None)
            record['prev_day_low'] = getattr(prev_day, 'l', None)
            record['prev_day_close'] = getattr(prev_day, 'c', None)
            record['prev_day_volume'] = getattr(prev_day, 'v', None)
            record['prev_day_vwap'] = getattr(prev_day, 'vw', None)
        else:
            record['prev_day_open'] = None
            record['prev_day_high'] = None
            record['prev_day_low'] = None
            record['prev_day_close'] = None
            record['prev_day_volume'] = None
            record['prev_day_vwap'] = None

        # Extract lastTrade (last trade) data
        last_trade = getattr(ticker_data, 'lastTrade', None)
        if last_trade:
            record['last_trade_price'] = getattr(last_trade, 'p', None)
            record['last_trade_size'] = getattr(last_trade, 's', None)
            record['last_trade_exchange'] = getattr(last_trade, 'x', None)
            record['last_trade_timestamp'] = getattr(last_trade, 't', None)
            record['last_trade_conditions'] = str(getattr(last_trade, 'c', None))
            record['last_trade_id'] = getattr(last_trade, 'i', None)
        else:
            record['last_trade_price'] = None
            record['last_trade_size'] = None
            record['last_trade_exchange'] = None
            record['last_trade_timestamp'] = None
            record['last_trade_conditions'] = None
            record['last_trade_id'] = None

        # Extract min (latest minute bar) data
        min_bar = getattr(ticker_data, 'min', None)
        if min_bar:
            record['min_open'] = getattr(min_bar, 'o', None)
            record['min_high'] = getattr(min_bar, 'h', None)
            record['min_low'] = getattr(min_bar, 'l', None)
            record['min_close'] = getattr(min_bar, 'c', None)
            record['min_volume'] = getattr(min_bar, 'v', None)
            record['min_vwap'] = getattr(min_bar, 'vw', None)
            record['min_timestamp'] = getattr(min_bar, 't', None)
            record['min_transactions'] = getattr(min_bar, 'n', None)
        else:
            record['min_open'] = None
            record['min_high'] = None
            record['min_low'] = None
            record['min_close'] = None
            record['min_volume'] = None
            record['min_vwap'] = None
            record['min_timestamp'] = None
            record['min_transactions'] = None

        df = pd.DataFrame([record])

        # Convert timestamp columns to datetime
        if not df.empty:
            # Convert updated timestamp (milliseconds)
            if 'updated' in df.columns:
                df['updated'] = pd.to_datetime(df['updated'], unit='ms', errors='coerce')

            # Convert last_trade_timestamp (milliseconds)
            if 'last_trade_timestamp' in df.columns:
                df['last_trade_timestamp'] = pd.to_datetime(df['last_trade_timestamp'], unit='ms', errors='coerce')

            # Convert min_timestamp (milliseconds)
            if 'min_timestamp' in df.columns:
                df['min_timestamp'] = pd.to_datetime(df['min_timestamp'], unit='ms', errors='coerce')

        logger.info(f"Successfully fetched snapshot for {ticker}")
        return df

    except Exception as e:
        logger.error(f"Error fetching crypto snapshot for {ticker}: {e}")
        raise


def register_polygon_crypto_snapshot_ticker(local_mcp_instance, local_polygon_client, csv_dir):
    """Register the polygon_crypto_snapshot_ticker tool"""
    @local_mcp_instance.tool()
    def polygon_crypto_snapshot_ticker(
        ticker: str
    ) -> str:
        """
        Fetch single ticker snapshot data for a specific cryptocurrency and save to CSV.

        This endpoint retrieves the most recent market data snapshot for a single ticker,
        consolidating the latest trade and aggregated data (minute, day, and previous day)
        for the specified ticker. Snapshot data is cleared at 12:00 AM EST and begins
        updating as exchanges report new information. By focusing on a single ticker,
        users can closely monitor real-time developments and incorporate up-to-date
        information into trading strategies, alerts, or ticker-level reporting.

        Parameters:
            ticker (str, required): Cryptocurrency ticker symbol in X:BASEUSD format.
                Examples:
                - "X:BTCUSD" (Bitcoin/USD)
                - "X:ETHUSD" (Ethereum/USD)
                - "X:SOLUSD" (Solana/USD)
                - "X:ADAUSD" (Cardano/USD)
                - "X:DOGEUSD" (Dogecoin/USD)
                - "X:MATICUSD" (Polygon/USD)
                - "X:AVAXUSD" (Avalanche/USD)
                - "X:LINKUSD" (Chainlink/USD)

                Note: The 'X:' prefix indicates crypto exchange aggregation

        Returns:
            str: Formatted response with CSV file info, schema, sample data, and Python snippet.

        CSV Output Columns:
            Core Identification & Metrics:
            - ticker (str): Cryptocurrency ticker symbol (e.g., "X:BTCUSD")
            - updated (datetime): Last update timestamp in 'YYYY-MM-DD HH:MM:SS' format
            - todays_change (float): Absolute price change from previous close (USD)
            - todays_change_perc (float): Percentage price change from previous close

            Current Day (Today) Data:
            - day_open (float): Today's opening price in USD
            - day_high (float): Today's highest price in USD
            - day_low (float): Today's lowest price in USD
            - day_close (float): Today's most recent close/current price in USD
            - day_volume (float): Today's total trading volume (base currency units)
            - day_vwap (float): Today's Volume Weighted Average Price

            Previous Day Data:
            - prev_day_open (float): Previous day's opening price in USD
            - prev_day_high (float): Previous day's highest price in USD
            - prev_day_low (float): Previous day's lowest price in USD
            - prev_day_close (float): Previous day's closing price in USD
            - prev_day_volume (float): Previous day's total trading volume
            - prev_day_vwap (float): Previous day's Volume Weighted Average Price

            Last Trade Information:
            - last_trade_price (float): Most recent trade price in USD
            - last_trade_size (float): Most recent trade size (base currency)
            - last_trade_exchange (int): Exchange ID where last trade occurred
            - last_trade_timestamp (datetime): Last trade timestamp in 'YYYY-MM-DD HH:MM:SS'
            - last_trade_conditions (str): Trade condition codes (JSON array as string)
            - last_trade_id (str): Unique identifier for the last trade

            Latest Minute Bar Data:
            - min_open (float): Latest minute's opening price
            - min_high (float): Latest minute's highest price
            - min_low (float): Latest minute's lowest price
            - min_close (float): Latest minute's closing price
            - min_volume (float): Latest minute's trading volume
            - min_vwap (float): Latest minute's Volume Weighted Average Price
            - min_timestamp (datetime): Minute bar timestamp in 'YYYY-MM-DD HH:MM:SS'
            - min_transactions (int): Number of transactions in the latest minute

        Use Cases:
            - **Focused Monitoring**: Track a single crypto asset with maximum detail
            - **Real-time Analysis**: Monitor price movements and volume for trading decisions
            - **Price Alerts**: Set up alerts based on specific price thresholds or changes
            - **Investor Relations**: Provide detailed, up-to-date information for stakeholders
            - **Trading Execution**: Assess current market conditions before placing orders
            - **Performance Tracking**: Compare today's metrics vs. previous day baseline
            - **Market Research**: Deep dive into specific crypto's trading characteristics
            - **Algorithm Testing**: Feed precise, single-ticker data into trading algorithms

        Important Notes:
            - Snapshot data resets daily at 12:00 AM EST (midnight Eastern Time)
            - Data begins repopulating from 4:00 AM EST as exchanges report trades
            - Crypto markets trade 24/7, so snapshots continuously update
            - Returns detailed data for ONE ticker only
            - For multiple tickers, use polygon_crypto_snapshots tool instead
            - VWAP is critical for evaluating execution quality
            - Exchange IDs can be mapped using polygon_crypto_exchanges tool
            - Trade conditions are encoded as integer arrays (see Polygon docs)

        Performance Tips:
            - Use this endpoint when you need detailed focus on 1 specific crypto
            - Use polygon_crypto_snapshots for batch monitoring of multiple cryptos
            - Poll regularly (e.g., every 30-60 seconds) for near-real-time tracking
            - Combine with polygon_crypto_aggregates for historical context
            - Use with polygon_news to correlate price changes with news events

        Example Workflow:
            1. Call polygon_crypto_snapshot_ticker(ticker="X:BTCUSD")
            2. Receive CSV filename with detailed Bitcoin snapshot data
            3. Use py_eval to load CSV and analyze metrics
            4. Example analysis: check if BTC price > day_vwap (bullish signal)
            5. Compare today's_change_perc to identify momentum

        Always use py_eval tool to analyze the saved CSV file for insights.

        Example usage:
            polygon_crypto_snapshot_ticker(ticker="X:BTCUSD")
        """
        logger.info(f"polygon_crypto_snapshot_ticker invoked for ticker: {ticker}")

        # Validate ticker parameter
        if not ticker or not ticker.strip():
            return "Error: 'ticker' parameter is required. Please provide a cryptocurrency ticker in X:BASEUSD format (e.g., 'X:BTCUSD')"

        ticker = ticker.strip()

        try:
            # Fetch snapshot for single ticker
            df = fetch_crypto_snapshot_ticker(
                polygon_client=local_polygon_client,
                ticker=ticker,
            )

            if df.empty:
                return f"No snapshot data found for ticker '{ticker}'.\n\nPossible causes:\n1. Ticker format might be incorrect (should be X:BASEUSD, e.g., X:BTCUSD)\n2. Ticker might not exist or be valid\n3. Snapshot data might not be available yet (resets at 12 AM EST)\n4. This endpoint may require a paid Polygon.io plan\n\nPlease verify the ticker symbol and try again."

            # Generate filename with ticker in name
            ticker_clean = ticker.replace(':', '_')
            filename = f"crypto_snapshot_{ticker_clean}_{uuid.uuid4().hex[:8]}.csv"
            filepath = csv_dir / filename

            # Save to CSV
            df.to_csv(filepath, index=False)
            logger.info(f"Saved crypto snapshot to {filename}")

            # Return formatted response
            return format_csv_response(filepath, df)

        except Exception as e:
            logger.error(f"Error in polygon_crypto_snapshot_ticker: {e}")
            return f"Error fetching crypto snapshot for ticker '{ticker}': {str(e)}"
