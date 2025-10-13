import logging
from pathlib import Path
from typing import Optional
import uuid
from mcp_service import format_csv_response
import pandas as pd
from polygon import RESTClient

logger = logging.getLogger(__name__)


def fetch_crypto_daily_open_close(
    polygon_client: RESTClient,
    ticker: str,
    date: str,
    adjusted: bool = True,
    output_dir: Optional[Path] = None
) -> pd.DataFrame:
    """
    Fetch daily open/close data for a specific cryptocurrency pair on a specific date.

    Args:
        polygon_client: Initialized Polygon RESTClient
        ticker: Crypto ticker symbol in format X:BASEUSD (e.g., "X:BTCUSD", "X:ETHUSD")
        date: Date in YYYY-MM-DD format
        adjusted: Whether or not results are adjusted for splits (default: True)
        output_dir: Optional output directory to save CSV file

    Returns:
        DataFrame with daily open/close data including OHLC and trade details

    Note:
        This endpoint provides the opening and closing trades for a specific crypto pair
        on a given date, enabling daily performance analysis and historical data collection.
    """
    logger.info(f"Fetching crypto daily open/close for {ticker} on {date}")

    try:
        # Get daily open/close data
        result = polygon_client.get_daily_open_close_agg(
            ticker=ticker,
            date=date,
            adjusted=adjusted
        )

        # Extract main data
        record = {
            'ticker': ticker,
            'date': date,
            'symbol': getattr(result, 'symbol', None),
            'open': getattr(result, 'open', None),
            'close': getattr(result, 'close', None),
            'high': getattr(result, 'high', None),
            'low': getattr(result, 'low', None),
            'volume': getattr(result, 'volume', None),
            'after_hours': getattr(result, 'after_hours', None),
            'pre_market': getattr(result, 'pre_market', None),
            'is_utc': getattr(result, 'isUTC', None),
        }

        # Process opening trades if available
        opening_trades = getattr(result, 'openTrades', None) or getattr(result, 'opening_trades', None)
        if opening_trades:
            record['num_opening_trades'] = len(opening_trades)
            # Sum up the trade sizes (volumes)
            record['opening_volume_total'] = sum(getattr(trade, 's', 0) or getattr(trade, 'size', 0) for trade in opening_trades)
            # Get the price of the first opening trade
            if len(opening_trades) > 0:
                first_trade = opening_trades[0]
                record['first_opening_trade_price'] = getattr(first_trade, 'p', None) or getattr(first_trade, 'price', None)
        else:
            record['num_opening_trades'] = 0
            record['opening_volume_total'] = 0
            record['first_opening_trade_price'] = None

        # Process closing trades if available
        closing_trades = getattr(result, 'closingTrades', None) or getattr(result, 'closing_trades', None)
        if closing_trades:
            record['num_closing_trades'] = len(closing_trades)
            # Sum up the trade sizes (volumes)
            record['closing_volume_total'] = sum(getattr(trade, 's', 0) or getattr(trade, 'size', 0) for trade in closing_trades)
            # Get the price of the last closing trade
            if len(closing_trades) > 0:
                last_trade = closing_trades[-1]
                record['last_closing_trade_price'] = getattr(last_trade, 'p', None) or getattr(last_trade, 'price', None)
        else:
            record['num_closing_trades'] = 0
            record['closing_volume_total'] = 0
            record['last_closing_trade_price'] = None

        df = pd.DataFrame([record])

        logger.info(f"Successfully fetched daily open/close data for {ticker} on {date}")

        # Save to CSV if output directory is provided
        if output_dir and not df.empty:
            csv_file = output_dir / f"crypto_daily_open_close_{ticker.replace(':', '_')}_{date}.csv"
            df.to_csv(csv_file, index=False)
            logger.info(f"Saved crypto daily open/close to {csv_file}")

        return df

    except Exception as e:
        logger.error(f"Error fetching crypto daily open/close for {ticker} on {date}: {e}")
        raise


def register_polygon_crypto_daily_open_close(local_mcp_instance, local_polygon_client, csv_dir):
    """Register the polygon_crypto_daily_open_close tool"""
    @local_mcp_instance.tool()
    def polygon_crypto_daily_open_close(
        ticker: str,
        date: str,
        adjusted: bool = True
    ) -> str:
        """
        Fetch daily open/close data for a specific cryptocurrency pair on a specific date and save to CSV file.

        This endpoint retrieves the opening and closing trades for a crypto pair, providing essential daily
        pricing details for performance evaluation, historical analysis, and trading activity insights.

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

            date (str, required): The specific date for which to fetch open/close data.
                Format: YYYY-MM-DD
                Examples: "2024-01-15", "2023-12-31"
                Note: Must be a past date (cannot fetch future dates)

            adjusted (bool, optional): Whether results are adjusted for splits.
                Default: True
                - True: Results are adjusted for any splits (recommended for analysis)
                - False: Raw, unadjusted results

        Returns:
            str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

        CSV Output Structure:
            - ticker (str): Cryptocurrency ticker symbol (e.g., "X:BTCUSD")
            - date (str): The requested date in 'YYYY-MM-DD' format
            - symbol (str): The symbol pair evaluated (e.g., "BTC-USD")
            - open (float): Opening price for the day in USD
            - close (float): Closing price for the day in USD
            - high (float): Highest price during the day in USD
            - low (float): Lowest price during the day in USD
            - volume (float): Total trading volume for the day (in base currency units)
            - after_hours (float): After hours trading price (if available)
            - pre_market (float): Pre-market trading price (if available)
            - is_utc (bool): Whether timestamps are in UTC timezone
            - num_opening_trades (int): Number of trades that constitute the opening price
            - opening_volume_total (float): Total volume of all opening trades
            - first_opening_trade_price (float): Price of the first opening trade
            - num_closing_trades (int): Number of trades that constitute the closing price
            - closing_volume_total (float): Total volume of all closing trades
            - last_closing_trade_price (float): Price of the last closing trade

        Use Cases:
            - **Daily Performance Analysis**: Compare open vs. close to determine daily gains/losses
            - **Historical Data Collection**: Build datasets for backtesting trading strategies
            - **Portfolio Tracking**: Monitor daily changes in crypto holdings
            - **Volatility Assessment**: Calculate daily price ranges (high - low) for risk analysis
            - **Trading Pattern Analysis**: Identify opening/closing trade patterns and volumes
            - **Price Discovery**: Understand how markets establish opening and closing prices
            - **Risk Management**: Analyze daily drawdowns and price movements
            - **Algorithm Development**: Train models on historical daily OHLC data

        Important Notes:
            - Crypto markets trade 24/7, so "open" and "close" refer to UTC day boundaries
            - Opening trades occur at the start of the UTC day (00:00:00 UTC)
            - Closing trades occur at the end of the UTC day (23:59:59 UTC)
            - Multiple trades may comprise the opening/closing prices
            - Trade counts and volumes provide liquidity insights
            - Data is typically available 1-2 days after the trading day
            - Use this for single-day snapshots; use polygon_crypto_aggregates for time series

        Example Workflow:
            1. Call this tool to fetch daily data for a specific date
            2. Receive CSV filename in response
            3. Use py_eval to load and analyze the CSV file
            4. Calculate metrics like daily return, volatility, trade analysis

        Always use py_eval tool to analyze the saved CSV file for trading decisions and insights.
        """
        logger.info(f"polygon_crypto_daily_open_close invoked: ticker={ticker}, date={date}, adjusted={adjusted}")

        try:
            # Fetch crypto daily open/close data
            df = fetch_crypto_daily_open_close(
                polygon_client=local_polygon_client,
                ticker=ticker,
                date=date,
                adjusted=adjusted,
                output_dir=None
            )

            if df.empty:
                return f"No daily open/close data were retrieved for {ticker} on {date}. Please check the ticker format (should be X:BASEUSD, e.g., X:BTCUSD) and ensure the date is valid and in the past."

            # Always save to CSV file
            # Clean ticker for filename (replace : with _)
            clean_ticker = ticker.replace(':', '_')
            clean_date = date.replace('-', '')
            filename = f"crypto_daily_open_close_{clean_ticker}_{clean_date}_{uuid.uuid4().hex[:8]}.csv"
            filepath = csv_dir / filename
            df.to_csv(filepath, index=False)
            logger.info(f"Saved crypto daily open/close to {filename} ({len(df)} records)")

            # Return formatted response
            return format_csv_response(filepath, df)

        except Exception as e:
            logger.error(f"Error in polygon_crypto_daily_open_close: {e}")
            return f"Error fetching crypto daily open/close: {str(e)}"
