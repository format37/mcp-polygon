import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional
import uuid
from mcp_service import format_csv_response
import pandas as pd
from polygon import RESTClient
import time

logger = logging.getLogger(__name__)


def fetch_price_data(
    polygon_client: RESTClient,
    tickers: List[str],
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    timespan: str = "day",
    multiplier: int = 1,
    output_dir: Optional[Path] = None
) -> pd.DataFrame:
    """
    Fetch historical price data for tickers.

    Args:
        polygon_client: Initialized Polygon RESTClient
        tickers: List of ticker symbols
        from_date: Start date in YYYY-MM-DD format (defaults to 30 days ago)
        to_date: End date in YYYY-MM-DD format (defaults to today)
        timespan: Time span (day, week, month, quarter, year)
        multiplier: Size of the time window
        output_dir: Optional output directory to save CSV file

    Returns:
        DataFrame with price data
    """
    if from_date is None:
        from_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    if to_date is None:
        to_date = datetime.now().strftime('%Y-%m-%d')

    logger.info(f"Fetching price data for {len(tickers)} tickers from {from_date} to {to_date}")

    records = []
    for i, ticker in enumerate(tickers):
        try:
            logger.info(f"Processing {i+1}/{len(tickers)}: {ticker}")

            # Get price aggregates
            aggs = polygon_client.list_aggs(
                ticker=ticker,
                multiplier=multiplier,
                timespan=timespan,
                from_=from_date,
                to=to_date,
                adjusted=True,
                limit=50000
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

            # Rate limiting
            time.sleep(0.1)

        except Exception as e:
            logger.error(f"Error fetching price data for {ticker}: {e}")

    df = pd.DataFrame(records)

    # Convert timestamp to datetime
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['date'] = df['timestamp'].dt.date

    logger.info(f"Successfully fetched {len(df)} price records")

    # Save to CSV if output directory is provided
    if output_dir and not df.empty:
        csv_file = output_dir / "price_data.csv"
        df.to_csv(csv_file, index=False)
        logger.info(f"Saved price data to {csv_file}")

    return df


def calculate_price_metrics(price_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate price-based metrics for each ticker.

    Args:
        price_df: DataFrame with price data

    Returns:
        DataFrame with calculated metrics per ticker
    """
    if price_df.empty:
        return pd.DataFrame()

    logger.info("Calculating price metrics")

    metrics = []
    for ticker in price_df['ticker'].unique():
        ticker_data = price_df[price_df['ticker'] == ticker].sort_values('timestamp')

        if len(ticker_data) < 2:
            continue

        # Basic metrics
        first_price = ticker_data.iloc[0]['close']
        last_price = ticker_data.iloc[-1]['close']
        high_price = ticker_data['high'].max()
        low_price = ticker_data['low'].min()
        avg_volume = ticker_data['volume'].mean()
        total_volume = ticker_data['volume'].sum()

        # Calculate returns
        total_return = (last_price / first_price - 1) if first_price != 0 else None

        # Calculate daily returns for volatility
        ticker_data = ticker_data.copy()
        ticker_data['daily_return'] = ticker_data['close'].pct_change()
        volatility = ticker_data['daily_return'].std()

        # Price range metrics
        price_range = (high_price - low_price) / last_price if last_price != 0 else None

        metrics.append({
            'ticker': ticker,
            'start_date': ticker_data.iloc[0]['date'],
            'end_date': ticker_data.iloc[-1]['date'],
            'trading_days': len(ticker_data),
            'start_price': first_price,
            'end_price': last_price,
            'high_price': high_price,
            'low_price': low_price,
            'total_return': total_return,
            'volatility': volatility,
            'avg_daily_volume': avg_volume,
            'total_volume': total_volume,
            'price_range_ratio': price_range,
        })

    return pd.DataFrame(metrics)


def register_polygon_price_data(local_mcp_instance, local_polygon_client, csv_dir):
    """Register the polygon_price_data and polygon_price_metrics tools"""

    @local_mcp_instance.tool()
    def polygon_price_data(
        tickers: List[str],
        from_date: str = "",
        to_date: str = "",
        timespan: str = "day",
        multiplier: int = 1
    ) -> str:
        """
        Fetch historical price data and save to CSV file.

        Parameters:
            tickers (List[str]): List of ticker symbols
            from_date (str): Start date in YYYY-MM-DD format (defaults to 30 days ago)
            to_date (str): End date in YYYY-MM-DD format (defaults to today)
            timespan (str): Time span (day, week, month, quarter, year)
            multiplier (int): Size of the time window

        Returns:
            str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

        CSV Output Structure:
            - ticker (str): Stock ticker symbol
            - timestamp (str): Trading session timestamp in 'YYYY-MM-DD HH:MM:SS' format
            - open (float): Opening price for the period
            - high (float): Highest price during the period
            - low (float): Lowest price during the period
            - close (float): Closing price for the period
            - volume (float): Total trading volume for the period
            - vwap (float): Volume Weighted Average Price
            - transactions (int): Number of transactions during the period
            - date (str): Trading date in 'YYYY-MM-DD' format

        Use this data for: Technical analysis, price trend analysis, volume analysis, OHLC charting, volatility calculations.
        Always use py_eval tool to analyze the saved CSV file.
        """
        logger.info(f"polygon_price_data invoked with {len(tickers)} tickers, dates {from_date} to {to_date}")

        try:
            # Set default dates if not provided
            from_date_param = from_date if from_date else None
            to_date_param = to_date if to_date else None

            # Fetch price data (no output_dir, we'll save separately)
            df = fetch_price_data(
                polygon_client=local_polygon_client,
                tickers=tickers,
                from_date=from_date_param,
                to_date=to_date_param,
                timespan=timespan,
                multiplier=multiplier,
                output_dir=None
            )

            if df.empty:
                return "No price data were retrieved"

            # Always save to CSV file
            filename = f"price_data_{uuid.uuid4().hex[:8]}.csv"
            filepath = csv_dir / filename
            df.to_csv(filepath, index=False)
            logger.info(f"Saved price data to {filename} ({len(df)} records)")

            # Return formatted response
            return format_csv_response(filepath, df)

        except Exception as e:
            logger.error(f"Error in polygon_price_data: {e}")
            return f"Error fetching price data: {str(e)}"

    @local_mcp_instance.tool()
    def polygon_price_metrics(
        tickers: List[str],
        from_date: str = "",
        to_date: str = ""
    ) -> str:
        """
        Calculate price-based metrics and save to CSV file.

        Parameters:
            tickers (List[str]): List of ticker symbols
            from_date (str): Start date in YYYY-MM-DD format (defaults to 30 days ago)
            to_date (str): End date in YYYY-MM-DD format (defaults to today)

        Returns:
            str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

        CSV Output Structure:
            - ticker (str): Stock ticker symbol
            - start_date (str): Analysis period start date in 'YYYY-MM-DD' format
            - end_date (str): Analysis period end date in 'YYYY-MM-DD' format
            - trading_days (int): Number of trading days in the period
            - start_price (float): Opening price on start date
            - end_price (float): Closing price on end date
            - high_price (float): Highest price during the period
            - low_price (float): Lowest price during the period
            - total_return (float): Total return as decimal (e.g., 0.12 = 12% gain)
            - volatility (float): Price volatility (standard deviation of daily returns)
            - avg_daily_volume (float): Average daily trading volume
            - total_volume (float): Total volume traded during period
            - price_range_ratio (float): (High - Low) / Low ratio indicating price range

        Use this data for: Risk analysis, performance comparison, volatility assessment, return calculations, portfolio optimization.
        Always use py_eval tool to analyze the saved CSV file.
        """
        logger.info(f"polygon_price_metrics invoked with {len(tickers)} tickers")

        try:
            # Set default dates if not provided
            from_date_param = from_date if from_date else None
            to_date_param = to_date if to_date else None

            # First fetch price data
            price_df = fetch_price_data(
                polygon_client=local_polygon_client,
                tickers=tickers,
                from_date=from_date_param,
                to_date=to_date_param
            )

            if price_df.empty:
                return "No price data available for metrics calculation"

            # Calculate metrics
            metrics_df = calculate_price_metrics(price_df)

            if metrics_df.empty:
                return "No metrics could be calculated"

            # Always save to CSV file
            filename = f"price_metrics_{uuid.uuid4().hex[:8]}.csv"
            filepath = csv_dir / filename
            metrics_df.to_csv(filepath, index=False)
            logger.info(f"Saved price metrics to {filename} ({len(metrics_df)} records)")

            # Return formatted response
            return format_csv_response(filepath, metrics_df)

        except Exception as e:
            logger.error(f"Error in polygon_price_metrics: {e}")
            return f"Error calculating price metrics: {str(e)}"
