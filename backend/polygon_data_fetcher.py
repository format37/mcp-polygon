#!/usr/bin/env python3
"""
Polygon.io Data Fetcher

This script fetches data from two reliable Polygon.io endpoints:
1. Ticker details (company information, market cap, sector, etc.)
2. Price aggregates (historical price data)

The data is combined into DataFrames that can be saved as CSV for analysis.
"""

import os
import pandas as pd
from polygon import RESTClient
from datetime import datetime, timedelta
from typing import List, Optional
import logging
import time
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# API configuration
API_KEY = (
    os.getenv("POLYGON_API_KEY")
    or os.getenv("POLYGON_KEY")
    or "YOUR_POLYGON_KEY_HERE"
)

if API_KEY == "YOUR_POLYGON_KEY_HERE":
    logger.error("Please set POLYGON_API_KEY or POLYGON_KEY environment variable")
    exit(1)

masked_key = f"{API_KEY[:4]}***" if API_KEY else "<not set>"
logger.info("Using API Key: %s", masked_key)

client = RESTClient(API_KEY)


def create_output_folder(base_name: str = "polygon_output") -> Path:
    """
    Create timestamped output folder for saving CSV files.

    Returns:
        Path to the created output folder
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = Path(f"{base_name}_{timestamp}")
    output_dir.mkdir(exist_ok=True)
    logger.info(f"Created output directory: {output_dir}")
    return output_dir


def fetch_ticker_details(tickers: List[str], output_dir: Optional[Path] = None) -> pd.DataFrame:
    """
    Fetch comprehensive ticker details including company info, market cap, sector, etc.

    Args:
        tickers: List of ticker symbols (e.g., ['AAPL', 'MSFT'])
        output_dir: Optional output directory to save CSV file

    Returns:
        DataFrame with ticker details
    """
    logger.info(f"Fetching ticker details for {len(tickers)} tickers")

    records = []
    for i, ticker in enumerate(tickers):
        try:
            logger.info(f"Processing {i+1}/{len(tickers)}: {ticker}")

            # Get ticker details
            details = client.get_ticker_details(ticker)

            record = {
                'ticker': ticker,
                'name': getattr(details, 'name', None),
                'market_cap': getattr(details, 'market_cap', None),
                'share_class_shares_outstanding': getattr(details, 'share_class_shares_outstanding', None),
                'weighted_shares_outstanding': getattr(details, 'weighted_shares_outstanding', None),
                'primary_exchange': getattr(details, 'primary_exchange', None),
                'type': getattr(details, 'type', None),
                'active': getattr(details, 'active', None),
                'currency_name': getattr(details, 'currency_name', None),
                'cik': getattr(details, 'cik', None),
                'composite_figi': getattr(details, 'composite_figi', None),
                'share_class_figi': getattr(details, 'share_class_figi', None),
                'locale': getattr(details, 'locale', None),
                'description': getattr(details, 'description', None),
                'homepage_url': getattr(details, 'homepage_url', None),
                'total_employees': getattr(details, 'total_employees', None),
                'list_date': getattr(details, 'list_date', None),
                'logo_url': getattr(details, 'logo_url', None),
                'icon_url': getattr(details, 'icon_url', None),
                'sic_code': getattr(details, 'sic_code', None),
                'sic_description': getattr(details, 'sic_description', None),
                'ticker_root': getattr(details, 'ticker_root', None),
                'phone_number': getattr(details, 'phone_number', None),
            }

            # Add address information if available
            address = getattr(details, 'address', None)
            if address:
                record['address_1'] = getattr(address, 'address1', None)
                record['city'] = getattr(address, 'city', None)
                record['state'] = getattr(address, 'state', None)
                record['postal_code'] = getattr(address, 'postal_code', None)
            else:
                record['address_1'] = None
                record['city'] = None
                record['state'] = None
                record['postal_code'] = None

            # Add branding information if available
            branding = getattr(details, 'branding', None)
            if branding:
                record['logo_url'] = getattr(branding, 'logo_url', record['logo_url'])
                record['icon_url'] = getattr(branding, 'icon_url', record['icon_url'])

            records.append(record)

            # Rate limiting - be respectful
            time.sleep(0.1)

        except Exception as e:
            logger.error(f"Error fetching details for {ticker}: {e}")
            # Add a record with minimal info so we don't lose the ticker
            records.append({
                'ticker': ticker,
                'name': None,
                'market_cap': None,
                'error': str(e)
            })

    df = pd.DataFrame(records)

    # Convert date columns
    if not df.empty and 'list_date' in df.columns:
        df['list_date'] = pd.to_datetime(df['list_date'], errors='coerce')

    logger.info(f"Successfully fetched details for {len(df)} tickers")

    # Save to CSV if output directory is provided
    if output_dir and not df.empty:
        csv_file = output_dir / "ticker_details.csv"
        df.to_csv(csv_file, index=False)
        logger.info(f"Saved ticker details to {csv_file}")

    return df


def fetch_price_data(tickers: List[str],
                    from_date: Optional[str] = None,
                    to_date: Optional[str] = None,
                    timespan: str = "day",
                    multiplier: int = 1,
                    output_dir: Optional[Path] = None) -> pd.DataFrame:
    """
    Fetch historical price data for tickers.

    Args:
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
            aggs = client.list_aggs(
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


def fetch_crypto_aggregates(ticker: str,
                           from_date: Optional[str] = None,
                           to_date: Optional[str] = None,
                           timespan: str = "hour",
                           multiplier: int = 1,
                           limit: int = 50000,
                           output_dir: Optional[Path] = None) -> pd.DataFrame:
    """
    Fetch historical crypto aggregate bars (OHLCV data) for a cryptocurrency pair.

    Args:
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
        from_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    if to_date is None:
        to_date = datetime.now().strftime('%Y-%m-%d')

    logger.info(f"Fetching crypto aggregates for {ticker} from {from_date} to {to_date}")
    logger.info(f"Parameters: timespan={timespan}, multiplier={multiplier}, limit={limit}")

    records = []
    try:
        # Get crypto price aggregates
        aggs = client.list_aggs(
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
        return pd.DataFrame()

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


def create_combined_analysis(tickers: List[str],
                           from_date: Optional[str] = None,
                           to_date: Optional[str] = None,
                           output_dir: Optional[Path] = None) -> pd.DataFrame:
    """
    Create a combined analysis DataFrame with ticker details and price metrics.

    Args:
        tickers: List of ticker symbols
        from_date: Start date for price data
        to_date: End date for price data
        output_dir: Optional output directory to save CSV files

    Returns:
        Combined DataFrame with both ticker details and price metrics
    """
    logger.info("Creating combined analysis")

    # Fetch data from both endpoints
    details_df = fetch_ticker_details(tickers, output_dir)
    price_df = fetch_price_data(tickers, from_date, to_date, output_dir=output_dir)

    # Calculate price metrics
    metrics_df = calculate_price_metrics(price_df)

    # Combine the data
    if not details_df.empty and not metrics_df.empty:
        combined_df = details_df.merge(metrics_df, on='ticker', how='outer')
    elif not details_df.empty:
        combined_df = details_df
    elif not metrics_df.empty:
        combined_df = metrics_df
    else:
        combined_df = pd.DataFrame()

    # Save combined analysis if output directory is provided
    if output_dir and not combined_df.empty:
        csv_file = output_dir / "combined_analysis.csv"
        combined_df.to_csv(csv_file, index=False)
        logger.info(f"Saved combined analysis to {csv_file}")

    # Save price metrics separately if available
    if output_dir and not metrics_df.empty:
        csv_file = output_dir / "price_metrics.csv"
        metrics_df.to_csv(csv_file, index=False)
        logger.info(f"Saved price metrics to {csv_file}")

    logger.info(f"Combined analysis complete with {len(combined_df)} records")
    return combined_df


def main():
    """Main function to demonstrate the data fetcher."""

    # Example tickers - mix of large caps and different sectors
    tickers = [
        'AAPL',  # Technology
        'MSFT',  # Technology
        'GOOGL', # Technology
        'TSLA',  # Automotive/Technology
        'NVDA',  # Semiconductors
        'JPM',   # Financial
        'JNJ',   # Healthcare
        'PG',    # Consumer Goods
        'KO',    # Beverages
        'DIS'    # Entertainment
    ]

    # Set date range for price data (last 60 days)
    to_date = datetime.now().strftime('%Y-%m-%d')
    from_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')

    logger.info(f"Starting analysis for tickers: {', '.join(tickers)}")
    logger.info(f"Price data range: {from_date} to {to_date}")

    # Create output directory
    output_dir = create_output_folder()

    # Create combined analysis
    combined_df = create_combined_analysis(tickers, from_date, to_date, output_dir)

    if not combined_df.empty:
        # Display summary
        print("\n" + "="*50)
        print("ANALYSIS SUMMARY")
        print("="*50)
        print(f"Total tickers analyzed: {len(combined_df)}")
        print(f"Date range: {from_date} to {to_date}")
        print(f"Output directory: {output_dir}")
        print(f"Files created:")
        for file in output_dir.glob("*.csv"):
            print(f"  - {file.name}")

        print(f"\nAll files saved to: {output_dir.absolute()}")

        # Show sample of the data
        print("\nSample data (first 5 rows):")
        print(combined_df.head().to_string())

        # Show some basic statistics
        if 'total_return' in combined_df.columns:
            print(f"\nReturn statistics:")
            print(f"Average return: {combined_df['total_return'].mean():.2%}")
            print(f"Best performer: {combined_df.loc[combined_df['total_return'].idxmax(), 'ticker']} ({combined_df['total_return'].max():.2%})")
            print(f"Worst performer: {combined_df.loc[combined_df['total_return'].idxmin(), 'ticker']} ({combined_df['total_return'].min():.2%})")

        if 'market_cap' in combined_df.columns:
            print(f"\nMarket cap statistics:")
            print(f"Average market cap: ${combined_df['market_cap'].mean():,.0f}")
            print(f"Largest company: {combined_df.loc[combined_df['market_cap'].idxmax(), 'ticker']} (${combined_df['market_cap'].max():,.0f})")

    else:
        logger.error("No data was successfully fetched")


if __name__ == "__main__":
    main()