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
from polygon_tools.ticker_details import fetch_ticker_details

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


def fetch_market_status(output_dir: Optional[Path] = None) -> pd.DataFrame:
    """
    Fetch current market status from Polygon.io.

    This endpoint retrieves the current trading status for various exchanges and overall
    financial markets, providing real-time indicators of whether markets are open, closed,
    or operating in pre-market/after-hours sessions.

    Args:
        output_dir: Optional output directory to save CSV file

    Returns:
        DataFrame with current market status including exchange and currency market states

    Note:
        This endpoint returns a snapshot of the current market status at the time of the call.
        Crypto markets trade 24/7, but traditional markets have specific hours.
    """
    logger.info("Fetching current market status")

    try:
        # Fetch market status
        status = client.get_market_status()

        # Build the record with flattened structure
        # Try both camelCase (JSON) and snake_case (Python SDK) naming conventions
        record = {
            'fetched_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'server_time': getattr(status, 'server_time', None) or getattr(status, 'serverTime', None),
            'market': getattr(status, 'market', None),
            'after_hours': getattr(status, 'after_hours', None) if getattr(status, 'after_hours', None) is not None else getattr(status, 'afterHours', None),
            'early_hours': getattr(status, 'early_hours', None) if getattr(status, 'early_hours', None) is not None else getattr(status, 'earlyHours', None),
        }

        # Extract currencies object
        currencies = getattr(status, 'currencies', None)
        if currencies:
            record['crypto_status'] = getattr(currencies, 'crypto', None)
            record['fx_status'] = getattr(currencies, 'fx', None)
        else:
            record['crypto_status'] = None
            record['fx_status'] = None

        # Extract exchanges object
        exchanges = getattr(status, 'exchanges', None)
        if exchanges:
            record['nasdaq_status'] = getattr(exchanges, 'nasdaq', None)
            record['nyse_status'] = getattr(exchanges, 'nyse', None)
            record['otc_status'] = getattr(exchanges, 'otc', None)
        else:
            record['nasdaq_status'] = None
            record['nyse_status'] = None
            record['otc_status'] = None

        # Extract indicesGroups object - handle all possible index groups dynamically
        # Try both naming conventions: indicesGroups (camelCase) and indices_groups (snake_case)
        indices_groups = getattr(status, 'indices_groups', None) or getattr(status, 'indicesGroups', None)
        if indices_groups:
            # List of known index groups from the documentation
            index_group_keys = ['cccy', 'cgi', 'dow_jones', 'ftse_russell', 'msci',
                               'mstar', 'mstarc', 'nasdaq', 's_and_p', 'societe_generale']
            for key in index_group_keys:
                record[f'index_{key}'] = getattr(indices_groups, key, None)
        else:
            # Add None for all index groups if not present
            index_group_keys = ['cccy', 'cgi', 'dow_jones', 'ftse_russell', 'msci',
                               'mstar', 'mstarc', 'nasdaq', 's_and_p', 'societe_generale']
            for key in index_group_keys:
                record[f'index_{key}'] = None

        df = pd.DataFrame([record])

        logger.info(f"Successfully fetched market status: market={record['market']}, crypto={record['crypto_status']}")

        # Save to CSV if output directory is provided
        if output_dir and not df.empty:
            csv_file = output_dir / "market_status.csv"
            df.to_csv(csv_file, index=False)
            logger.info(f"Saved market status to {csv_file}")

        return df

    except Exception as e:
        logger.error(f"Error fetching market status: {e}")
        return pd.DataFrame()
