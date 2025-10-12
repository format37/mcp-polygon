import logging
from pathlib import Path
from typing import List, Optional
import uuid
from mcp_service import format_csv_response
import pandas as pd
from polygon import RESTClient
import time

logger = logging.getLogger(__name__)


def fetch_ticker_details(
    polygon_client: RESTClient,
    tickers: List[str],
    output_dir: Optional[Path] = None
) -> pd.DataFrame:
    """
    Fetch comprehensive ticker details including company info, market cap, sector, etc.

    Args:
        polygon_client: Initialized Polygon RESTClient
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
            details = polygon_client.get_ticker_details(ticker)

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


def register_polygon_ticker_details(local_mcp_instance, local_polygon_client, csv_dir):
    """Register the polygon_ticker_details tool"""
    @local_mcp_instance.tool()
    def polygon_ticker_details(
        tickers: List[str]
    ) -> str:
        """
        Fetch comprehensive ticker details and save to CSV file.

        Parameters:
            tickers (List[str]): List of ticker symbols (e.g., ['AAPL', 'MSFT'])

        Returns:
            str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

        CSV Output Structure:
            - ticker (str): Stock ticker symbol
            - name (str): Company full name
            - market_cap (float): Market capitalization in USD
            - share_class_shares_outstanding (float): Number of shares outstanding for this share class
            - weighted_shares_outstanding (float): Weighted average shares outstanding
            - primary_exchange (str): Primary stock exchange code (e.g., 'XNAS' for NASDAQ)
            - type (str): Security type (e.g., 'CS' for Common Stock)
            - active (bool): Whether the ticker is actively traded
            - currency_name (str): Currency denomination (e.g., 'usd')
            - cik (str): SEC Central Index Key
            - composite_figi (str): Financial Instrument Global Identifier
            - share_class_figi (str): Share class specific FIGI
            - locale (str): Market locale (e.g., 'us')
            - description (str): Detailed company description
            - homepage_url (str): Company website URL
            - total_employees (int): Number of employees
            - list_date (str): IPO/listing date in YYYY-MM-DD format
            - logo_url (str): Company logo image URL
            - icon_url (str): Company icon image URL
            - sic_code (str): Standard Industrial Classification code
            - sic_description (str): SIC code description/industry name
            - ticker_root (str): Root ticker symbol
            - phone_number (str): Company contact phone number
            - address_1 (str): Street address
            - city (str): City
            - state (str): State/province
            - postal_code (str): ZIP/postal code

        Use Cases:
            - Fundamental analysis and company research
            - Portfolio construction and screening
            - Due diligence and investment research
            - Market analysis and sector comparison
            - Building company databases and reference tables

        Always use the py_eval tool to analyze the saved CSV file for insights.

        Example usage:
            polygon_ticker_details(tickers=["AAPL", "MSFT", "GOOGL"])
        """

        logger.info(f"polygon_ticker_details tool invoked with {len(tickers)} tickers")

        if not tickers:
            return "Error: tickers parameter is required and must not be empty"

        # Call fetch_ticker_details function
        df = fetch_ticker_details(
            polygon_client=local_polygon_client,
            tickers=tickers,
            output_dir=None  # We'll save separately
        )

        if df.empty:
            return "No ticker details were retrieved"

        # Generate filename
        filename = f"ticker_details_{uuid.uuid4().hex[:8]}.csv"
        filepath = csv_dir / filename

        # Save to CSV file
        df.to_csv(filepath, index=False)
        logger.info(f"Saved ticker details to {filename} ({len(df)} records)")

        # Return formatted response
        return format_csv_response(filepath, df)
