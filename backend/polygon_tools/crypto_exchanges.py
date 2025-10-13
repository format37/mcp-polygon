import logging
import uuid
import pandas as pd
from typing import Optional
from pathlib import Path
from polygon import RESTClient
from mcp_service import format_csv_response

logger = logging.getLogger(__name__)


def fetch_crypto_exchanges(
    polygon_client: RESTClient,
    locale: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch cryptocurrency exchange reference data from Polygon.io.

    This endpoint retrieves a list of known crypto exchanges including their identifiers,
    names, market types, and other relevant attributes. This information helps map exchange
    codes, understand market coverage, and integrate exchange details into applications.

    Args:
        polygon_client: Initialized Polygon RESTClient
        locale: Optional locale filter ("us" or "global")

    Returns:
        DataFrame with crypto exchange metadata

    Note:
        This is reference/metadata data that provides exchange information for mapping
        and understanding crypto market data. Fetch once and use as a lookup table.
    """
    logger.info(f"Fetching crypto exchanges" + (f" (locale={locale})" if locale else ""))

    records = []
    try:
        # Build kwargs for the API call
        kwargs = {'asset_class': 'crypto'}

        if locale:
            kwargs['locale'] = locale

        # Fetch exchanges from Polygon API
        exchanges = polygon_client.get_exchanges(**kwargs)

        for exchange in exchanges:
            # Extract all available fields
            record = {
                'id': getattr(exchange, 'id', None),
                'name': getattr(exchange, 'name', None),
                'acronym': getattr(exchange, 'acronym', None),
                'asset_class': getattr(exchange, 'asset_class', None),
                'locale': getattr(exchange, 'locale', None),
                'mic': getattr(exchange, 'mic', None),
                'operating_mic': getattr(exchange, 'operating_mic', None),
                'participant_id': getattr(exchange, 'participant_id', None),
                'type': getattr(exchange, 'type', None),
                'url': getattr(exchange, 'url', None),
            }
            records.append(record)

    except Exception as e:
        logger.error(f"Error fetching crypto exchanges: {e}")
        return pd.DataFrame()

    df = pd.DataFrame(records)

    logger.info(f"Successfully fetched {len(df)} crypto exchanges")

    return df


def register_polygon_crypto_exchanges(local_mcp_instance, local_polygon_client, csv_dir):
    """Register the polygon_crypto_exchanges tool"""
    @local_mcp_instance.tool()
    def polygon_crypto_exchanges(
        locale: str = ""
    ) -> str:
        """
        Fetch cryptocurrency exchange reference data and save to CSV file.

        This endpoint retrieves a list of known crypto exchanges including their identifiers,
        names, market types, and other relevant attributes. This reference information helps map
        exchange codes, understand market coverage, integrate exchange details into applications,
        and ensure regulatory compliance.

        Parameters:
            locale (str, optional): Filter exchanges by geographical location.
                Options: "us" (United States), "global" (international)
                Default: "" (returns all locales)
                Examples:
                - "us": Only exchanges operating in the United States
                - "global": Only international/global exchanges
                - "" (empty): All exchanges regardless of locale

        Returns:
            str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

        CSV Output Structure:
            - id (int): Unique identifier for this exchange in Polygon.io system
            - name (str): Full official name of the exchange
                Examples: "Binance", "Coinbase", "Kraken", "Bitfinex"
            - acronym (str): Commonly used abbreviation for the exchange
                Examples: "BNCE", "GDAX", "KRKN"
            - asset_class (str): Type of assets traded (always "crypto" for this endpoint)
            - locale (str): Geographical location identifier
                Values: "us" (United States) or "global" (international)
            - mic (str): Market Identifier Code following ISO 10383 standard
                Used for regulatory reporting and market identification
            - operating_mic (str): MIC of the entity that operates this exchange
                May differ from 'mic' for exchanges with multiple operating entities
            - participant_id (str): ID used by SIP (Securities Information Processor) to represent this exchange
                Used in data feeds and market data aggregation
            - type (str): Exchange classification type
                Values: "exchange", "TRF" (Trade Reporting Facility), "SIP" (Securities Information Processor)
            - url (str): Official website URL for the exchange (if available)
                Example: "https://www.binance.com"

        Use Cases:
            - **Exchange Mapping**: Create lookup tables to map exchange codes to full names
            - **Market Coverage Analysis**: Understand which exchanges are available in your data
            - **Application Development**: Display exchange options in trading applications
            - **Data Integration**: Integrate exchange details when processing trade/quote data
            - **Regulatory Compliance**: Use MIC codes for regulatory reporting requirements
            - **Exchange Selection**: Identify exchanges by locale for geographic-specific analysis
            - **Trading Strategy**: Filter data by specific exchanges for venue-specific strategies

        Reference Data Notes:
            - This is metadata/reference data, not time-series trading data
            - Exchange lists are relatively static; fetch once and cache for lookups
            - Use returned data as a lookup table when processing crypto trades/quotes
            - MIC codes are standard identifiers used across financial systems
            - Some exchanges may have multiple MIC codes for different services

        Integration Example:
            1. Call this tool once to get all crypto exchanges
            2. Use py_eval to load CSV and create exchange lookup dictionary
            3. When analyzing trade data, cross-reference exchange IDs with this reference
            4. Filter data by specific exchanges based on your trading strategy

        Important Notes:
            - This endpoint returns exchange metadata, not trading data
            - Exchange information helps interpret where trades occurred
            - Use 'mic' field for regulatory reporting and compliance
            - 'participant_id' helps identify exchange in market data feeds

        Always use py_eval tool to analyze the saved CSV file for exchange lookups and filtering.
        """
        logger.info(f"polygon_crypto_exchanges invoked" + (f": locale={locale}" if locale else ""))

        try:
            # Handle optional parameter (convert empty string to None)
            locale_param = locale if locale else None

            # Fetch crypto exchanges
            df = fetch_crypto_exchanges(
                polygon_client=local_polygon_client,
                locale=locale_param,
            )

            if df.empty:
                return "No crypto exchanges were retrieved. Please check your parameters."

            # Always save to CSV file
            filename = f"crypto_exchanges_{uuid.uuid4().hex[:8]}.csv"
            filepath = csv_dir / filename
            df.to_csv(filepath, index=False)
            logger.info(f"Saved crypto exchanges to {filename} ({len(df)} records)")

            # Return formatted response
            return format_csv_response(filepath, df)

        except Exception as e:
            logger.error(f"Error in polygon_crypto_exchanges: {e}")
            return f"Error fetching crypto exchanges: {str(e)}"
