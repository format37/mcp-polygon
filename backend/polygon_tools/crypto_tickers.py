import logging
import uuid
import pandas as pd
from typing import Optional
from pathlib import Path
from polygon import RESTClient
from mcp_service import format_csv_response

logger = logging.getLogger(__name__)


def fetch_crypto_tickers(
    polygon_client: RESTClient,
    ticker: Optional[str] = None,
    search: Optional[str] = None,
    active: Optional[bool] = True,
    limit: int = 1000,
) -> pd.DataFrame:
    """
    Fetch cryptocurrency ticker symbols and metadata from Polygon.io.

    This endpoint retrieves a comprehensive list of crypto ticker symbols with details
    like name, currency information, active status, and more. Essential for discovering
    available crypto assets and building trading symbol lists.

    Args:
        polygon_client: Initialized Polygon RESTClient
        ticker: Specific ticker symbol to search for (e.g., "X:BTCUSD")
        search: Search terms within ticker or asset name (e.g., "BTC", "ethereum")
        active: Filter for actively traded tickers only (default: True)
        limit: Maximum number of results to return (max: 1000)

    Returns:
        DataFrame with crypto ticker metadata including symbols, names, currencies, etc.

    Note:
        This is reference/metadata data useful for asset discovery and symbol mapping.
        Use for building watchlists and understanding available crypto instruments.
    """
    logger.info(
        f"Fetching crypto tickers: ticker={ticker or 'all'}, "
        f"search={search or 'none'}, active={active}, limit={limit}"
    )

    records = []
    try:
        # Build kwargs for the API call
        kwargs = {
            'market': 'crypto',
            'limit': min(limit, 1000),  # Enforce max limit
            'active': active,
        }

        if ticker:
            kwargs['ticker'] = ticker

        if search:
            kwargs['search'] = search

        # Fetch tickers from Polygon API using iterator
        for t in polygon_client.list_tickers(**kwargs):
            # Extract all available fields
            record = {
                'ticker': getattr(t, 'ticker', None),
                'name': getattr(t, 'name', None),
                'market': getattr(t, 'market', None),
                'locale': getattr(t, 'locale', None),
                'primary_exchange': getattr(t, 'primary_exchange', None),
                'type': getattr(t, 'type', None),
                'active': getattr(t, 'active', None),
                'currency_symbol': getattr(t, 'currency_symbol', None),
                'currency_name': getattr(t, 'currency_name', None),
                'base_currency_symbol': getattr(t, 'base_currency_symbol', None),
                'base_currency_name': getattr(t, 'base_currency_name', None),
                'cik': getattr(t, 'cik', None),
                'composite_figi': getattr(t, 'composite_figi', None),
                'share_class_figi': getattr(t, 'share_class_figi', None),
                'last_updated_utc': getattr(t, 'last_updated_utc', None),
                'delisted_utc': getattr(t, 'delisted_utc', None),
            }
            records.append(record)

    except Exception as e:
        logger.error(f"Error fetching crypto tickers: {e}")
        raise

    df = pd.DataFrame(records)

    logger.info(f"Successfully fetched {len(df)} crypto tickers")

    return df


def register_polygon_crypto_tickers(local_mcp_instance, local_polygon_client, csv_dir):
    """Register the polygon_crypto_tickers tool"""
    @local_mcp_instance.tool()
    def polygon_crypto_tickers(
        ticker: str = "",
        search: str = "",
        active: str = "true",
        limit: int = 1000,
    ) -> str:
        """
        Fetch cryptocurrency ticker symbols and metadata, save to CSV file.

        This endpoint retrieves a comprehensive list of crypto ticker symbols with essential
        details including name, currency information, trading status, and identifiers. Use this
        to discover available crypto assets, build trading symbol lists, and understand the
        crypto market coverage.

        Parameters:
            ticker (str, optional): Specific ticker symbol to search for.
                Format: "X:BTCUSD" (Polygon crypto format with X: prefix)
                Examples:
                - "X:BTCUSD": Bitcoin vs US Dollar
                - "X:ETHUSD": Ethereum vs US Dollar
                - "X:DOGEUSD": Dogecoin vs US Dollar
                Default: "" (returns all crypto tickers)

            search (str, optional): Search terms to filter tickers by name or symbol.
                Case-insensitive partial matching against ticker symbol and asset name.
                Examples:
                - "BTC": Returns Bitcoin-related tickers (BTCUSD, BTCEUR, etc.)
                - "ethereum": Returns Ethereum-related pairs
                - "USD": Returns all USD-denominated crypto pairs
                Default: "" (no search filter)

            active (str, optional): Filter by trading status.
                Options: "true" (only active), "false" (only delisted), "" (all)
                - "true": Only actively traded crypto pairs (recommended for trading)
                - "false": Only delisted/inactive pairs (for historical analysis)
                - "": All tickers regardless of status
                Default: "true" (active only)

            limit (int, optional): Maximum number of ticker results to return.
                Range: 1-1000
                Default: 1000 (fetch maximum available)
                Note: Use lower limits (10-100) for quick discovery, higher for comprehensive lists

        Returns:
            str: Formatted response with CSV file info, schema, sample data, and Python snippet.

        CSV Output Structure:
            - ticker (str): Exchange symbol (format: "X:BASECUR" for crypto)
                Example: "X:BTCUSD" means Bitcoin priced in US Dollars
            - name (str): Full name of the crypto asset or currency pair
                Example: "Bitcoin / US Dollar"
            - market (str): Market type (always "crypto" for this endpoint)
            - locale (str): Geographical locale ("us" or "global")
            - primary_exchange (str): Primary exchange where this pair is traded
                Examples: "Coinbase", "Binance", "Kraken"
            - type (str): Asset type classification
                Common values: "CS" (Crypto Security), "CRYPTO" (Cryptocurrency)
            - active (bool): Trading status
                - True: Currently actively traded
                - False: Delisted or inactive
            - currency_symbol (str): ISO 4217 currency code for the quote currency
                Example: "USD", "EUR", "BTC"
            - currency_name (str): Full name of the quote currency
                Example: "US Dollar", "Euro", "Bitcoin"
            - base_currency_symbol (str): ISO code for the base cryptocurrency
                Example: "BTC", "ETH", "DOGE"
            - base_currency_name (str): Full name of the base cryptocurrency
                Example: "Bitcoin", "Ethereum", "Dogecoin"
            - cik (str): CIK number (if applicable, mostly null for crypto)
            - composite_figi (str): OpenFIGI composite identifier (if available)
            - share_class_figi (str): OpenFIGI share class identifier (if available)
            - last_updated_utc (str): Timestamp when information was last updated
                Format: ISO 8601 datetime string
            - delisted_utc (str): Timestamp when ticker was delisted (null if active)

        Use Cases:
            - **Asset Discovery**: Find all available crypto trading pairs for your strategy
            - **Watchlist Creation**: Build custom crypto watchlists based on search criteria
            - **Symbol Mapping**: Create lookup tables mapping symbols to full names
            - **Market Coverage Analysis**: Understand what crypto assets are available
            - **Trading Pair Selection**: Identify specific currency pairs for trading algorithms
            - **Data Integration**: Get ticker metadata for enriching market data
            - **Portfolio Construction**: Discover crypto assets for diversified portfolios
            - **Historical Analysis**: Include delisted tickers for complete historical studies

        Crypto Ticker Format:
            Polygon uses "X:" prefix for crypto tickers:
            - Format: X:BASECUR (e.g., X:BTCUSD = Bitcoin in US Dollars)
            - BASE: The cryptocurrency (BTC, ETH, DOGE, etc.)
            - CUR: The quote currency (USD, EUR, BTC, etc.)
            - This format is consistent across all Polygon crypto endpoints

        Trading Strategy Applications:
            1. **Discovery**: Find all BTC pairs → Use search="BTC"
            2. **USD Pairs**: Find all USD-denominated pairs → Use search="USD"
            3. **Active Trading**: Get currently tradable pairs → Use active="true"
            4. **Quick Scan**: Get top 50 cryptos → Use limit=50
            5. **Full Market**: Get all crypto tickers → Use default parameters

        Important Notes:
            - Returns reference/metadata, not price data (use price endpoints for quotes)
            - Ticker format "X:BASECUR" is Polygon-specific (differs from exchange formats)
            - 'active=true' recommended for live trading to avoid delisted assets
            - Use 'search' parameter for flexible filtering by symbol or name
            - Limit affects performance: lower limits return faster
            - Some tickers may have null values for optional fields (FIGI, CIK)

        Integration Workflow:
            1. Call this tool to get available crypto tickers
            2. Use py_eval to load CSV and filter/analyze ticker list
            3. Extract ticker symbols for use in price/trade data endpoints
            4. Build watchlists or symbol databases for trading applications

        Always use py_eval tool to analyze the saved CSV file for ticker discovery and filtering.

        Example usage:
            polygon_crypto_tickers(search="BTC", active="true", limit=20)
        """
        logger.info(
            f"polygon_crypto_tickers invoked: ticker={ticker or 'all'}, "
            f"search={search or 'none'}, active={active}, limit={limit}"
        )

        try:
            # Convert string parameters to proper types
            ticker_param = ticker if ticker else None
            search_param = search if search else None

            # Convert active string to boolean (handle empty string as None)
            if active.lower() == "true":
                active_param = True
            elif active.lower() == "false":
                active_param = False
            else:
                active_param = None

            # Fetch crypto tickers
            df = fetch_crypto_tickers(
                polygon_client=local_polygon_client,
                ticker=ticker_param,
                search=search_param,
                active=active_param,
                limit=limit,
            )

            if df.empty:
                return "No crypto tickers found for the specified criteria."

            # Always save to CSV file
            filename = f"crypto_tickers_{uuid.uuid4().hex[:8]}.csv"
            filepath = csv_dir / filename
            df.to_csv(filepath, index=False)
            logger.info(f"Saved crypto tickers to {filename} ({len(df)} records)")

            # Return formatted response
            return format_csv_response(filepath, df)

        except Exception as e:
            logger.error(f"Error in polygon_crypto_tickers: {e}")
            return f"Error fetching crypto tickers: {str(e)}"
