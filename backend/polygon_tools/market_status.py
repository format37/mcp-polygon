import logging
import uuid
import pandas as pd
from datetime import datetime
from typing import Optional
from pathlib import Path
from polygon import RESTClient
from mcp_service import format_csv_response

logger = logging.getLogger(__name__)


def fetch_market_status(
    polygon_client: RESTClient,
    output_dir: Optional[Path] = None
) -> pd.DataFrame:
    """
    Fetch current market status from Polygon.io.

    This endpoint retrieves the current trading status for various exchanges and overall
    financial markets, providing real-time indicators of whether markets are open, closed,
    or operating in pre-market/after-hours sessions.

    Args:
        polygon_client: Initialized Polygon RESTClient
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
        status = polygon_client.get_market_status()

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
        raise


def register_polygon_market_status(local_mcp_instance, local_polygon_client, csv_dir):
    """Register the polygon_market_status tool"""
    @local_mcp_instance.tool()
    def polygon_market_status() -> str:
        """
        Fetch current market status across all exchanges and asset classes, and save to CSV file.

        This endpoint retrieves the current trading status for various exchanges and overall financial
        markets. It provides real-time indicators of whether markets are open, closed, or operating in
        pre-market/after-hours sessions, along with timing details for the current state.

        Parameters:
            None (endpoint automatically returns current status snapshot)

        Returns:
            str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

        CSV Output Structure:
            - fetched_at (str): Local timestamp when this data was fetched in 'YYYY-MM-DD HH:MM:SS' format
            - server_time (str): API server time in RFC3339 format (e.g., "2020-11-10T17:37:37-05:00")
                Use this as the authoritative timestamp for market status
            - market (str): Overall market status across all exchanges
                Values: "open", "closed", "extended-hours", "early-hours"
            - after_hours (bool): Whether markets are in post-market hours (after normal trading)
            - early_hours (bool): Whether markets are in pre-market hours (before normal trading)
            - crypto_status (str): Status of cryptocurrency markets
                Values: "open", "closed"
                Note: Crypto markets typically trade 24/7, so usually "open"
            - fx_status (str): Status of foreign exchange (forex) markets
                Values: "open", "closed"
                Note: FX markets trade nearly 24/7 on weekdays
            - nasdaq_status (str): Status of NASDAQ exchange
                Values: "open", "closed", "extended-hours", "early-close"
            - nyse_status (str): Status of New York Stock Exchange
                Values: "open", "closed", "extended-hours", "early-close"
            - otc_status (str): Status of Over-The-Counter markets
                Values: "open", "closed"
            - index_cccy (str): Status of Cboe Streaming Market Indices Cryptocurrency ("CCCY") indices
            - index_cgi (str): Status of Cboe Global Indices ("CGI") trading hours
            - index_dow_jones (str): Status of Dow Jones indices trading hours
            - index_ftse_russell (str): Status of FTSE Russell indices trading hours
            - index_msci (str): Status of MSCI indices trading hours
            - index_mstar (str): Status of Morningstar indices trading hours
            - index_mstarc (str): Status of Morningstar Customer indices trading hours
            - index_nasdaq (str): Status of Nasdaq indices trading hours
            - index_s_and_p (str): Status of S&P indices trading hours
            - index_societe_generale (str): Status of Societe Generale indices trading hours

        Use Cases:
            - **Real-Time Monitoring**: Check if markets are currently open for trading
            - **Algorithm Scheduling**: Determine when to run trading algorithms based on market hours
            - **UI Updates**: Display current market status to users in trading applications
            - **Operational Planning**: Schedule system maintenance during market closures
            - **Trading Strategy**: Adjust crypto trading strategies based on traditional market hours
            - **Order Validation**: Prevent order submission when target markets are closed
            - **Risk Management**: Understand when gaps may occur due to market closures
            - **Crypto vs Traditional Markets**: Coordinate crypto trading with traditional market status
            - **International Trading**: Track multiple exchange statuses for global trading operations
            - **Market Hours Analysis**: Analyze patterns of market openings and closings

        Important Notes:
            - This endpoint returns a single snapshot of the current status (one row)
            - Status changes throughout the trading day (pre-market → open → after-hours → closed)
            - **Crypto markets trade 24/7** - crypto_status is typically always "open"
            - Traditional stock markets (NYSE, NASDAQ) have specific hours (typically 9:30 AM - 4:00 PM ET)
            - Extended hours trading occurs before and after regular market hours
            - Use server_time as the authoritative timestamp (not fetched_at)
            - Call this endpoint regularly to monitor real-time market status changes
            - Combine with polygon_market_holidays to understand upcoming closures

        Trading Strategy Considerations:
            - **Crypto Trading**: Crypto markets are open 24/7, but liquidity/volatility varies when traditional markets close
            - **Volatility**: Markets often experience higher volatility at open and close
            - **Liquidity**: Extended hours typically have lower volume and wider spreads
            - **Gap Risk**: Markets closed overnight can create price gaps at next open
            - **Coordination**: Some crypto traders adjust strategies based on traditional market hours

        Example Workflow:
            1. Call this tool to fetch current market status
            2. Receive CSV filename in response
            3. Use py_eval to load and analyze the CSV file
            4. Check specific market statuses (crypto_status, nyse_status, etc.)
            5. Make trading decisions based on which markets are currently active
            6. Integrate into automated systems for real-time market monitoring

        Integration with Other Tools:
            - Use with polygon_market_holidays to plan around upcoming closures
            - Combine with polygon_crypto_aggregates for 24/7 crypto market analysis
            - Coordinate with polygon_news to understand market-moving events
            - Use with trading algorithms to ensure orders are placed during market hours

        Always use py_eval tool to analyze the saved CSV file for market status checks and trading decisions.
        """
        logger.info("polygon_market_status invoked")

        try:
            # Fetch market status
            df = fetch_market_status(
                polygon_client=local_polygon_client,
                output_dir=None
            )

            if df.empty:
                return "No market status data was retrieved. The endpoint may be temporarily unavailable."

            # Always save to CSV file
            filename = f"market_status_{uuid.uuid4().hex[:8]}.csv"
            filepath = csv_dir / filename
            df.to_csv(filepath, index=False)
            logger.info(f"Saved market status to {filename}")

            # Return formatted response
            return format_csv_response(filepath, df)

        except Exception as e:
            logger.error(f"Error in polygon_market_status: {e}")
            raise
