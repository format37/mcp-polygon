import logging
import uuid
import pandas as pd
from typing import Optional
from pathlib import Path
from polygon import RESTClient
from mcp_service import format_csv_response

logger = logging.getLogger(__name__)


def fetch_market_holidays(
    polygon_client: RESTClient,
    output_dir: Optional[Path] = None
) -> pd.DataFrame:
    """
    Fetch upcoming market holidays from Polygon.io.

    This endpoint retrieves forward-looking market holidays and their corresponding
    open/close times. Use this data to plan ahead for trading activities, system
    operations, and schedule adjustments.

    Args:
        polygon_client: Initialized Polygon RESTClient
        output_dir: Optional output directory to save CSV file

    Returns:
        DataFrame with upcoming market holidays including dates, exchanges, and status

    Note:
        This endpoint is forward-looking only, listing future holidays that affect
        market hours. Data applies to all asset classes (stocks, crypto, forex, etc.).
    """
    logger.info("Fetching upcoming market holidays")

    records = []
    try:
        # Fetch market holidays
        holidays = polygon_client.get_market_holidays()

        for holiday in holidays:
            # Extract basic holiday information
            record = {
                'date': getattr(holiday, 'date', None),
                'exchange': getattr(holiday, 'exchange', None),
                'name': getattr(holiday, 'name', None),
                'status': getattr(holiday, 'status', None),
            }

            # Handle optional open/close times (for early-close days)
            # These are typically ISO 8601 datetime strings
            open_time = getattr(holiday, 'open', None)
            close_time = getattr(holiday, 'close', None)

            record['open'] = open_time if open_time else None
            record['close'] = close_time if close_time else None

            records.append(record)

    except Exception as e:
        logger.error(f"Error fetching market holidays: {e}")
        raise

    df = pd.DataFrame(records)

    # Convert date column to proper date format if present
    if not df.empty and 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d')

    logger.info(f"Successfully fetched {len(df)} upcoming market holidays")

    # Save to CSV if output directory is provided
    if output_dir and not df.empty:
        csv_file = output_dir / "market_holidays.csv"
        df.to_csv(csv_file, index=False)
        logger.info(f"Saved market holidays to {csv_file}")

    return df


def register_polygon_market_holidays(local_mcp_instance, local_polygon_client, csv_dir):
    """Register the polygon_market_holidays tool"""
    @local_mcp_instance.tool()
    def polygon_market_holidays() -> str:
        """
        Fetch upcoming market holidays and their corresponding open/close times, and save to CSV file.

        This endpoint retrieves forward-looking market holiday information across all exchanges.
        Use this data to plan ahead for trading activities, system maintenance, operational planning,
        and notifying users about upcoming market closures or early closes.

        Parameters:
            None (endpoint automatically returns upcoming holidays)

        Returns:
            str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

        CSV Output Structure:
            - date (str): Holiday date in 'YYYY-MM-DD' format
                Example: "2024-11-28" (Thanksgiving)
            - exchange (str): Exchange identifier affected by this holiday
                Examples: "NYSE" (New York Stock Exchange), "NASDAQ", "OTC" (Over-The-Counter)
                Note: Same holiday may appear multiple times for different exchanges
            - name (str): Holiday name
                Examples: "Thanksgiving", "Christmas Day", "New Year's Day", "Independence Day"
            - status (str): Market status for this day
                Values:
                * "closed": Market is completely closed for the entire day
                * "early-close": Market closes earlier than usual
            - open (str): Opening time in ISO 8601 format (YYYY-MM-DDTHH:MM:SS.sssZ)
                Only present for "early-close" days when market opens normally
                Example: "2024-11-29T14:30:00.000Z" (9:30 AM EST)
                Note: null/empty for "closed" status days
            - close (str): Closing time in ISO 8601 format (YYYY-MM-DDTHH:MM:SS.sssZ)
                Present for "early-close" days showing the early closing time
                Example: "2024-11-29T18:00:00.000Z" (1:00 PM EST)
                Note: null/empty for "closed" status days

        Use Cases:
            - **Trading Schedule Adjustments**: Plan trading strategies around market closures
            - **System Maintenance**: Schedule system updates during market holidays
            - **Operational Planning**: Adjust staffing and operations for holiday schedules
            - **User Notifications**: Alert users about upcoming market closures and early closes
            - **Algorithm Scheduling**: Pause or adjust automated trading during holidays
            - **Calendar Integration**: Build integrated holiday calendars for trading platforms
            - **Risk Management**: Anticipate reduced liquidity before/after holidays
            - **Order Management**: Avoid placing orders that won't execute during closures
            - **Portfolio Rebalancing**: Plan rebalancing activities around market availability
            - **Crypto Trading**: Understand when traditional markets are closed (crypto trades 24/7)

        Important Notes:
            - This endpoint is **forward-looking only** - shows future holidays, not historical
            - Same holiday date may appear multiple times (once per exchange)
            - Crypto markets trade 24/7 but this data helps coordinate with traditional markets
            - Early-close days have reduced trading hours (check open/close times)
            - Times are typically in UTC/ISO format - convert to your local timezone as needed
            - Data covers major U.S. exchanges (NYSE, NASDAQ, OTC)
            - Plan ahead: Use this to anticipate market gaps in your trading strategies

        Trading Strategy Considerations:
            - Markets often have reduced volume and volatility before holidays
            - Gap risk increases over extended closures (e.g., 3-day weekends)
            - Early-close days may have compressed trading activity
            - Consider closing risky positions before extended holiday closures
            - Crypto markets continue during traditional market holidays

        Example Workflow:
            1. Call this tool to fetch upcoming market holidays
            2. Receive CSV filename in response
            3. Use py_eval to load and analyze the CSV file
            4. Filter by specific exchanges or date ranges
            5. Identify next closure date for scheduling purposes
            6. Alert systems or adjust trading algorithms accordingly

        Always use py_eval tool to analyze the saved CSV file for comprehensive holiday planning.
        """
        logger.info("polygon_market_holidays invoked")

        try:
            # Fetch market holidays
            df = fetch_market_holidays(
                polygon_client=local_polygon_client,
                output_dir=None
            )

            if df.empty:
                return "No upcoming market holidays were retrieved. The endpoint may be temporarily unavailable or there are no upcoming holidays in the system."

            # Always save to CSV file
            filename = f"market_holidays_{uuid.uuid4().hex[:8]}.csv"
            filepath = csv_dir / filename
            df.to_csv(filepath, index=False)
            logger.info(f"Saved market holidays to {filename} ({len(df)} records)")

            # Return formatted response
            return format_csv_response(filepath, df)

        except Exception as e:
            logger.error(f"Error in polygon_market_holidays: {e}")
            return f"Error fetching market holidays: {str(e)}"
