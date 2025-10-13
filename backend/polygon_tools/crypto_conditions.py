import logging
import uuid
import pandas as pd
from typing import Optional
from pathlib import Path
import json
from polygon import RESTClient
from mcp_service import format_csv_response

logger = logging.getLogger(__name__)


def fetch_crypto_conditions(
    polygon_client: RESTClient,
    data_type: Optional[str] = None,
    condition_id: Optional[int] = None,
    sip: Optional[str] = None,
    limit: int = 100,
    sort: Optional[str] = None,
    order: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch crypto condition codes from Polygon.io.

    Condition codes explain special circumstances associated with crypto trades and quotes,
    such as trades occurring outside regular sessions or at averaged prices.

    Args:
        polygon_client: Initialized Polygon RESTClient
        data_type: Filter by data type (e.g., "trade", "quote")
        condition_id: Filter for a specific condition ID
        sip: Filter by SIP (Systematic Internalization Provider)
        limit: Maximum number of results (default: 100, max: 1000)
        sort: Field to sort by (e.g., "id", "name")
        order: Sort order - "asc" or "desc"

    Returns:
        DataFrame with crypto condition codes and their details

    Note:
        This is reference/metadata data that helps interpret crypto trading data.
        Use these conditions to understand trade flags and filter data appropriately.
    """
    logger.info(f"Fetching crypto conditions (limit={limit})")

    records = []
    try:
        # Build kwargs for the API call
        kwargs = {
            'asset_class': 'crypto',
            'limit': min(limit, 1000)  # Enforce max limit
        }

        if data_type:
            kwargs['data_type'] = data_type
        if condition_id is not None:
            kwargs['id'] = condition_id
        if sip:
            kwargs['sip'] = sip
        if sort:
            kwargs['sort'] = sort
        if order:
            kwargs['order'] = order

        # Fetch conditions from Polygon API
        conditions = polygon_client.list_conditions(**kwargs)

        for condition in conditions:
            # Extract basic fields
            record = {
                'id': getattr(condition, 'id', None),
                'name': getattr(condition, 'name', None),
                'abbreviation': getattr(condition, 'abbreviation', None),
                'asset_class': getattr(condition, 'asset_class', None),
                'description': getattr(condition, 'description', None),
                'type': getattr(condition, 'type', None),
                'legacy': getattr(condition, 'legacy', None),
                'exchange': getattr(condition, 'exchange', None),
            }

            # Handle array field - data_types
            data_types = getattr(condition, 'data_types', None)
            if data_types:
                record['data_types'] = json.dumps(data_types) if isinstance(data_types, list) else str(data_types)
            else:
                record['data_types'] = None

            # Handle complex object fields - convert to JSON strings
            sip_mapping = getattr(condition, 'sip_mapping', None)
            if sip_mapping:
                try:
                    # Convert to dict if it's an object, then to JSON
                    if hasattr(sip_mapping, '__dict__'):
                        record['sip_mapping'] = json.dumps(sip_mapping.__dict__)
                    elif isinstance(sip_mapping, dict):
                        record['sip_mapping'] = json.dumps(sip_mapping)
                    else:
                        record['sip_mapping'] = str(sip_mapping)
                except Exception as e:
                    logger.warning(f"Could not serialize sip_mapping: {e}")
                    record['sip_mapping'] = str(sip_mapping)
            else:
                record['sip_mapping'] = None

            update_rules = getattr(condition, 'update_rules', None)
            if update_rules:
                try:
                    # Convert to dict if it's an object, then to JSON
                    if hasattr(update_rules, '__dict__'):
                        record['update_rules'] = json.dumps(update_rules.__dict__)
                    elif isinstance(update_rules, dict):
                        record['update_rules'] = json.dumps(update_rules)
                    else:
                        record['update_rules'] = str(update_rules)
                except Exception as e:
                    logger.warning(f"Could not serialize update_rules: {e}")
                    record['update_rules'] = str(update_rules)
            else:
                record['update_rules'] = None

            records.append(record)

    except Exception as e:
        logger.error(f"Error fetching crypto conditions: {e}")
        raise

    df = pd.DataFrame(records)

    logger.info(f"Successfully fetched {len(df)} crypto conditions")

    return df


def register_polygon_crypto_conditions(local_mcp_instance, local_polygon_client, csv_dir):
    """Register the polygon_crypto_conditions tool"""
    @local_mcp_instance.tool()
    def polygon_crypto_conditions(
        limit: int = 100,
        data_type: str = "",
        condition_id: int = 0,
        sip: str = "",
        sort: str = "",
        order: str = ""
    ) -> str:
        """
        Fetch cryptocurrency condition codes for interpreting trade and quote data, and save to CSV file.

        Condition codes are reference metadata that explain special circumstances associated with crypto trades
        and quotes. They help you understand trade flags, filter data appropriately, and correctly interpret
        trading activity. Essential for accurate data analysis and algorithmic trading decisions.

        Parameters:
            limit (int, optional): Maximum number of condition codes to retrieve.
                Range: 1-1000
                Default: 100
                Recommendation: Use 1000 to get comprehensive reference data on first call.

            data_type (str, optional): Filter condition codes by the type of data they apply to.
                Options: "trade", "quote"
                Default: "" (returns all data types)
                Examples:
                - "trade": Only conditions that apply to trade data
                - "quote": Only conditions that apply to quote/bid-ask data
                Use this to focus on conditions relevant to your analysis type.

            condition_id (int, optional): Retrieve a specific condition code by its ID.
                Default: 0 (not used)
                Use when you need details about a specific condition you encountered in trade data.
                Example: condition_id=1 returns details for condition ID 1

            sip (str, optional): Filter by SIP (Systematic Internalization Provider) mapping.
                Default: "" (not used)
                If a condition has a mapping for the specified SIP, it will be included.
                Advanced parameter for exchange-specific condition interpretation.

            sort (str, optional): Field to sort results by.
                Options: "id", "name", "type"
                Default: "" (API default sorting)
                Examples:
                - "name": Sort alphabetically by condition name
                - "id": Sort numerically by condition ID

            order (str, optional): Sort order for results.
                Options: "asc" (ascending), "desc" (descending)
                Default: "" (API default order)
                Combine with sort parameter for specific ordering.

        Returns:
            str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

        CSV Output Structure:
            - id (int): Unique identifier for this condition code in Polygon.io system
            - name (str): Full descriptive name of the condition (e.g., "Average Price Trade")
            - abbreviation (str): Commonly-used short code for this condition (e.g., "AP")
            - asset_class (str): Asset type this applies to (always "crypto" for this endpoint)
            - description (str): Detailed explanation of what this condition means for trades/quotes
            - type (str): Category of condition. Values include:
                * "sale_condition": Conditions affecting trade/sale records
                * "quote_condition": Conditions affecting quote/bid-ask records
                * "sip_generated_flag": Flags generated by SIP aggregation
                * "financial_status_indicator": Financial status markers
                * "short_sale_restriction_indicator": Short sale restrictions
                * "settlement_condition": Settlement-related conditions
                * "market_condition": Market state conditions
                * "trade_thru_exempt": Trade-through exemption markers
            - legacy (bool): Whether this condition is deprecated (True) or active (False)
                Legacy conditions may appear in historical data but are no longer used
            - exchange (int): Optional exchange ID if condition is exchange-specific
                If present, this condition only applies to data from that exchange
            - data_types (JSON string): Array of data types this condition applies to
                Example: ["trade"], ["quote"], or ["trade", "quote"]
                Parse with json.loads() in Python
            - sip_mapping (JSON string): Maps condition codes from individual exchanges to Polygon.io code
                Example: {"CTA": "B", "UTP": "W"} means CTA exchange uses "B", UTP uses "W"
                Parse with json.loads() in Python for exchange-specific interpretation
            - update_rules (JSON string): Rules for how this condition affects aggregated data
                Contains nested objects showing if condition updates high/low/open/close/volume
                Example: {"consolidated": {"updates_high_low": false, "updates_volume": true}}
                Critical for understanding how conditions affect OHLCV bars
                Parse with json.loads() in Python

        Use Cases:
            - **Data Interpretation**: Understand what condition codes in your trade data mean
            - **Filtering**: Identify which conditions to include/exclude for clean analysis
            - **Aggregation Rules**: Learn how conditions affect OHLCV calculations
            - **Exchange Mapping**: Map raw exchange condition codes to standardized Polygon codes
            - **Compliance**: Ensure trading algorithms handle special conditions correctly
            - **Algorithm Development**: Build filters for valid/invalid trade conditions
            - **Historical Analysis**: Identify legacy conditions when analyzing old data

        Reference Data Notes:
            - This is metadata/reference data, not time-series trading data
            - Condition codes are relatively static; fetch once and cache for lookup
            - Use returned data as a lookup table when processing trade/quote data
            - Check 'legacy' field to exclude deprecated conditions from new algorithms

        Integration Example:
            1. Call this tool once to get all crypto conditions (limit=1000)
            2. Use py_eval to load CSV and create a lookup dictionary
            3. When analyzing trade data, cross-reference condition codes with this reference
            4. Apply update_rules to correctly calculate aggregates

        Always use py_eval tool to analyze the saved CSV file for condition code lookups.
        """
        logger.info(f"polygon_crypto_conditions invoked: limit={limit}, data_type={data_type}")

        try:
            # Handle optional parameters (convert empty strings to None)
            data_type_param = data_type if data_type else None
            condition_id_param = condition_id if condition_id != 0 else None
            sip_param = sip if sip else None
            sort_param = sort if sort else None
            order_param = order if order else None

            # Fetch crypto conditions
            df = fetch_crypto_conditions(
                polygon_client=local_polygon_client,
                data_type=data_type_param,
                condition_id=condition_id_param,
                sip=sip_param,
                limit=limit,
                sort=sort_param,
                order=order_param,
            )

            if df.empty:
                return "No crypto condition codes were retrieved. Please check your filter parameters."

            # Always save to CSV file
            filename = f"crypto_conditions_{uuid.uuid4().hex[:8]}.csv"
            filepath = csv_dir / filename
            df.to_csv(filepath, index=False)
            logger.info(f"Saved crypto conditions to {filename} ({len(df)} records)")

            # Return formatted response
            return format_csv_response(filepath, df)

        except Exception as e:
            logger.error(f"Error in polygon_crypto_conditions: {e}")
            return f"Error fetching crypto conditions: {str(e)}"
