import logging
from datetime import datetime, timedelta
import uuid
from mcp_service import format_csv_response
import pandas as pd
from polygon import RESTClient
from typing import Optional
import json

logger = logging.getLogger(__name__)


def fetch_news(
    polygon_client: RESTClient,
    ticker: str = "",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 10,
    order: str = "desc",
    sort: str = "published_utc",
) -> pd.DataFrame:
    """
    Fetch news articles from Polygon.io and return as DataFrame.

    Args:
        polygon_client: Initialized Polygon RESTClient
        ticker: Specific ticker symbol to fetch news for (e.g., 'AAPL'). If empty, fetches general market news.
        start_date: Start date in YYYY-MM-DD format. Defaults to 7 days ago if not provided.
        end_date: End date in YYYY-MM-DD format. Defaults to today if not provided.
        limit: Maximum number of news articles to fetch (default: 10, max: 1000)
        order: Order results based on sort field (default: "desc")
        sort: Sort field used for ordering (default: "published_utc")

    Returns:
        DataFrame with news articles containing columns:
        - published_utc: Publication datetime
        - title: Article title/headline
        - description: Article description/summary
        - author: Article author
        - article_url: Link to the article
        - tickers: Associated ticker symbols (JSON array string)
        - publisher_name: Name of the publisher

    Note:
        Period parameters (start_date/end_date) are strongly recommended to avoid request timeouts.
        If not provided, defaults to the last 7 days.
    """
    # Set default date range if not provided (required to avoid request freezing)
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')

    logger.info(
        f"Fetching news: ticker={ticker or 'general'}, "
        f"start_date={start_date}, end_date={end_date}, limit={limit}"
    )

    records = []
    try:
        # Build API parameters
        api_kwargs = {
            'published_utc_gte': start_date,
            'published_utc_lte': end_date,
            'order': order,
            'limit': min(limit, 1000),  # Enforce max limit
            'sort': sort
        }

        # Add ticker filter if specified
        if ticker:
            api_kwargs['ticker'] = ticker

        # Fetch news from Polygon API
        response = polygon_client.list_ticker_news(**api_kwargs)

        # Process each article
        for article in response:
            # Parse published_utc datetime
            published_utc_raw = getattr(article, 'published_utc', None)
            if published_utc_raw:
                try:
                    # Handle both Z and +00:00 timezone formats
                    dt_str = published_utc_raw
                    if dt_str.endswith('Z'):
                        dt_str = dt_str[:-1] + '+00:00'
                    dt = datetime.fromisoformat(dt_str)
                    published_utc = dt.strftime("%Y-%m-%d %H:%M:%S")
                except (ValueError, TypeError) as e:
                    logger.warning(f"Error parsing datetime: {e}")
                    published_utc = str(published_utc_raw)
            else:
                published_utc = None

            # Extract publisher name
            publisher = getattr(article, 'publisher', None)
            publisher_name = getattr(publisher, 'name', None) if publisher else None

            # Extract tickers array and convert to JSON string for CSV storage
            tickers_list = getattr(article, 'tickers', [])
            tickers_json = json.dumps(tickers_list) if tickers_list else None

            record = {
                'published_utc': published_utc,
                'title': getattr(article, 'title', None),
                'description': getattr(article, 'description', None),
                'author': getattr(article, 'author', None),
                'article_url': getattr(article, 'article_url', None),
                'tickers': tickers_json,
                'publisher_name': publisher_name
            }
            records.append(record)

    except Exception as e:
        logger.error(f"Error fetching news from Polygon API: {e}")
        return pd.DataFrame()

    df = pd.DataFrame(records)

    logger.info(f"Successfully fetched {len(df)} news articles")

    return df


def register_polygon_news(local_mcp_instance, local_polygon_client, csv_dir):
    """Register the polygon_news tool"""
    @local_mcp_instance.tool()
    def polygon_news(
        ticker: str = "",
        start_date: str = "",
        end_date: str = "",
        limit: int = 10
    ) -> str:
        """
        Fetch market news from Polygon.io and save to CSV file for analysis.

        This tool fetches recent news articles related to a specific ticker or general market news.
        IMPORTANT: Period parameters are strongly recommended. If not provided, defaults to the last 7 days
        to prevent request timeouts.

        Parameters:
            ticker (str, optional): Specific ticker symbol to fetch news for (e.g., 'AAPL', 'TSLA').
                If not provided or empty, fetches general market news.
            start_date (str, optional): Start date in 'YYYY-MM-DD' format.
                Defaults to 7 days before today if not provided.
            end_date (str, optional): End date in 'YYYY-MM-DD' format.
                Defaults to today if not provided.
            limit (int, optional): Maximum number of news articles to fetch.
                Defaults to 10. Maximum allowed is 1000.

        Returns:
            str: Formatted response with CSV file info, schema, sample data, and Python snippet to load the file.

        CSV Output Columns:
            - published_utc (datetime): Publication date and time in 'YYYY-MM-DD HH:MM:SS' format
            - title (string): News article headline
            - description (string): Article description/summary
            - author (string): Article author name
            - article_url (string): Link to the full article
            - tickers (string): JSON array of associated ticker symbols
            - publisher_name (string): Name of the news publisher

        Use Cases:
            - News sentiment analysis
            - Market event tracking and timeline analysis
            - Fundamental research and due diligence
            - Trading signal generation based on news flow

        Always use the py_eval tool to analyze the saved CSV file for insights.

        Example usage:
            polygon_news(ticker="AAPL", start_date="2025-10-05", end_date="2025-10-12", limit=10)
        """

        logger.info(
            f"polygon_news tool invoked: ticker={ticker or 'general'}, "
            f"start_date={start_date or 'default'}, end_date={end_date or 'default'}, limit={limit}"
        )

        # Convert empty strings to None for proper default handling
        start_date_param = start_date if start_date else None
        end_date_param = end_date if end_date else None

        # Call fetch_news function
        df = fetch_news(
            polygon_client=local_polygon_client,
            ticker=ticker,
            start_date=start_date_param,
            end_date=end_date_param,
            limit=limit
        )

        if df.empty:
            return "No news articles found for the specified criteria."

        # Generate filename
        filename = f"news_{ticker}_{str(uuid.uuid4())[:8]}.csv" if ticker else f"news_general_{str(uuid.uuid4())[:8]}.csv"
        filepath = csv_dir / filename

        # Save to CSV file
        df.to_csv(filepath, index=False)
        logger.info(f"Saved news data to {filename} ({len(df)} records)")

        # Return formatted response
        return format_csv_response(filepath, df)
