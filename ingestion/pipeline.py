from database.repository import save_raw_response
from ingestion.api_client import APIClient


def ingest_stock_data(symbol: str) -> int:
    """
    Fetch daily stock data and save the original
    API response into the Bronze layer.
    """

    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError("Symbol must be a non-empty string.")

    normalized_symbol = symbol.strip().upper()

    client = APIClient()

    response_data = client.get_daily_data(normalized_symbol)

    record_id = save_raw_response(
        symbol=normalized_symbol,
        source="alpha_vantage",
        response_data=response_data,
    )

    return record_id