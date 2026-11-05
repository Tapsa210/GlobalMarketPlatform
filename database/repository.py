
from psycopg.types.json import Jsonb

from database.connection import get_connection


def save_raw_response(
    symbol: str,
    source: str,
    response_data: dict,
) -> int:
    """
    Save an original API response into the Bronze layer.
    Returns the generated database ID.
    """

    query = """
        INSERT INTO raw_api_responses (
            symbol,
            source,
            response_data
        )
        VALUES (%s, %s, %s)
        RETURNING id;
    """

    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                query,
                (
                    symbol.strip().upper(),
                    source,
                    Jsonb(response_data),
                ),
            )

            record_id = cursor.fetchone()[0]

        return record_id


def get_raw_response(record_id: int) -> dict | None:
    """Retrieve a raw API response by its database ID."""

    query = """
        SELECT id, symbol, source, response_data, ingested_at
        FROM raw_api_responses
        WHERE id = %s;
    """

    with get_connection() as connection, connection.cursor() as cursor:
        cursor.execute(query, (record_id,))
        row = cursor.fetchone()

    if row is None:
        return None

    return {
        "id": row[0],
        "symbol": row[1],
        "source": row[2],
        "response_data": row[3],
        "ingested_at": row[4],
    }
