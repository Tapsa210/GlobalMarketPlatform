
from database.connection import get_connection


def test_database_connection():
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT current_database(), current_user;"
            )

            result = cursor.fetchone()

        assert result[0] == "market_data_test"
        assert result[1] == "market_user"

    finally:
        connection.close()
