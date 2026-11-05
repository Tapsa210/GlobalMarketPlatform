
import psycopg

from ingestion.config import load_settings


def get_connection():
    """
    Create and return a PostgreSQL database connection.
    """
    settings = load_settings()

    connection = psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        connect_timeout=10,
    )

    return connection
