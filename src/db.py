import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

load_dotenv()


def get_database_url() -> str:
    """Read DATABASE_URL from the .env file."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL is not set in the .env file")
    return database_url


def get_engine(echo: bool = False) -> Engine:
    """Create and return a SQLAlchemy engine."""
    return create_engine(get_database_url(), echo=echo)