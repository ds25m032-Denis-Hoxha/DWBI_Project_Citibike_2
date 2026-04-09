from sqlalchemy import text

from db import get_engine


def main() -> None:
    engine = get_engine()

    with engine.connect() as connection:
        version = connection.execute(text("SELECT version();")).scalar()

    print("Database connection successful.")
    print("PostgreSQL version:")
    print(version)


if __name__ == "__main__":
    main()