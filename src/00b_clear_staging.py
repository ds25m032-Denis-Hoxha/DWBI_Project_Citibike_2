from sqlalchemy import text

from db import get_engine


def main() -> None:
    engine = get_engine()

    with engine.begin() as connection:
        row_count = connection.execute(
            text("SELECT COUNT(*) FROM staging.trips_raw")
        ).scalar()

        if row_count == 0:
            print("staging.trips_raw is already empty.")
            return

        connection.execute(text("TRUNCATE TABLE staging.trips_raw"))
        print(f"Staging cleared. Removed {row_count} rows from staging.trips_raw.")


if __name__ == "__main__":
    main()