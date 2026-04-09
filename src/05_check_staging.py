from sqlalchemy import text

from db import get_engine


CHECKS = {
    "rows_in_staging": "SELECT COUNT(*) FROM staging.trips_raw;",
    "distinct_ride_id": "SELECT COUNT(DISTINCT ride_id) FROM staging.trips_raw;",
    "source_files": "SELECT COUNT(DISTINCT source_file) FROM staging.trips_raw;",
    "null_ride_id": "SELECT COUNT(*) FROM staging.trips_raw WHERE ride_id IS NULL;",
    "null_started_at": "SELECT COUNT(*) FROM staging.trips_raw WHERE started_at IS NULL;",
    "null_ended_at": "SELECT COUNT(*) FROM staging.trips_raw WHERE ended_at IS NULL;",
}


def main() -> None:
    engine = get_engine()

    with engine.connect() as connection:
        print("Staging check results:\n")

        for check_name, query in CHECKS.items():
            value = connection.execute(text(query)).scalar()
            print(f"{check_name}: {value}")


if __name__ == "__main__":
    main()