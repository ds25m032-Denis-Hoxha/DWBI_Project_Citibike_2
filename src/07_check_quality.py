from sqlalchemy import text

from db import get_engine


QUALITY_CHECKS = {
    "rows_in_staging": """
        SELECT COUNT(*) FROM staging.trips_raw;
    """,
    "null_ride_id": """
        SELECT COUNT(*) FROM staging.trips_raw
        WHERE ride_id IS NULL;
    """,
    "null_started_at": """
        SELECT COUNT(*) FROM staging.trips_raw
        WHERE started_at IS NULL;
    """,
    "null_ended_at": """
        SELECT COUNT(*) FROM staging.trips_raw
        WHERE ended_at IS NULL;
    """,
    "null_start_station_id": """
        SELECT COUNT(*) FROM staging.trips_raw
        WHERE start_station_id IS NULL;
    """,
    "null_start_station_name": """
        SELECT COUNT(*) FROM staging.trips_raw
        WHERE start_station_name IS NULL;
    """,
    "ended_before_started": """
        SELECT COUNT(*)
        FROM staging.trips_raw
        WHERE started_at IS NOT NULL
          AND ended_at IS NOT NULL
          AND ended_at < started_at;
    """,
    "duplicate_ride_id_groups": """
        SELECT COUNT(*)
        FROM (
            SELECT ride_id
            FROM staging.trips_raw
            WHERE ride_id IS NOT NULL
            GROUP BY ride_id
            HAVING COUNT(*) > 1
        ) duplicate_ids;
    """
}


def main() -> None:
    engine = get_engine()

    with engine.connect() as connection:
        print("Data quality checks for staging.trips_raw:\n")

        for check_name, query in QUALITY_CHECKS.items():
            value = connection.execute(text(query)).scalar()
            print(f"{check_name}: {value}")


if __name__ == "__main__":
    main()