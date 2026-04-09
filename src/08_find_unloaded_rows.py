from sqlalchemy import text

from db import get_engine


UNLOADED_ROWS_QUERY = """
SELECT
    s.ride_id,
    s.started_at,
    s.ended_at,
    s.start_station_id,
    s.start_station_name,
    s.rideable_type,
    s.member_casual,
    s.source_file
FROM staging.trips_raw s
LEFT JOIN dwh.fact_trip f
    ON s.ride_id = f.ride_id
WHERE f.ride_id IS NULL
ORDER BY s.started_at NULLS LAST, s.ride_id
LIMIT 20;
"""

UNLOADED_COUNT_QUERY = """
SELECT COUNT(*)
FROM staging.trips_raw s
LEFT JOIN dwh.fact_trip f
    ON s.ride_id = f.ride_id
WHERE f.ride_id IS NULL;
"""


def main() -> None:
    engine = get_engine()

    with engine.connect() as connection:
        unloaded_count = connection.execute(text(UNLOADED_COUNT_QUERY)).scalar()
        print(f"Unloaded rows in staging: {unloaded_count}")

        if unloaded_count == 0:
            print("All staging rows that meet the join conditions were loaded into fact_trip.")
            return

        print("\nSample of unloaded rows (max 20):\n")
        rows = connection.execute(text(UNLOADED_ROWS_QUERY)).fetchall()

        for row in rows:
            print(row)


if __name__ == "__main__":
    main()