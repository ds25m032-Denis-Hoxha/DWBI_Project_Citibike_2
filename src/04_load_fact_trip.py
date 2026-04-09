from sqlalchemy import text

from db import get_engine


FACT_LOAD_SQL = """
INSERT INTO dwh.fact_trip (
    ride_id,
    date_key,
    time_key,
    start_station_key,
    bike_type_key,
    member_type_key,
    trip_count
)
SELECT
    s.ride_id,
    d.date_key,
    t.time_key,
    ss.start_station_key,
    bt.bike_type_key,
    mt.member_type_key,
    1 AS trip_count
FROM staging.trips_raw s
JOIN dwh.dim_date d
    ON DATE(s.started_at) = d.full_date
JOIN dwh.dim_time t
    ON EXTRACT(HOUR FROM s.started_at)::INT = t.hour
JOIN dwh.dim_start_station ss
    ON s.start_station_id = ss.start_station_id
LEFT JOIN dwh.dim_bike_type bt
    ON s.rideable_type = bt.rideable_type
LEFT JOIN dwh.dim_member_type mt
    ON s.member_casual = mt.member_casual
WHERE s.ride_id IS NOT NULL
  AND s.started_at IS NOT NULL
  AND s.ended_at IS NOT NULL
  AND s.ended_at >= s.started_at
  AND s.start_station_id IS NOT NULL
  AND s.start_station_name IS NOT NULL
ON CONFLICT (ride_id) DO NOTHING;
"""


def get_count(connection, table_name: str) -> int:
    query = text(f"SELECT COUNT(*) FROM dwh.{table_name}")
    return connection.execute(query).scalar()


def main() -> None:
    engine = get_engine()

    with engine.begin() as connection:
        staging_rows = connection.execute(
            text("SELECT COUNT(*) FROM staging.trips_raw")
        ).scalar()

        if staging_rows == 0:
            raise RuntimeError(
                "staging.trips_raw is empty. Load staging and dimensions first."
            )

        required_dimension_tables = [
            "dim_date",
            "dim_time",
            "dim_start_station",
            "dim_bike_type",
            "dim_member_type",
        ]

        for table_name in required_dimension_tables:
            row_count = get_count(connection, table_name)
            if row_count == 0:
                raise RuntimeError(
                    f"dwh.{table_name} is empty. Build dimensions before loading facts."
                )

        before_count = get_count(connection, "fact_trip")
        connection.execute(text(FACT_LOAD_SQL))
        after_count = get_count(connection, "fact_trip")
        inserted_rows = after_count - before_count

    print("Fact loading finished.")
    print(f"Rows inserted into dwh.fact_trip: {inserted_rows}")
    print(f"Total rows now in dwh.fact_trip: {after_count}\n")

    print("How foreign keys are assigned:")
    print("- date_key comes from matching DATE(started_at) to dim_date.full_date")
    print("- time_key comes from matching the hour of started_at to dim_time.hour")
    print("- start_station_key comes from matching start_station_id to dim_start_station.start_station_id")
    print("- bike_type_key comes from matching rideable_type to dim_bike_type.rideable_type")
    print("- member_type_key comes from matching member_casual to dim_member_type.member_casual")


if __name__ == "__main__":
    main()