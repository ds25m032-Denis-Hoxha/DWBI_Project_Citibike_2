from sqlalchemy import text

from db import get_engine


DIMENSION_LOADS = [
    (
        "dim_date",
        """
        INSERT INTO dwh.dim_date (
            full_date,
            year,
            month,
            day,
            weekday_name,
            is_weekend
        )
        SELECT DISTINCT
            DATE(started_at) AS full_date,
            EXTRACT(YEAR FROM started_at)::INT AS year,
            EXTRACT(MONTH FROM started_at)::INT AS month,
            EXTRACT(DAY FROM started_at)::INT AS day,
            TRIM(TO_CHAR(started_at, 'Day')) AS weekday_name,
            EXTRACT(ISODOW FROM started_at) IN (6, 7) AS is_weekend
        FROM staging.trips_raw
        WHERE started_at IS NOT NULL
        ON CONFLICT (full_date) DO NOTHING;
        """
    ),
    (
        "dim_time",
        """
        INSERT INTO dwh.dim_time (hour)
        SELECT DISTINCT
            EXTRACT(HOUR FROM started_at)::INT AS hour
        FROM staging.trips_raw
        WHERE started_at IS NOT NULL
        ON CONFLICT (hour) DO NOTHING;
        """
    ),
    (
        "dim_start_station",
        """
        INSERT INTO dwh.dim_start_station (
            start_station_id,
            start_station_name,
            start_lat,
            start_lng
        )
        SELECT DISTINCT
            start_station_id,
            start_station_name,
            start_lat,
            start_lng
        FROM staging.trips_raw
        WHERE start_station_id IS NOT NULL
          AND start_station_name IS NOT NULL
        ON CONFLICT (start_station_id) DO NOTHING;
        """
    ),
    (
        "dim_bike_type",
        """
        INSERT INTO dwh.dim_bike_type (rideable_type)
        SELECT DISTINCT
            rideable_type
        FROM staging.trips_raw
        WHERE rideable_type IS NOT NULL
        ON CONFLICT (rideable_type) DO NOTHING;
        """
    ),
    (
        "dim_member_type",
        """
        INSERT INTO dwh.dim_member_type (member_casual)
        SELECT DISTINCT
            member_casual
        FROM staging.trips_raw
        WHERE member_casual IS NOT NULL
        ON CONFLICT (member_casual) DO NOTHING;
        """
    ),
]


def get_table_count(connection, table_name: str) -> int:
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
                "staging.trips_raw is empty. Load the staging data first."
            )

        print(f"Rows available in staging.trips_raw: {staging_rows}\n")

        for table_name, insert_sql in DIMENSION_LOADS:
            before_count = get_table_count(connection, table_name)
            connection.execute(text(insert_sql))
            after_count = get_table_count(connection, table_name)
            inserted_rows = after_count - before_count

            print(
                f"{table_name}: inserted {inserted_rows} new rows "
                f"(total now: {after_count})"
            )

    print("\nDimension loading finished.")
    print("Note: surrogate keys are generated automatically by the dimension tables.")
    print("The script loads unique business values from staging into each dimension.")


if __name__ == "__main__":
    main()