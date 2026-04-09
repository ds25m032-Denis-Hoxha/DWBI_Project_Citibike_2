from sqlalchemy import text

from db import get_engine


PREVIEW_QUERIES = {
    "dim_date": """
        SELECT *
        FROM dwh.dim_date
        ORDER BY date_key
        LIMIT 5;
    """,
    "dim_time": """
        SELECT *
        FROM dwh.dim_time
        ORDER BY time_key
        LIMIT 5;
    """,
    "dim_start_station": """
        SELECT *
        FROM dwh.dim_start_station
        ORDER BY start_station_key
        LIMIT 5;
    """,
    "dim_bike_type": """
        SELECT *
        FROM dwh.dim_bike_type
        ORDER BY bike_type_key
        LIMIT 5;
    """,
    "dim_member_type": """
        SELECT *
        FROM dwh.dim_member_type
        ORDER BY member_type_key
        LIMIT 5;
    """,
    "fact_trip": """
        SELECT *
        FROM dwh.fact_trip
        LIMIT 5;
    """
}


def main() -> None:
    engine = get_engine()

    with engine.connect() as connection:
        for table_name, query in PREVIEW_QUERIES.items():
            print("\n" + "=" * 70)
            print(table_name)
            print("=" * 70)

            rows = connection.execute(text(query)).fetchall()

            if not rows:
                print("No rows found.")
                continue

            for row in rows:
                print(row)


if __name__ == "__main__":
    main()