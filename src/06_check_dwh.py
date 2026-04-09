from sqlalchemy import text

from db import get_engine


TABLES = [
    "dim_date",
    "dim_time",
    "dim_start_station",
    "dim_bike_type",
    "dim_member_type",
    "fact_trip",
]


def main() -> None:
    engine = get_engine()

    with engine.connect() as connection:
        print("DWH row counts:\n")

        for table_name in TABLES:
            query = text(f"SELECT COUNT(*) FROM dwh.{table_name}")
            row_count = connection.execute(query).scalar()
            print(f"{table_name}: {row_count}")


if __name__ == "__main__":
    main()