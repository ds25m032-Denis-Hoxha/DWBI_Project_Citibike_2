from db import get_engine
from sqlalchemy import text


TABLES_TO_CHECK = [
    ("staging", "trips_raw"),
    ("dwh", "dim_date"),
    ("dwh", "dim_time"),
    ("dwh", "dim_start_station"),
    ("dwh", "dim_bike_type"),
    ("dwh", "dim_member_type"),
    ("dwh", "fact_trip"),
]


def table_exists(connection, schema_name: str, table_name: str) -> bool:
    query = text(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = :schema_name
              AND table_name = :table_name
        )
        """
    )
    return connection.execute(
        query,
        {"schema_name": schema_name, "table_name": table_name},
    ).scalar()


def get_row_count(connection, schema_name: str, table_name: str) -> int:
    query = text(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
    return connection.execute(query).scalar()


def main() -> None:
    engine = get_engine()

    print("Checking staging and DWH before running ETL...\n")

    with engine.connect() as connection:
        total_rows_found = 0

        for schema_name, table_name in TABLES_TO_CHECK:
            if not table_exists(connection, schema_name, table_name):
                print(f"[MISSING] {schema_name}.{table_name} does not exist yet")
                continue

            row_count = get_row_count(connection, schema_name, table_name)
            total_rows_found += row_count

            if row_count == 0:
                print(f"[OK]      {schema_name}.{table_name} is empty")
            else:
                print(f"[WARNING] {schema_name}.{table_name} already contains {row_count} rows")

        print()
        if total_rows_found == 0:
            print("Safe to continue: staging and DWH are currently empty.")
        else:
            print("Be careful: some tables already contain data.")
            print("If you run the load scripts again, you may duplicate data or hit key conflicts.")


if __name__ == "__main__":
    main()