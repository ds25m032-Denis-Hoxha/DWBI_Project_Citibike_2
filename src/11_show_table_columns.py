from sqlalchemy import text

from db import get_engine


TABLES = [
    ("staging", "trips_raw"),
    ("dwh", "dim_date"),
    ("dwh", "dim_time"),
    ("dwh", "dim_start_station"),
    ("dwh", "dim_bike_type"),
    ("dwh", "dim_member_type"),
    ("dwh", "fact_trip"),
]


COLUMN_QUERY = """
SELECT
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = :schema_name
  AND table_name = :table_name
ORDER BY ordinal_position;
"""


def main() -> None:
    engine = get_engine()

    with engine.connect() as connection:
        for schema_name, table_name in TABLES:
            print("\n" + "=" * 70)
            print(f"{schema_name}.{table_name}")
            print("=" * 70)

            rows = connection.execute(
                text(COLUMN_QUERY),
                {"schema_name": schema_name, "table_name": table_name},
            ).fetchall()

            if not rows:
                print("Table not found.")
                continue

            for row in rows:
                print(f"{row.column_name} | {row.data_type} | nullable={row.is_nullable}")


if __name__ == "__main__":
    main()