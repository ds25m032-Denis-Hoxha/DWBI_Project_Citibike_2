from pathlib import Path
from sqlalchemy import text
from db import get_engine

SQL_FILES = [
    "01_create_schemas.sql",
    "02_create_staging_tables.sql",
    "03_create_dwh_tables.sql",
]


def run_sql_file(connection, file_path: Path) -> None:
    sql_text = file_path.read_text(encoding="utf-8")
    connection.execute(text(sql_text))
    print(f"Executed: {file_path.name}")


def main() -> None:
    engine = get_engine()

    # go from src/ → project root → sql/
    project_root = Path(__file__).resolve().parent.parent
    sql_path = project_root / "sql"

    with engine.begin() as connection:
        for file_name in SQL_FILES:
            file_path = sql_path / file_name
            run_sql_file(connection, file_path)

    print("\nSchemas and tables are ready.")


if __name__ == "__main__":
    main()