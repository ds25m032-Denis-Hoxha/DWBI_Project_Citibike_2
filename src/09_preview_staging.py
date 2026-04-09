from sqlalchemy import text

from db import get_engine


QUERY = """
SELECT *
FROM staging.trips_raw
ORDER BY load_timestamp DESC NULLS LAST
LIMIT 5;
"""


def main() -> None:
    engine = get_engine()

    with engine.connect() as connection:
        rows = connection.execute(text(QUERY)).fetchall()

    print("Preview of staging.trips_raw:\n")

    if not rows:
        print("No rows found.")
        return

    for row in rows:
        print(row)


if __name__ == "__main__":
    main()