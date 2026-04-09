from pathlib import Path

import pandas as pd
from sqlalchemy import text

from db import get_engine


DATA_DIR = Path("data/sample")
STAGING_TABLE = "staging.trips_raw"

REQUIRED_COLUMNS = [
    "ride_id",
    "rideable_type",
    "started_at",
    "ended_at",
    "start_station_name",
    "start_station_id",
    "member_casual",
]

TEXT_COLUMNS = [
    "ride_id",
    "rideable_type",
    "start_station_name",
    "start_station_id",
    "end_station_name",
    "end_station_id",
    "member_casual",
]


def staging_has_data(engine) -> bool:
    query = text("SELECT COUNT(*) FROM staging.trips_raw")
    with engine.connect() as connection:
        row_count = connection.execute(query).scalar()
    return row_count > 0


def prepare_dataframe(file_path: Path) -> pd.DataFrame:
    df = pd.read_csv(file_path)

    missing_columns = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns in {file_path.name}: {missing_columns}")

    for column in TEXT_COLUMNS:
        if column in df.columns:
            df[column] = df[column].astype("string").str.strip()

    df["started_at"] = pd.to_datetime(df["started_at"], errors="coerce")
    df["ended_at"] = pd.to_datetime(df["ended_at"], errors="coerce")
    df["source_file"] = file_path.name

    return df


def print_file_summary(df: pd.DataFrame, file_name: str) -> None:
    print(f"\nLoading {file_name}")
    print(f"Rows read: {len(df)}")
    print(f"Duplicate ride_id rows: {df.duplicated(subset=['ride_id']).sum()}")
    print(f"Rows with null started_at: {df['started_at'].isna().sum()}")
    print(f"Rows with null start_station_id: {df['start_station_id'].isna().sum()}")
    print(f"Rows with null start_station_name: {df['start_station_name'].isna().sum()}")


def load_file_to_staging(df: pd.DataFrame, engine) -> None:
    df.to_sql(
        "trips_raw",
        engine,
        schema="staging",
        if_exists="append",
        index=False,
        method="multi",
    )


def main() -> None:
    engine = get_engine()

    if not DATA_DIR.exists():
        raise FileNotFoundError(f"Data folder not found: {DATA_DIR}")

    csv_files = sorted(DATA_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in: {DATA_DIR}")

    if staging_has_data(engine):
        raise RuntimeError(
            "staging.trips_raw already contains data. "
            "Clear staging first if you want to reload the CSV files."
        )

    total_rows_loaded = 0

    for file_path in csv_files:
        df = prepare_dataframe(file_path)
        print_file_summary(df, file_path.name)
        load_file_to_staging(df, engine)
        total_rows_loaded += len(df)
        print(f"Loaded {len(df)} rows from {file_path.name}")

    print(f"\nFinished loading staging. Total rows loaded: {total_rows_loaded}")


if __name__ == "__main__":
    main()