from pathlib import Path

import pandas as pd


DATA_DIR = Path("data/sample")


def main() -> None:
    if not DATA_DIR.exists():
        raise FileNotFoundError(f"Data folder not found: {DATA_DIR}")

    csv_files = sorted(DATA_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in: {DATA_DIR}")

    for file_path in csv_files:
        print("\n" + "=" * 70)
        print(f"File: {file_path.name}")
        print("=" * 70)

        df = pd.read_csv(file_path, nrows=5)

        print("Columns:")
        for column in df.columns.tolist():
            print(f"- {column}")

        print("\nSample rows:")
        print(df.head().to_string(index=False))


if __name__ == "__main__":
    main()