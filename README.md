# CitiBike Data Warehouse Project

## 1. Project Overview

This project implements a simple Data Warehouse (DWH) for CitiBike trip data.

The goal is to transform raw CSV trip data into a structured format that supports analysis and reporting (e.g., in Power BI).

The pipeline follows a basic ETL process:

CSV files → Staging → Data Warehouse (star schema)

---

## 2. Project Structure

The project is divided into three main parts:

- `data/` → contains the source CSV files
- `sql/` → contains scripts for creating schemas and tables
- `src/` → contains Python scripts for the ETL process

---

### Folder overview

```
.
├── data/
│   └── sample/
├── sql/
│   ├── 01_create_schemas.sql
│   ├── 02_create_staging_tables.sql
│   └── 03_create_dwh_tables.sql
├── src/
│   ├── 00a_test_connection.py
│   ├── 00_check_before_run.py
│   ├── 00b_clear_staging.py
│   ├── 00c_show_csv_columns.py
│   ├── 01_create_database_objects.py
│   ├── 02_load_staging_from_csv.py
│   ├── 03_build_dimensions.py
│   ├── 04_load_fact_trip.py
│   ├── 05_check_staging.py
│   ├── 06_check_dwh.py
│   ├── 07_check_quality.py
│   ├── 08_find_unloaded_rows.py
│   ├── 09_preview_staging.py
│   ├── 10_preview_dwh.py
│   ├── 11_show_table_columns.py
│   └── db.py
├── requirements.txt
└── README.md
```

---

## 3. Setup

### Install dependencies

```bash
pip install -r requirements.txt
```

---

### Create `.env` file

Create a file in the project root:

```
.env
```

Add:

```
DATABASE_URL=postgresql+psycopg2://USER:PASSWORD@HOST/DBNAME?sslmode=require
```

---

### Test database connection

```bash
python src/00a_test_connection.py
```

Expected output:

```
('PostgreSQL ...',)
```
This should print the PostgreSQL version.

---

## 4. ETL Process (Step-by-Step)
------------------------------

All scripts should be executed from the project root.

### Step 0 — Check current state

`python src/00_check_before_run.py`

Purpose:

*   checks whether staging or DWH already contain data
    
*   prevents accidental duplicate loading
    

### Step 1 — (Optional) Clear staging

`python src/00b_clear_staging.py`

Use this only if:

*   staging already contains data
    
*   you want to reload CSV files
    

Important:

*   this does NOT delete DWH data
    

### Step 2 — Inspect CSV structure (optional)

`python src/00c_show_csv_columns.py`

Purpose:

*   shows available columns in the source files
    
*   useful for understanding the data
    

### Step 3 — Create schemas and tables

`python src/01_create_database_objects.py`

This step creates the required database structure:

*   schema staging
    
*   schema dwh
    
*   all staging and DWH tables
    

### Step 4 — Load CSV data into staging

`python src/02_load_staging_from_csv.py`

What happens:

*   CSV files are read using pandas
    
*   data is inserted into staging.trips\_raw
    
*   basic checks are printed (nulls, duplicates)
    

### Step 5 — Build dimension tables

`python src/03_build_dimensions.py`

What happens:

*   unique values are extracted from staging
    
*   inserted into dimension tables
    
*   surrogate keys are created automatically
    

Tables created:

*   dim\_date
    
*   dim\_time
    
*   dim\_start\_station
    
*   dim\_bike\_type
    
*   dim\_member\_type
    

### Step 6 — Load fact table

`python src/04_load_fact_trip.py`

What happens:

*   staging data is joined with dimension tables
    
*   foreign keys are assigned
    
*   valid rows are inserted into fact\_trip
    

Important:

*   rows with missing required values are excluded
    
*   duplicates are prevented using ON CONFLICT

\
Note:
The ETL process is designed to be repeatable without creating duplicates. 
Existing data in the DWH is preserved, while new data is added incrementally.



## 5. Validation and Checks
-------------------------

After running the pipeline, use the following scripts.

### Check staging

`python src/05_check_staging.py`

### Check DWH tables

`python src/06_check_dwh.py`

### Data quality checks

`python src/07_check_quality.py`

### Find rows not loaded into fact table

`python src/08_find_unloaded_rows.py`

This helps identify:

*   missing values
    
*   rows filtered during transformation
    

### Preview data

`python src/09_preview_staging.py` \
`python src/10_preview_dwh.py`

## 6. Data Model
--------------

### Fact Table

*   fact\_trip
    
    *   one row per trip
        
    *   contains foreign keys to dimensions
        

### Dimension Tables

*   dim\_date
    
*   dim\_time
    
*   dim\_start\_station
    
*   dim\_bike\_type
    
*   dim\_member\_type
    

## 7. ETL Logic Summary
---------------------

### Staging

*   raw data is loaded without modification
    

### Transformation

*   timestamps are converted
    
*   date and hour are extracted
    
*   invalid rows are filtered
    

### Loading

*   dimensions store unique business values
    
*   fact table references dimension keys
    

Example:

*   start\_station\_id → mapped to start\_station\_key
    
*   rideable\_type → mapped to bike\_type\_key
    

## 8. Data Quality Rules
----------------------

The following rows are excluded from the fact table:

*   missing start\_station\_id
    
*   missing start\_station\_name
    
*   invalid timestamps
    
*   ended\_at < started\_at
    

Duplicates are prevented using:
 
`ON CONFLICT (ride_id) DO NOTHING`

## 9. Important Notes
-------------------

*   Always run scripts from the project root
    
*   Do NOT delete DWH data unless absolutely necessary
    
*   Staging can be cleared safely
    
*   DWH is used for reporting (e.g., Power BI)
    

## 10. Typical Workflow
---------------------

Normal execution:

`python src/00_check_before_run.py` \
`python src/00b_clear_staging.py` \
`python src/02_load_staging_from_csv.py` \
`python src/03_build_dimensions.py` \
`python src/04_load_fact_trip.py `