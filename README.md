# Trade Mark

A Python-based toolkit for downloading, processing, indexing, and querying large USPTO trademark datasets.

Trade Mark is designed to transform raw USPTO Parquet data into a query-friendly local database using DuckDB. It supports dataset download, schema inspection, data integration, large-file splitting, and CSV export for downstream research or business analysis.

## Why this project

USPTO trademark data is large, fragmented, and stored across multiple Parquet tables. This project consolidates those tables into a unified database and provides practical tools for:

- trademark lookup by serial number
- keyword search by mark name
- filtering by registration date
- exporting results to CSV
- processing large datasets in segmented DuckDB files

## Key features

- Download USPTO trademark data from Hugging Face
- Support optional mirror acceleration for domestic environments
- Build a unified DuckDB trademark database
- Aggregate key fields such as owner, class, correspondent, and status metadata
- Split large database files into manageable chunks
- Export query results to CSV for analysis or downstream integration
- Provide automation via GitHub Actions

## Project structure

```text
trade_mark/
├── .github/
│   └── workflows/
│       ├── process_data.yml
│       ├── build_window_exe.yml
│       └── main_trigger.yml
├── src/
│   ├── aaa.py
│   ├── all_data.py
│   ├── columns.py
│   ├── download.py
│   ├── fixe_for.py
│   ├── split_db.py
│   ├── test.py
│   ├── tiny_db.py
│   ├── to_duckdb.py
│   ├── to_duckdb_2.py
│   └── ...
├── setup.py
├── setup_bak.py
├── setup_split_bak.py
├── columns.md
├── uspto.md
├── .gitignore
├── README.md
└── ...
```

### Module overview

- `src/download.py` — downloads USPTO dataset from Hugging Face
- `src/to_duckdb.py` / `src/to_duckdb_2.py` — converts raw Parquet tables into DuckDB
- `src/split_db.py` — splits large DuckDB tables into smaller files
- `src/columns.py` — inspects source schema and fields
- `src/test.py` — query/test script for local validation
- `setup.py` — interactive query entry point for local use
- `columns.md` / `uspto.md` — schema and field references

## Requirements

- Python 3.10+
- DuckDB
- pandas
- requests
- beautifulsoup4
- huggingface_hub

Install dependencies:

```bash
pip install duckdb pandas requests beautifulsoup4 huggingface_hub
```

## Quick start

### 1. Clone the repository

```bash
git clone https://github.com/sundogya/trade_mark.git
cd trade_mark
```

### 2. Download the dataset

```bash
python src/download.py
```

Optional mirror:

```bash
python src/download.py --use_mirror
```

### 3. Build the DuckDB database

```bash
python src/to_duckdb.py
```

### 4. Split large database files

```bash
python src/split_db.py
```

### 5. Launch the interactive query tool

```bash
python setup.py
```

## Typical use cases

- search by trademark serial number
- search by mark name keyword
- filter by registration date
- export selected records as CSV

## Notes

- This project is intended for processing public USPTO trademark data.
- Large datasets may require significant disk space and memory.
- Some workflows use network requests and may be rate-limited.

## License

This project use MIT license.
