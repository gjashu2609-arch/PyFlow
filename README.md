# PyFlow — Production ETL Framework

## Setup
pip install -r requirements.txt

## Usage
python main.py

## running pytest 

pytest tests/ -v



## Dataset
- NYC Yellow Taxi Trip Records (January 2024)
- 3M+ rows, 500MB CSV
- Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Test Results
24 passed in 0.35s — 100% passing


## project structure
- extractors.py  — reads CSV, JSON, Parquet, Excel
- transformers.py — cleans and enriches data
- loaders.py     — batch inserts to SQLite
- validators.py  — data validation
- logger.py      — rotating file logging
- main.py        — orchestrates full pipeline
pyflow/
├── pyflow/
│   ├── extractors.py    — reads CSV, JSON, Parquet, Excel, compressed files
│   ├── transformers.py  — cleans, validates, and enriches data
│   ├── loaders.py       — batch inserts to SQLite database
│   ├── validators.py    — email, phone, date range validation
│   ├── logger.py        — rotating file logging (INFO + ERROR)
│   ├── models.py        — TripRecord dataclass with hash/eq
│   ├── benchmarks.py    — list vs set performance comparison
│   └── exceptions.py    — custom exception hierarchy
├── tests/
│   └── test_pyflow.py   — 24 unit tests (100% passing)
├── config/
│   └── config.yaml      — controls entire ETL flow
├── data/
│   └── output/          — exported files saved here
├── logs/
│   └── pyflow.log       — rotating logg file
├── main.py              — pipeline orchestrator
└── requirements.txt