# Cryptocurrency Data Pipeline with Snakemake

A data pipeline that fetches cryptocurrency price data from CoinGecko, validates it, and stores it in DuckDB for analysis. Built with modern Python tools including Polars, DuckDB, and Ibis.

## Features

- Data fetching from CoinGecko API
- Data validation with Polars
- Local storage in DuckDB
- Pipeline orchestration with Snakemake
- Modern Python tooling (Polars, Ibis, DuckDB)

## Project Structure

```
.
├── config/
│   └── config.yaml        # Pipeline configuration
├── data/                  # Data directory (created by pipeline)
│   ├── raw/              # Raw API data
│   └── processed/        # Validated data
├── scripts/
│   ├── fetch_data.py     # API data fetching
│   ├── validate_data.py  # Data validation
│   └── load_to_duckdb.py # DuckDB loading
├── Snakefile             # Pipeline definition
└── requirements.txt      # Python dependencies
```

## Setup

1. Install system dependencies:
   ```bash
   # macOS
   brew install cmake graphviz apache-arrow lz4 zstd

   # Ubuntu/Debian
   sudo apt-get update && sudo apt-get install -y \
       cmake graphviz libarrow-dev liblz4-dev libzstd-dev
   ```

2. Install uv:
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

3. Create virtual environment and install dependencies:
   ```bash
   uv venv
   source .venv/bin/activate
   uv pip install -r requirements.txt
   ```

## Running the Pipeline

1. Configure the pipeline in `config/config.yaml`:
   ```yaml
   api:
     coin_id: "bitcoin"
     days: 30
   ```

2. Run the pipeline:
   ```bash
   snakemake --cores 1
   ```

3. View the pipeline DAG:
   ```bash
   snakemake --dag | dot -Tpng > dag.png
   ```

## Pipeline Steps

1. **Fetch Data** (`fetch_data.py`)
   - Retrieves cryptocurrency data from CoinGecko API
   - Saves raw data as CSV

2. **Validate Data** (`validate_data.py`)
   - Checks for missing values
   - Validates data types
   - Ensures price values are positive
   - Verifies timestamp uniqueness

3. **Load Data** (`load_to_duckdb.py`)
   - Stores validated data in DuckDB
   - Uses Arrow for efficient data transfer
   - Creates table if not exists

## Querying Data

After running the pipeline, you can query the data using DuckDB:

```python
import duckdb
import polars as pl

# Connect to database
con = duckdb.connect('data/crypto.duckdb')

# Example query
df = pl.from_arrow(
    con.sql('SELECT * FROM crypto_prices ORDER BY timestamp DESC LIMIT 5')
    .arrow()
)
print(df)
```

## Development Notes

- Uses uv instead of pip for faster package management
- Polars for efficient DataFrame operations
- DuckDB for local data storage and querying
- Arrow for zero-copy data transfer
- Snakemake for pipeline orchestration

## Troubleshooting

If you encounter issues:

1. Ensure all system dependencies are installed
2. Check the DuckDB file permissions
3. Verify API rate limits haven't been exceeded
4. Check logs in the terminal output

## Contributing

1. Fork the repository
2. Create a feature branch
3. Submit a pull request