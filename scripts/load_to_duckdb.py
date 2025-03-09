import ibis
import polars as pl
import yaml
import sys
from pathlib import Path

def load_config():
    with open("config/config.yaml", "r") as f:
        return yaml.safe_load(f)

def connect_to_duckdb(config):
    db_config = config["duckdb"]
    db_path = db_config["database"]
    
    # Create the directory if it doesn't exist
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Connect using Ibis
    return ibis.duckdb.connect(db_path)

def load_data_to_duckdb(con, df: pl.DataFrame, table_name: str):
    # Convert Polars DataFrame to Arrow Table (DuckDB works well with Arrow)
    arrow_table = df.to_arrow()
    
    # Load data using DuckDB's native interface
    con.create_table(table_name, obj=arrow_table, overwrite=True)

def main():
    input_file = sys.argv[1]
    
    try:
        config = load_config()
        df = pl.read_csv(input_file, try_parse_dates=True)
        
        # Connect to DuckDB
        con = connect_to_duckdb(config)
        table_name = config["duckdb"]["table"]
        
        # Load data
        load_data_to_duckdb(con, df, table_name)
        print(f"Data successfully loaded to DuckDB table: {table_name}")
        
        # Execute a quick verification query
        result = con.table(table_name).count().execute()
        print(f"Total rows in table: {result}")
        
    except Exception as e:
        print(f"Error loading data to DuckDB: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        # No need to explicitly close with Ibis DuckDB connection
        pass

if __name__ == "__main__":
    main()