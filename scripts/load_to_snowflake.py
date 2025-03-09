import ibis
import polars as pl
import yaml
import sys
from ibis import _
import os

def load_config():
    with open("config/config.yaml", "r") as f:
        return yaml.safe_load(f)

def connect_to_snowflake(config):
    snowflake_config = config["snowflake"]
    
    # Connect using Ibis
    connection = ibis.snowflake.connect(
        account=snowflake_config["account"],
        user=os.environ.get("SNOWFLAKE_USER"),
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        database=snowflake_config["database"],
        schema=snowflake_config["schema"],
        warehouse=snowflake_config["warehouse"],
        role=snowflake_config["role"]
    )
    
    return connection

def create_table_if_not_exists(connection, table_name):
    # Check if table exists
    if not connection.table_exists(table_name):
        # Create table schema
        schema = ibis.schema([
            ("timestamp", "timestamp"),
            ("price", "double"),
            ("volume", "double"),
            ("market_cap", "double")
        ])
        
        # Create table
        connection.create_table(table_name, schema=schema)

def load_data_to_snowflake(connection, df, table_name):
    # Convert Polars DataFrame to Pandas for Ibis compatibility
    # Note: This is temporary until Ibis adds native Polars support
    pandas_df = df.to_pandas()
    
    # Load data
    connection.load_data(
        table_name,
        pandas_df,
        if_exists="append"
    )

def main():
    input_file = sys.argv[1]
    
    try:
        config = load_config()
        df = pl.read_csv(input_file, try_parse_dates=True)
        
        # Connect to Snowflake
        connection = connect_to_snowflake(config)
        table_name = config["snowflake"]["table"]
        
        # Create table if it doesn't exist
        create_table_if_not_exists(connection, table_name)
        
        # Load data
        load_data_to_snowflake(connection, df, table_name)
        print(f"Data successfully loaded to Snowflake table: {table_name}")
        
    except Exception as e:
        print(f"Error loading data to Snowflake: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()