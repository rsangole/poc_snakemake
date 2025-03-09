import polars as pl
import yaml
import sys
from typing import List, Dict

def load_config():
    with open("config/config.yaml", "r") as f:
        return yaml.safe_load(f)

def validate_data(df: pl.DataFrame, config: Dict) -> pl.DataFrame:
    validation_rules = config["validation"]
    required_columns = validation_rules["required_columns"]
    
    # Check required columns
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Check for missing values
    for col in required_columns:
        if df[col].null_count() > 0:
            raise ValueError(f"Found missing values in column: {col}")
    
    # Validate price values
    if df.filter(pl.col("price") < validation_rules["min_price"]).height > 0:
        raise ValueError("Found negative or zero prices")
    
    # Additional validation: ensure timestamps are unique
    if df.select(pl.col("timestamp").is_duplicated()).sum().item() > 0:
        raise ValueError("Found duplicate timestamps")
    
    # Sort by timestamp
    df = df.sort("timestamp")
    
    return df

def main():
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    config = load_config()
    
    # Read CSV with automatic type inference
    df = pl.read_csv(input_file, try_parse_dates=True)
    
    try:
        validated_df = validate_data(df, config)
        validated_df.write_csv(output_file)
        print(f"Validated data saved to {output_file}")
    
    except ValueError as e:
        print(f"Validation error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()