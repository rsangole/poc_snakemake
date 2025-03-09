import requests
import polars as pl
import yaml
from datetime import datetime
import sys

def load_config():
    with open("config/config.yaml", "r") as f:
        return yaml.safe_load(f)

def fetch_crypto_data(config):
    api_config = config["api"]
    url = f"https://api.coingecko.com/api/v3/coins/{api_config['coin_id']}/market_chart"
    
    params = {
        "vs_currency": api_config["vs_currency"],
        "days": api_config["days"],
        "interval": api_config["interval"]
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Create DataFrame using Polars
        df = pl.DataFrame({
            "timestamp": [x[0] for x in data["prices"]],
            "price": [x[1] for x in data["prices"]],
            "volume": [x[1] for x in data["total_volumes"]],
            "market_cap": [x[1] for x in data["market_caps"]]
        })
        
        # Convert timestamp from milliseconds to datetime
        df = df.with_columns(
            pl.col("timestamp")
            .cast(pl.Int64)
            .map_elements(lambda x: datetime.fromtimestamp(x / 1000))
            .alias("timestamp")
        )
        
        return df
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    output_file = sys.argv[1]
    config = load_config()
    
    df = fetch_crypto_data(config)
    df.write_csv(output_file)
    print(f"Data saved to {output_file}")

if __name__ == "__main__":
    main()