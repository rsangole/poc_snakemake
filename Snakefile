# Snakefile

rule all:
    input:
        "data/crypto.duckdb",
        "data/crypto2.duckdb"

rule fetch_data:
    output:
        "data/raw/crypto_data.csv"
    shell:
        "python scripts/fetch_data.py {output}"

rule validate_data:
    input:
        "data/raw/crypto_data.csv"
    output:
        "data/processed/crypto_data_validated.csv"
    shell:
        "python scripts/validate_data.py {input} {output}"

rule load_to_duckdb:
    input:
        "data/processed/crypto_data_validated.csv"
    output:
        "data/crypto.duckdb"
    shell:
        "python scripts/load_to_duckdb.py {input}"

rule duckdb_using_script:
    input:
        "data/processed/crypto_data_validated.csv"
    output:
        "data/crypto2.duckdb"
    script:
        "scripts/duckdb.py"