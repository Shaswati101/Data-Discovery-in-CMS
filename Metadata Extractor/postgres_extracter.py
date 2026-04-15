from sqlalchemy import create_engine, inspect, text
import json
import os

USERNAME = "postgres"
PASSWORD = "12345"
HOST = "localhost"
PORT = 5432

TARGET_DB = "behavioural_profiling"

base_engine = create_engine(
    f"postgresql+psycopg2://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/postgres"
)

with base_engine.connect() as conn:
    databases = conn.execute(text("""
        SELECT datname
        FROM pg_database
        WHERE datistemplate = false
    """)).scalars().all()

print(databases)

# Build nested structure: engine > database > table > column: datatype
postgres_metadata = {"postgres": {}}

for db in databases:
    if db not in ["postgres"] and db == TARGET_DB:
        db_engine = create_engine(
            f"postgresql+psycopg2://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{db}"
        )
        inspector = inspect(db_engine)

        postgres_metadata["postgres"][db] = {}

        for schema in inspector.get_schema_names():
            if schema in ("pg_catalog", "information_schema"):
                continue

            for table in inspector.get_table_names(schema=schema):
                postgres_metadata["postgres"][db][table] = {}

                for column in inspector.get_columns(table, schema=schema):
                    col_name = column["name"]
                    col_type = str(column["type"])
                    postgres_metadata["postgres"][db][table][col_name] = col_type

# Wrap under "engine" key to match the format
new_entry = {"engine": postgres_metadata}

# Load existing metadata.json if it exists, else start fresh
output_file = "metadata.json"

if os.path.exists(output_file):
    with open(output_file, "r", encoding="utf-8") as f:
        existing = json.load(f)
else:
    existing = {"engine": {}}

# Merge postgres key into existing engine block
existing["engine"]["postgres"] = postgres_metadata["postgres"]

# Write back to metadata.json
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(existing, f, indent=4)

print(f"Metadata appended to {output_file}")