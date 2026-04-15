from sqlalchemy import create_engine, inspect
import json

exceptions = [
    'information_schema', 'mysql', 'sakila',
    'performance_schema', 'news_details',
    'world', 'sys', 'sample'
]

engine = create_engine(
    "mysql+pymysql://root:12345@localhost:3306/"
)

inspector = inspect(engine)
databases = inspector.get_schema_names()

# to get table names
def get_tables(database):
    db_engine = create_engine(
        f"mysql+pymysql://root:12345@localhost:3306/{database}"
    )
    db_inspector = inspect(db_engine)
    return db_inspector.get_table_names()

# to get column names
def get_columns(database, table):
    db_engine = create_engine(
        f"mysql+pymysql://root:12345@localhost:3306/{database}"
    )
    db_inspector = inspect(db_engine)
    return db_inspector.get_columns(table)

# Final structured metadata
metadata = {
    "engine": {
        "mysql": {}
    }
}

for db in databases:
    if db in exceptions:
        continue

    metadata["engine"]["mysql"][db] = {}
    tables = get_tables(db)

    for table in tables:
        metadata["engine"]["mysql"][db][table] = {}
        columns = get_columns(db, table)

        for col in columns:
            metadata["engine"]["mysql"][db][table][col["name"]] = str(col["type"])

# Write to JSON
with open("metadata.json", "w", encoding="utf-8") as f:
    json.dump(metadata, f, indent=4)

print("Nested metadata.json generated successfully.")

