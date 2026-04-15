from pymongo import MongoClient
import json
import os

client = MongoClient("mongodb://localhost:27017")
databases = client.list_database_names()
print(databases)

# Load existing metadata.json if it exists, otherwise start fresh
metadata_file = "metadata.json"

if os.path.exists(metadata_file):
    with open(metadata_file, "r", encoding="utf-8") as f:
        metadata = json.load(f)
else:
    metadata = {"engine": {}}

# Ensure the engine key exists
if "engine" not in metadata:
    metadata["engine"] = {}

# Build MongoDB metadata
mongodb_meta = {}

for db_name in databases:
    if db_name not in ["admin", "config", "local"]:
        db = client[db_name]
        mongodb_meta[db_name] = {}

        for col_name in db.list_collection_names():
            collection = db[col_name]
            sample = collection.find_one()

            if sample is None:
                continue

            columns = {}
            for field, value in sample.items():
                columns[field] = type(value).__name__

            mongodb_meta[db_name][col_name] = columns

# Append/overwrite only the mongodb section
metadata["engine"]["mongodb"] = mongodb_meta

# Write back to metadata.json
with open(metadata_file, "w", encoding="utf-8") as f:
    json.dump(metadata, f, indent=4)

print(f"MongoDB metadata appended to '{metadata_file}' successfully.")