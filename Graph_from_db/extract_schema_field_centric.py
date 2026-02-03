import json
from collections import defaultdict

INPUT_FILE = "pii_demo.json"
OUTPUT_FILE = "schema_d3.json"

with open(INPUT_FILE) as f:
    data = json.load(f)




datatype_lookup = {}

system_types = (
    data.get("catalog", {})
        .get("system-column-data-types", [])
)

if isinstance(system_types, list) and len(system_types) > 1:
    for dt in system_types[1]:
        if isinstance(dt, dict) and "@uuid" in dt:
            datatype_lookup[dt["@uuid"]] = dt.get("name", "UNKNOWN")


field_to_tables = defaultdict(set)
field_datatype = {}

columns = data.get("all-table-columns", [None, []])[1]

for col in columns:
    # full-name: <db>.<table>.<column>
    _, table, column = col["full-name"].split(".")

    field_to_tables[column].add(table)

    # Resolve datatype once per field
    col_type = col.get("column-data-type")
    if column not in field_datatype:
        if isinstance(col_type, dict):
            field_datatype[column] = col_type.get("name", "UNKNOWN")
        elif isinstance(col_type, str):
            field_datatype[column] = datatype_lookup.get(col_type, "UNKNOWN")
        else:
            field_datatype[column] = "UNKNOWN"

  

  
nodes = {}
links = []

# Field nodes (CENTER)
for field, dtype in field_datatype.items():
    nodes[field] = {
        "id": field,
        "type": "field",
        "datatype": dtype
    }

# Table nodes + links
for field, tables in field_to_tables.items():
    for table in tables:
        if table not in nodes:
            nodes[table] = {
                "id": table,
                "type": "table"
            }

        links.append({
            "source": field,
            "target": table
        })

graph = {
    "nodes": list(nodes.values()),
    "links": links
}

with open(OUTPUT_FILE, "w") as f:
    json.dump(graph, f, indent=2)

print("[OK] Generated field-centric schema graph:", OUTPUT_FILE)
