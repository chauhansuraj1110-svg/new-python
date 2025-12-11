# find_customer_tables_UAT.py
from db_connector import get_connection
import csv

# Connect using external connector file
conn = get_connection()
cursor = conn.cursor()

# Keywords indicating customer-related fields
keywords = ["name", "customer", "phone", "mobile", "contact", "address", "email"]

# Only check UAT schema
db_name = "UAT"

# Build SQL query using GROUP_CONCAT (MySQL)
query = """
SELECT 
    table_schema, 
    table_name, 
    GROUP_CONCAT(column_name ORDER BY column_name SEPARATOR ', ') AS columns
FROM information_schema.columns
WHERE table_schema = %s
AND ({})
GROUP BY table_schema, table_name
ORDER BY table_name;
""".format(" OR ".join([f"LOWER(column_name) LIKE '%{kw}%'" for kw in keywords]))

cursor.execute(query, (db_name,))
results = cursor.fetchall()

# Prepare data for CSV
csv_data = []
for schema, table, columns in results:
    csv_data.append({
        "database": schema,
        "table_name": table,
        "columns": columns
    })

# Write to CSV
csv_file = "customer_tables_UAT.csv"
with open(csv_file, mode='w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=["database", "table_name", "columns"])
    writer.writeheader()
    writer.writerows(csv_data)

print(f"CSV file '{csv_file}' has been created with {len(csv_data)} rows.")

cursor.close()
conn.close()
