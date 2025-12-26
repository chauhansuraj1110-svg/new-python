from db_connector import get_connection
import pandas as pd

def find_column_data_issues():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
    """)

    columns = cursor.fetchall()
    results = []

    for schema, table, column, data_type in columns:
        schema_quoted = f"`{schema}`"
        table_quoted = f"`{table}`"
        column_quoted = f"`{column}`"

        # For all string types, use LENGTH(TRIM()) to avoid collation issues
        if data_type in ('char','varchar','text','mediumtext','longtext'):
            query = f"""
                SELECT
                    COUNT(*) AS total_rows,
                    SUM(CASE WHEN {column_quoted} IS NOT NULL THEN 1 ELSE 0 END) AS non_null_rows,
                    SUM(CASE WHEN {column_quoted} IS NOT NULL AND LENGTH(TRIM({column_quoted})) > 0 THEN 1 ELSE 0 END) AS non_space_rows
                FROM {schema_quoted}.{table_quoted}
            """
        else:
            # For other types, only check for NULLs
            query = f"""
                SELECT
                    COUNT(*) AS total_rows,
                    SUM(CASE WHEN {column_quoted} IS NOT NULL THEN 1 ELSE 0 END) AS non_null_rows
                FROM {schema_quoted}.{table_quoted}
            """

        try:
            cursor.execute(query)

            if data_type in ('char','varchar','text','mediumtext','longtext'):
                total, non_null, non_space = cursor.fetchone()
            else:
                total, non_null = cursor.fetchone()
                non_space = None

            # Only include columns/tables with issues
            if total == 0:
                status = "EMPTY TABLE"
            elif non_null == 0:
                status = "COLUMN ALL NULL"
            elif non_space == 0 and non_space is not None:
                status = "COLUMN NULL OR SPACES ONLY"
            elif non_null < total:
                status = "COLUMN HAS DATA BUT SOME NULLS"
            else:
                continue  # Fully populated, skip

            results.append({
                "Schema": schema,
                "Table": table,
                "Column": column,
                "Data Type": data_type,
                "Status": status,
                "Total Rows": total,
                "Non-NULL Rows": non_null,
                "Non-Space Rows": non_space
            })

        except Exception as e:
            print(f"Error processing {schema}.{table}.{column}: {e}")
            conn.rollback()
            continue

    cursor.close()
    conn.close()
    return results

if __name__ == "__main__":
    data = find_column_data_issues()

    if not data:
        print("No data quality issues found.")
    else:
        df = pd.DataFrame(data)
        output_file = "data_quality_issues_report_fixed.xlsx"
        df.to_excel(output_file, index=False)
        print(f"\n✅ Excel report generated successfully: {output_file}")
