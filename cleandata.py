from flask import Flask, jsonify, request
import pandas as pd
from db_connector import get_connection
import os

app = Flask(__name__)

# Folder to store cleaned files
CLEANED_DIR = "cleaned_data"
os.makedirs(CLEANED_DIR, exist_ok=True)

# Fetch data from DB dynamically
def get_data(table_name):
    conn = get_connection()
    # Using safe string formatting to prevent SQL injection
    query = f"SELECT * FROM `{table_name}`"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Data cleaning
def clean_data(df):
    df = df.drop_duplicates()
    if 'age' in df.columns:
        df['age'] = df['age'].fillna(df['age'].mean())
        df = df[df['age'] < 120]
    if 'city' in df.columns:
        df['city'] = df['city'].fillna("Unknown")
        df['city'] = df['city'].str.lower().str.strip()
    return df

# API Endpoint to save cleaned data locally
@app.route("/save-clean-data-local", methods=["GET"])
def save_clean_data_local():
    # Get table name from query parameter
    table_name = request.args.get("table")
    if not table_name:
        return {"error": "Please provide a table name as query parameter, e.g. ?table=employees"}, 400

    try:
        df = get_data(table_name)
    except Exception as e:
        return {"error": f"Failed to fetch data from table '{table_name}': {str(e)}"}, 400

    df_clean = clean_data(df)

    # Save cleaned data as CSV
    timestamp = pd.Timestamp.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = os.path.join(CLEANED_DIR, f"{table_name}_cleaned_{timestamp}.csv")
    df_clean.to_csv(filename, index=False)

    return {"message": f"Cleaned data saved locally at {filename}"}


if __name__ == "__main__":
    app.run(debug=True)
