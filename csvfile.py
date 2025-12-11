from fastapi import FastAPI, UploadFile, File, HTTPException, Request
import pandas as pd
import io
from db_connector import get_connection
import logging
import asyncio

# ---------------------------
# Logging setup
# ---------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------
# FastAPI app
# ---------------------------
app = FastAPI(title="CSV/Excel Upload API")


# ---------------------------
# Check if table exists
# ---------------------------
def check_table_exists(cursor, table_name: str):
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    result = cursor.fetchone()
    if not result:
        logging.error(f"Table '{table_name}' does NOT exist.")
        raise HTTPException(status_code=400, detail=f"Table '{table_name}' does NOT exist.")
    logging.info(f"Table '{table_name}' exists.")


# ---------------------------
# Get table columns
# ---------------------------
def get_table_columns(cursor, table_name: str):
    cursor.execute(f"DESCRIBE {table_name}")
    columns = [row[0] for row in cursor.fetchall()]
    logging.info(f"Table '{table_name}' columns: {columns}")
    return columns


# ---------------------------
# Upload API with CANCEL SUPPORT
# ---------------------------
@app.post("/upload/{table_name}")
async def upload_file(table_name: str, request: Request, file: UploadFile = File(...)):
    try:
        file_bytes = await file.read()
        logging.info(f"Received file: {file.filename} ({len(file_bytes)} bytes)")

        if file.filename.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(file_bytes))
        elif file.filename.endswith((".xlsx", ".xls")):
            df = pd.read_excel(io.BytesIO(file_bytes))
        else:
            raise HTTPException(status_code=400, detail="Only CSV, XLS, XLSX files are allowed.")

        logging.info(f"Loaded DataFrame with shape: {df.shape}")

        df = df.where(pd.notnull(df), None)

        conn = get_connection()
        cursor = conn.cursor()

        check_table_exists(cursor, table_name)
        table_cols = get_table_columns(cursor, table_name)

        missing_cols = [col for col in table_cols if col not in df.columns]
        if missing_cols:
            logging.error(f"Missing required columns: {missing_cols}")
            raise HTTPException(status_code=400, detail=f"Missing required column(s): {missing_cols}")

        insert_query = f"""
            INSERT INTO {table_name} ({', '.join(table_cols)})
            VALUES ({', '.join(['%s'] * len(table_cols))})
        """

        row_count = 0

        try:
            for idx, row in df.iterrows():

                # 🔥 Check if Postman/Client disconnected
                if await request.is_disconnected():
                    logging.warning("Client disconnected! Stopping insertion and rolling back.")
                    conn.rollback()
                    raise HTTPException(status_code=499, detail="Client cancelled the request")

                values = [None if pd.isna(row.get(col, None)) else row.get(col, None)
                          for col in table_cols]

                cursor.execute(insert_query, tuple(values))
                row_count += 1

                logging.info(f"Inserted row {idx+1}")

                await asyncio.sleep(0)  # allow cancellation

            conn.commit()
            logging.info(f"Inserted {row_count} rows successfully.")

        except Exception as e:
            conn.rollback()
            logging.error("Failed to insert data: " + str(e))
            raise e
        finally:
            cursor.close()
            conn.close()

        return {"status": "success", "rows_inserted": row_count}

    except Exception as e:
        logging.exception("Unexpected error occurred")
        raise HTTPException(status_code=500, detail=str(e))

# ---------------------------
# Direct run support
# ---------------------------
if __name__ == "__main__":
    import uvicorn
    logging.info("Starting FastAPI upload API...")
    uvicorn.run("csvfile:app", host="127.0.0.1", port=8000, reload=True)
