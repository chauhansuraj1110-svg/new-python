from flask import Flask, request, redirect, render_template, jsonify
from db_connector import get_connection

app = Flask(__name__)

# ------------------ READ ------------------
#only http://127.0.0.1:5000/view/uusers to view users data
@app.route("/view/<table_name>", methods=["GET"])
def view_table(table_name):

    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # Check table exists
        cursor.execute("SHOW TABLES LIKE %s", (table_name,))
        if not cursor.fetchone():
            return jsonify({
                "status": "error",
                "message": f"Table '{table_name}' does not exist"
            }), 400

        # Get row count
        cursor.execute(f"SELECT COUNT(*) AS total FROM {table_name}")
        count = cursor.fetchone()["total"]

        # Fetch all data
        cursor.execute(f"SELECT * FROM {table_name}")
        data = cursor.fetchall()

        return jsonify({
            "status": "success",
            "table": table_name,
            "total_rows": count,
            "data": data
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

    finally:
        conn.close()
# ------------------ adding------------------
#add in form data in postman key and value http://127.0.0.1:5000/add/uusers
@app.route("/add/<table_name>", methods=["POST"])
def add_data(table_name):

    try:
        # Accept JSON or form-data
        data = request.get_json() if request.is_json else request.form.to_dict()

        if not data:
            return jsonify({
                "status": "error",
                "message": "No data provided"
            }), 400

        conn = get_connection()
        cursor = conn.cursor()

        # verify table exists
        cursor.execute("SHOW TABLES LIKE %s", (table_name,))
        if not cursor.fetchone():
            return jsonify({
                "status": "error",
                "message": f"Table '{table_name}' does not exist"
            }), 400

        # build dynamic insert
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["%s"] * len(data))
        values = tuple(data.values())

        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        cursor.execute(sql, values)
        conn.commit()

        return jsonify({
            "status": "success",
            "message": "Data inserted successfully",
            "inserted_data": data
        }), 200

    except Exception as e:
        conn.rollback()
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

    finally:
        conn.close()
# ------------------ UPDATE ------------------
#http://127.0.0.1:5000/update/uusers/1 add in postman key and value to update
@app.route("/update/<table_name>/<int:id>", methods=["POST"])
def update_data(table_name, id):

    try:
        # Accept JSON or form data
        data = request.get_json() if request.is_json else request.form.to_dict()

        if not data:
            return jsonify({
                "status": "error",
                "message": "No update data provided"
            }), 400

        conn = get_connection()
        cursor = conn.cursor()

        # Check table exists
        cursor.execute("SHOW TABLES LIKE %s", (table_name,))
        if not cursor.fetchone():
            return jsonify({
                "status": "error",
                "message": f"Table '{table_name}' does not exist"
            }), 400

        # Check row exists
        cursor.execute(f"SELECT * FROM {table_name} WHERE id=%s", (id,))
        if not cursor.fetchone():
            return jsonify({
                "status": "error",
                "message": f"No record found with id {id}"
            }), 404

        # Build dynamic update query
        columns = []
        values = []

        for key, value in data.items():
            columns.append(f"{key}=%s")
            values.append(value)

        values.append(id)

        sql = f"UPDATE {table_name} SET {', '.join(columns)} WHERE id=%s"

        cursor.execute(sql, tuple(values))
        conn.commit()

        return jsonify({
            "status": "success",
            "message": f"Record {id} updated in {table_name}",
            "updated_fields": data
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

    finally:
        if conn:
            conn.close()
# ------------------ DELETE ------------------
#http://127.0.0.1:5000/delete/uuuuuuuuuuuuu to delete full table
#http://127.0.0.1:5000/delete/uusers/1 to delete specific id
@app.route("/delete/<table_name>", methods=["DELETE"])
@app.route("/delete/<table_name>/<int:id>", methods=["DELETE"])
def delete_data(table_name, id=None):

    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Check table exists
        cursor.execute("SHOW TABLES LIKE %s", (table_name,))
        if not cursor.fetchone():
            return jsonify({
                "status": "error",
                "message": f"Table '{table_name}' does not exist"
            }), 400

        # ✅ CASE 1 — delete only ONE row
        if id is not None:

            cursor.execute(f"SELECT * FROM {table_name} WHERE id=%s", (id,))
            row = cursor.fetchone()

            if not row:
                return jsonify({
                    "status": "error",
                    "message": f"ID {id} not found"
                }), 404

            cursor.execute(f"DELETE FROM {table_name} WHERE id=%s", (id,))
            conn.commit()

            return jsonify({
                "status": "success",
                "message": f"Deleted ID {id} from {table_name}"
            }), 200

        # ✅ CASE 2 — delete FULL table
        else:
            cursor.execute(f"DELETE FROM {table_name}")
            conn.commit()

            return jsonify({
                "status": "success",
                "message": f"All records deleted from table {table_name}"
            }), 200

    except Exception as e:
        conn.rollback()
        return jsonify({
            "status": "error",
            "message": "Delete operation failed",
            "error": str(e)
        }), 500

    finally:
        conn.close()
#--------create table route--------
#http://127.0.0.1:5000/create_table to create table in row postman add in raw json
@app.route("/create_table", methods=["POST"])
def create_table():

    try:
        data = request.get_json()

        if not data:
            return "Invalid JSON", 400

        table_name = data.get("table_name")
        columns = data.get("columns")

        if not table_name or not columns:
            return "Missing table_name or columns", 400

        conn = get_connection()
        cursor = conn.cursor()

        # Check if table exists
        cursor.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
              AND table_name = %s
        """, (table_name,))

        (exists,) = cursor.fetchone()
        if exists:
            return "Table already exists!", 400

        # Build column query
        col_list = []

        for col_name, col_type in columns.items():
            col_list.append(f"{col_name} {col_type}")

        col_query = ", ".join(col_list)

        # Final create query
        sql = f"CREATE TABLE {table_name} ({col_query})"

        cursor.execute(sql)
        conn.commit()

        return f"Table '{table_name}' created successfully!"

    except Exception as e:
        return f"Error: {str(e)}", 500

    finally:
        conn.close()

#---------testing route---------
@app.route("/test")
def test_route():
    return "Test route is working!"
# Run the app if this file is executed directly
#http://127.0.0.1:5000

if __name__ == "__main__":
    app.run(debug=True)


#http://127.0.0.1:8000/upload/PROVIDER_TEST_DETAILS to upload csv file
#go to body in postman select form-data key as file and select csv file to upload
