from flask import Flask
from create import app as create   # your create.py Flask app
# from project_flow import some_function   # example import if needed
# from db_connector import get_connection  # DB import if needed

# -----------------------------------------
# MASTER APP — Combine or Run Single App
# -----------------------------------------

app = create  # use the app created in create.py


if __name__ == "__main__":
    print("🚀 Starting Flask server (main.py)...")
    app.run(debug=True)

