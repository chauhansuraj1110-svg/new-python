import os
import subprocess
import datetime
import logging
import shutil
from db_connector import get_connection

# ---------------------------
# Logging Setup
# ---------------------------
logging.basicConfig(
    filename="backup.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

BACKUP_DIR = "db_backups"


# ------------------------------------------
# Auto-detect mysqldump.exe
# ------------------------------------------
def find_mysqldump():
    # 1: Check PATH
    path = shutil.which("mysqldump")
    if path:
        return path

    # 2: Common MySQL locations
    common_paths = [
        r"C:\xampp\mysql\bin\mysqldump.exe",
        r"C:\wamp64\bin\mysql\mysql8.0.30\bin\mysqldump.exe",
        r"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysqldump.exe",
        r"C:\Program Files\MySQL\MySQL Server 5.7\bin\mysqldump.exe",
        r"C:\Program Files (x86)\MySQL\MySQL Server 8.0\bin\mysqldump.exe",
    ]

    for p in common_paths:
        if os.path.exists(p):
            return p

    return None


# ------------------------------------------
# Create backup folder
# ------------------------------------------
def create_backup_directory():
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR)
        logging.info("Backup directory created: " + BACKUP_DIR)


# ------------------------------------------
# Perform MySQL Backup
# ------------------------------------------
def backup_database():
    try:
        # Detect mysqldump
        mysqldump_path = find_mysqldump()
        if not mysqldump_path:
            raise Exception("mysqldump.exe not found. Install MySQL or add mysqldump to PATH.")

        print("Using mysqldump at:", mysqldump_path)

        conn = get_connection()
        if conn is None:
            raise Exception("Database connection failed")

        create_backup_directory()

        database = conn.database
        host = conn.server_host
        user = conn.user

        conn.close()

        # Get password from config.ini
        import configparser
        config_path = os.path.join(os.path.dirname(__file__), 'config', 'config.ini')
        config = configparser.ConfigParser()
        config.read(config_path)
        password = config['mysql']['password']

        # Backup filename
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        backup_file = os.path.join(BACKUP_DIR, f"{database}_backup_{timestamp}.sql")

        # Build command
        command = [
            mysqldump_path,
            f"-h{host}",
            f"-u{user}",
            f"-p{password}",
            database
        ]

        print("Running:", command)

        with open(backup_file, "w") as outfile:
            result = subprocess.run(command, stdout=outfile, stderr=subprocess.PIPE, text=True)

        if result.returncode == 0:
            print(f"Backup successful: {backup_file}")
            logging.info(f"Backup successful: {backup_file}")
        else:
            print("Backup error:", result.stderr)
            logging.error("Backup error: " + result.stderr)

    except Exception as e:
        print("Unexpected error:", str(e))
        logging.error("Unexpected error: " + str(e))


if __name__ == "__main__":
    backup_database()
