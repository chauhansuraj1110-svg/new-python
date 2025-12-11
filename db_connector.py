import os
import mysql.connector
import configparser

def get_connection():
    """Connect to MySQL using credentials from config/config.ini"""
    
    # Absolute path to config.ini
    config_path = os.path.join(os.path.dirname(__file__), 'config', 'config.ini')
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")
    
    # Read credentials from config.ini
    config = configparser.ConfigParser()
    config.read(config_path)
    
    host = config['mysql']['host']
    user = config['mysql']['user']
    password = config['mysql']['password']
    database = config['mysql']['database']
    
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            autocommit=True 
        )
        print("Database connection successful UAT")
        return connection
    except mysql.connector.Error as err:
        print(f"Database connection UAT failed: {err}")
        return None


if __name__ == '__main__':
    conn = get_connection()