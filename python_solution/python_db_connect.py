# Path: python_solution/python_db_connect.py

# Import the required libraries
import os
import psycopg2
from .python_logger import logs
from psycopg2 import OperationalError


class Database:
    def __init__(self):
        self.conn = None
        self.cur = None
        self.logger = logs(__name__)

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname=os.environ.get("DB_NAME"),
                user=os.environ.get("DB_USER"),
                password=os.environ.get("DB_PASSWORD"),
                host=os.environ.get("DB_HOST"),
                port=os.environ.get("DB_PORT")
            )
            self.cur = self.conn.cursor()
            self.logger.info("Connected to the database.")
        except OperationalError as e:
            self.logger.error(f"Error: {e}")

    def disconnect(self):
        try:
            if self.cur:
                self.cur.close()
            if self.conn:
                self.conn.close()
            self.logger.info("Disconnected from the database.")
        except Exception as e:
            self.logger.error(f"Error: {e}")
            
    def execute(self, query):
        try:
            self.cur.execute(query)
            self.logger.info("Query executed successfully.")
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")

    def commit(self):
        try:
            self.conn.commit()
            self.logger.info("Changes committed successfully.")
        except Exception as e:
            self.logger.error(f"Error committing changes: {e}")
