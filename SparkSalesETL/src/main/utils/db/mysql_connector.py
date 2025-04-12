from pathlib import Path
import mysql.connector
from mysql.connector import Error

class MySQLConnector:
        def __init__(self, config):
            self.config = config
            self.conn = None
            self.cursor = None

        def connect(self):
            try:
                self.conn = mysql.connector.connect(**self.config)
                self.cursor = self.conn.cursor(dictionary=True)
            except Error as e:
                print(f"Error connecting to MySQL: {e}")
                raise

        def get_connection(self):
            return self.conn

        def get_cursor(self):
            return self.cursor

        def initialize_schema(self, sql_file_path):

            with open(sql_file_path, "r") as file:
                sql_script = file.read()
            statements = sql_script.split(";")
            for stmt in statements:
                stmt = stmt.strip()
                if stmt:
                    self.cursor.execute(stmt)
            self.conn.commit()

        def close(self):
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
