
import pyodbc
from sqlalchemy import create_engine

class AzureSQLDatabaseModule:
    def __init__(self,server,database,username,password):
        self._server=server
        self._database=database
        self._username=username
        self._password=password
        self._driver='ODBC Driver 17 for SQL Server'
        self._connection_string = f"DRIVER={{{self._driver}}};SERVER={self._server};DATABASE={self._database};UID={self._username};PWD={self._password}"
        self._charset='utf8mb4'
        self._engine=engine = create_engine(f"mssql+pyodbc://{self._username}:{self._password}@{self._server}/{self._database}?driver={self._driver}&charset={self._charset}")