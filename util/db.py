import sqlite3
import pandas as pd
import os
from typing import Union, List, Dict, Any, Optional


class DatabaseManager:
    """
    A class to manage SQLite database operations for CSV data.
    """
    
    def __init__(self, db_path: str = "data.db"):
        """
        Initialize the database manager with a database file path.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self._ensure_db_exists()
    
    def _ensure_db_exists(self) -> None:
        """Create the database file if it doesn't exist."""
        if not os.path.exists(self.db_path):
            # Just connecting will create the file
            conn = sqlite3.connect(self.db_path)
            conn.close()
            print(f"Created new database at {self.db_path}")
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get a connection to the database."""
        return sqlite3.connect(self.db_path)
    
    def _df_to_sql_create_statement(self, df: pd.DataFrame, table_name: str) -> str:
        """
        Generate a SQL CREATE TABLE statement based on DataFrame columns.
        
        Args:
            df: pandas DataFrame
            table_name: Name of the table to create
            
        Returns:
            SQL CREATE TABLE statement
        """
        col_types = []
        
        for col in df.columns:
            dtype = df[col].dtype
            
            # Map pandas dtypes to SQLite types
            if pd.api.types.is_integer_dtype(dtype):
                sql_type = "INTEGER"
            elif pd.api.types.is_float_dtype(dtype):
                sql_type = "REAL"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                sql_type = "TIMESTAMP"
            else:
                sql_type = "TEXT"
            
            # Make StartTime a unique field to prevent duplicates
            if col == "StartTime":
                col_types.append(f'"{col}" {sql_type} PRIMARY KEY')
            else:
                col_types.append(f'"{col}" {sql_type}')
        
        create_stmt = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        create_stmt += ",\n".join(col_types)
        create_stmt += "\n);"
        
        return create_stmt
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            True if the table exists, False otherwise
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name=?;
        """, (table_name,))
        
        exists = cursor.fetchone() is not None
        conn.close()
        
        return exists
    
    def create_table_from_dataframe(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Create a table based on DataFrame structure if it doesn't exist.
        
        Args:
            df: pandas DataFrame
            table_name: Name of the table to create
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Create the table if it doesn't exist
        create_stmt = self._df_to_sql_create_statement(df, table_name)
        cursor.execute(create_stmt)
        
        conn.commit()
        conn.close()
        
        print(f"Table '{table_name}' is ready")
    
    def append_dataframe(self, df: pd.DataFrame, table_name: str) -> int:
        """
        Append DataFrame to a database table. Creates the table if it doesn't exist.
        Handles duplicates based on StartTime.
        
        Args:
            df: pandas DataFrame to append
            table_name: Target table name
            
        Returns:
            Number of rows successfully inserted
        """
        # Ensure StartTime column exists
        if "StartTime" not in df.columns:
            raise ValueError("DataFrame must contain a 'StartTime' column for duplicate prevention")
        
        # Create table if it doesn't exist
        if not self.table_exists(table_name):
            self.create_table_from_dataframe(df, table_name)
        
        conn = self._get_connection()
        
        # Get existing StartTime values
        existing_times = pd.read_sql(f"SELECT StartTime FROM {table_name}", conn)
        
        # Filter out rows with existing StartTime values
        if not existing_times.empty:
            df = df[~df["StartTime"].isin(existing_times["StartTime"])]
        
        if df.empty:
            print("No new data to insert (all rows are duplicates)")
            conn.close()
            return 0
        
        # Insert new rows
        df.to_sql(table_name, conn, if_exists="append", index=False)
        
        row_count = len(df)
        conn.commit()
        conn.close()
        
        print(f"Inserted {row_count} new rows into '{table_name}'")
        return row_count
    
    def execute_query(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return the results as a list of dictionaries.
        
        Args:
            query: SQL query to execute
            params: Parameters for the query
            
        Returns:
            Query results as a list of dictionaries
        """
        conn = self._get_connection()
        
        # Execute the query and fetch results
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        return df.to_dict(orient="records")
    
    def get_all_tables(self) -> List[str]:
        """
        Get a list of all tables in the database.
        
        Returns:
            List of table names
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        return tables
    
    def get_table_schema(self, table_name: str) -> Optional[str]:
        """
        Get the schema for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            SQL CREATE statement for the table or None if table doesn't exist
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
        result = cursor.fetchone()
        
        conn.close()
        return result[0] if result else None


# Example usage
if __name__ == "__main__":
    db = DatabaseManager("mydata.db")
    
    # Example: Import a CSV file to the database
    csv_file = "test_files/consumption_history.csv"
    if os.path.exists(csv_file):
        df = pd.read_csv(csv_file)
        
        # Ensure StartTime is properly formatted
        if "StartTime" in df.columns:
            # Convert to datetime if not already
            df["StartTime"] = pd.to_datetime(df["StartTime"])
            
        # Append to table
        db.append_dataframe(df, "example_table")
        
        # Example query
        results = db.execute_query("SELECT * FROM example_table LIMIT 5")
        print("\nSample data:")
        for row in results:
            print(row)
    else:
        print(f"Example file {csv_file} not found")
    
    # Show all tables
    tables = db.get_all_tables()
    print("\nAvailable tables:", tables)