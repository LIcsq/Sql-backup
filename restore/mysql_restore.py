import pymysql
from configuration_files.exceptions import RestoreError, DatabaseConnectionError
from typing import List


class MySQLBackupRestore:
    def __init__(self, host: str, user: str, password: str, db_name: str) -> None:
        """
        Initializes a MySQLBackupRestore object.

        Args:
            host (str): The host address of the MySQL server.
            user (str): The username for accessing the MySQL server.
            password (str): The password for accessing the MySQL server.
            db_name (str): The name of the database to be restored.
        """
        self.host = host
        self.user = user
        self.password = password
        self.db_name = db_name
        try:
            self.conn = pymysql.connect(host=host, user=user, password=password)
        except pymysql.MySQLError as e:
            raise DatabaseConnectionError(host, user) from e
        self.cursor = self.conn.cursor()
        self.create_database_if_not_exists()
        self.conn.select_db(db_name)

    def restore(self, sql_commands: List[str]) -> None:
        """
        Executes SQL commands to restore the database.

        Args:
            sql_commands (List[str]): List of SQL commands to execute for restoring the database.
        """
        try:
            for command in sql_commands:
                if command.strip():  # Ensure the command is not empty
                    self.cursor.execute(command)
        except pymysql.MySQLError as e:
            raise RestoreError(f"An error occurred while restoring the database: {str(e)}") from e

    def close_connection(self) -> None:
        """
        Commits changes and closes the database connection.
        """
        self.conn.commit()
        self.cursor.close()
        self.conn.close()

    def database_exists(self) -> bool:
        """
        Checks if the database already exists.

        Returns:
            bool: True if the database exists, False otherwise.
        """
        self.cursor.execute(f"SHOW DATABASES LIKE '{self.db_name}';")
        return self.cursor.fetchone() is not None

    def create_database_if_not_exists(self) -> None:
        """
        Creates the database if it does not exist.
        """
        if not self.database_exists():
            self.cursor.execute(f"CREATE DATABASE {self.db_name};")
            print(f"Database '{self.db_name}' created.")

    @staticmethod
    def read_sql_commands_from_file(file_path: str) -> List[str]:
        """
        Reads SQL commands from a file.

        Args:
            file_path (str): Path to the SQL file.

        Returns:
            List[str]: List of SQL commands.
        """
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read().split(';')[:-1]


def restore_data(host: str, user: str, password: str, db_name: str, file_path: str) -> None:
    """
    Restores data from a SQL file.

    Args:
        host (str): The host address of the MySQL server.
        user (str): The username for accessing the MySQL server.
        password (str): The password for accessing the MySQL server.
        db_name (str): The name of the database to restore data into.
        file_path (str): Path to the SQL file containing the backup data.
    """
    restore_handler = MySQLBackupRestore(host, user, password, db_name)
    sql_commands = restore_handler.read_sql_commands_from_file(file_path)
    restore_handler.restore(sql_commands)
    restore_handler.close_connection()
