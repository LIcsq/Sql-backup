import psycopg2


class PostgresBackupRestore:
    def __init__(self, host: str, user: str, password: str, db_name: str) -> None:
        self.host = host
        self.user = user
        self.password = password
        self.db_name = db_name

        # Connect to the default database to check if the target database exists
        self.conn = psycopg2.connect(host=host, user=user, password=password, dbname='postgres')
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        self.database_exists()
        # Check if the database exists, and create it if not
        self.create_database_if_not_exists()

        # Close the initial connection
        self.close_connection()

        # Connect to the target database
        self.conn = psycopg2.connect(host=host, user=user, password=password, dbname=db_name)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def restore(self, sql_commands):
        """
        Executes SQL commands to restore the database.

        Args:
            sql_commands (list): List of SQL commands to execute for restoring the database.
        """
        for command in sql_commands:
            self.cursor.execute(command)

    def close_connection(self):
        self.cursor.close()
        self.conn.close()

    def database_exists(self):
        self.cursor.execute(f"SELECT 1 FROM pg_database WHERE datname='{self.db_name}';")
        return self.cursor.fetchone() is not None

    def create_database_if_not_exists(self):
        if not self.database_exists():
            self.cursor.execute(f"CREATE DATABASE {self.db_name};")
            print(f"Database '{self.db_name}' created.")

    @staticmethod
    def read_sql_commands_from_file(file_path: str) -> list:
        """
        Reads SQL commands from a file.

        Args:
            file_path (str): Path to the SQL file.

        Returns:
            list: List of SQL commands.
        """
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read().split(';')[:-1]


def restore_data(host, user, password, db_name, file_path):
    restore_handler = PostgresBackupRestore(host, user, password, db_name)
    sql_commands = restore_handler.read_sql_commands_from_file(file_path)
    restore_handler.restore(sql_commands)
    restore_handler.close_connection()
