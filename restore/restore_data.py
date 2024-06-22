from restore.mysql_restore import MySQLBackupRestore
from restore.postgres_restore import PostgresBackupRestore


def restore_data(db_type: str, host: str, user: str, password: str, db_name: str, file_path: str) -> None:
    """
    Restores data from a SQL file into a MySQL or PostgreSQL database.

    Args:
        db_type (str): Type of the database. Supported values: 'mysql', 'postgres'.
        host (str): The host address of the database server.
        user (str): The username for accessing the database server.
        password (str): The password for accessing the database server.
        db_name (str): The name of the database to restore data into.
        file_path (str): Path to the SQL file containing the backup data.

    Raises:
        ValueError: If an unsupported database type is provided.
    """
    if db_type == 'mysql':
        restore_handler = MySQLBackupRestore(host, user, password, db_name)
    elif db_type == 'postgres':
        restore_handler = PostgresBackupRestore(host, user, password, db_name)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

    sql_commands = restore_handler.read_sql_commands_from_file(file_path)
    restore_handler.restore(sql_commands)
    restore_handler.close_connection()
