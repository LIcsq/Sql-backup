import pymysql
from tqdm import tqdm
from datetime import datetime
from configuration_files.exceptions import DatabaseConnectionError, BackupError
from typing import List, Dict, Optional


def _generate_backup_footer() -> str:
    return """
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
"""


def _format_value(value) -> str:
    if value is None:
        return 'NULL'
    elif isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    elif isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    elif isinstance(value, datetime):
        return "'" + value.strftime('%Y-%m-%d %H:%M:%S') + "'"
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, bytes):
        return "0x" + value.hex()
    else:
        return "'" + str(value).replace("'", "''") + "'"


def _generate_backup_header() -> str:
    return """
/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;\n
"""


class MySQLBackup:
    def __init__(self, host: str, user: str, password: str, db_name: str):
        """
        Initializes the MySQLBackup instance.

        Args:
            host (str): The host address of the MySQL server.
            user (str): The username for accessing the MySQL server.
            password (str): The password for accessing the MySQL server.
            db_name (str): The name of the database to back up.

        Raises:
            DatabaseConnectionError: If there's an error connecting to the database.
        """
        self.host = host
        self.user = user
        self.password = password
        self.db_name = db_name
        try:
            self.conn = pymysql.connect(host=host, user=user, password=password, database=db_name)
        except pymysql.MySQLError as e:
            raise DatabaseConnectionError(host, user) from e
        self.cursor = self.conn.cursor()

    def backup(self, backup_type: str, tables: Optional[List[str]] = None,
               include_permissions: bool = False) -> Dict[str, List[str]]:
        """
        Perform the backup for the specified tables and backup type.

        Args:
            backup_type (str): Type of backup to perform. Options: 'structure', 'data', 'structure_data'.
            tables (Optional[List[str]]): List of tables to include in the backup. If None, all tables are included.
            include_permissions (bool): Whether to include table permissions in the backup.

        Returns:
            Dict[str, List[str]]: The backup data organized by table name.

        Raises:
            BackupError: If there's an error during the backup process.
        """
        try:
            backup_data = {}
            header_written = False

            if tables is None:
                self.cursor.execute(
                    f"SELECT table_name, table_type FROM information_schema.tables "
                    f"WHERE table_schema='{self.db_name}'")
                all_tables = self.cursor.fetchall()
            else:
                self.cursor.execute(
                    f"SELECT table_name, table_type FROM information_schema.tables"
                    f" WHERE table_schema='{self.db_name}' AND table_name "
                    f"IN ({','.join(['%s'] * len(tables))})",
                    tables)
                all_tables = self.cursor.fetchall()

            for table, table_type in tqdm(all_tables, desc="Backing up tables and views", unit="object"):
                table_backup_data = []

                if not header_written:
                    table_backup_data.append(_generate_backup_header())
                    header_written = True

                if backup_type == 'structure' or backup_type == 'structure_data':
                    self.cursor.execute(f"SHOW CREATE TABLE `{table}`")
                    create_table_statement = self.cursor.fetchone()[1]
                    if table_type == 'VIEW':
                        table_backup_data.append(
                            f"-- Structure for view `{table}`\nDROP VIEW IF EXISTS `"
                            f"{table}`;\n{create_table_statement};\n\n")
                    else:
                        table_backup_data.append(
                            f"-- Table structure for table `{table}`\nDROP TABLE IF EXISTS `"
                            f"{table}`;\n{create_table_statement};\n\n")

                if table_type == 'BASE TABLE' and (backup_type == 'data' or backup_type == 'structure_data'):
                    self.cursor.execute(f"SELECT * FROM `{table}`")
                    rows = self.cursor.fetchall()
                    if rows:
                        column_names = [desc[0] for desc in self.cursor.description]
                        insert_statement_prefix = (f"INSERT INTO `{table}` "
                                                   f"(`{'`, `'.join(column_names)}`) VALUES ")
                        values_list = []
                        for row in rows:
                            formatted_values = tuple(_format_value(value) for value in row)
                            values_list.append(f"({', '.join(formatted_values)})")
                        insert_statement = insert_statement_prefix + ',\n'.join(values_list) + ';\n'
                        lock_tables = f"LOCK TABLES `{table}` WRITE;\n"
                        unlock_tables = f"UNLOCK TABLES;\n"
                        table_backup_data.append(
                            f"-- Data for table `{table}`\n" + lock_tables + insert_statement + unlock_tables + '\n\n')

                backup_data[table] = table_backup_data

            if include_permissions is True:
                self.cursor.execute(f"SHOW GRANTS FOR CURRENT_USER")
                grants = self.cursor.fetchall()
                grant_statements = "\n".join([grant[0] for grant in grants])
                backup_data['permissions'] = [f"-- Permissions\n{grant_statements}\n\n"]

            if header_written:
                backup_data['footer'] = [_generate_backup_footer()]

            self._close_connection()
            return backup_data
        except pymysql.MySQLError as e:
            raise BackupError(f"Error during backup: {str(e)}") from e

    def _close_connection(self) -> None:
        """
        Close the database connection.
        """
        self.cursor.close()
        self.conn.close()
