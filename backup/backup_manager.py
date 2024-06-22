import os
from datetime import datetime
from backup.mysql_backup import MySQLBackup
from backup.postgres_backup import PostgresBackup
from configuration_files.exceptions import ConfigError, BackupManagerError, BackupError
from typing import List, Dict, Optional


class BackupManager:
    def __init__(self, db_type: str, host: str, user: str, password: str, db_name: str) -> None:
        """
        Initializes the BackupManager instance with the appropriate backup instance based on the database type.

        Args:
            db_type (str): Type of the database. Supported values: 'mysql', 'postgres'.
            host (str): The host address of the database server.
            user (str): The username for accessing the database server.
            password (str): The password for accessing the database server.
            db_name (str): The name of the database to back up.
        """
        self.db_type = db_type
        try:
            if db_type == 'mysql':
                self.backup_instance = MySQLBackup(host, user, password, db_name)
            elif db_type == 'postgres':
                self.backup_instance = PostgresBackup(host, user, password, db_name)
        except ValueError as e:
            raise ConfigError(f"Invalid database type provided: {db_type}") from e

    def backup(self, backup_type: str, tables: Optional[List[str]], include_permissions: bool = False) -> str:
        """
        Perform the backup for the specified tables and backup type.

        Args:
            backup_type (str): Type of backup to perform. Options: 'structure', 'data', 'structure_data'.
            tables (Optional[List[str]]): List of tables to include in the backup. If None, all tables are included.
            include_permissions (bool): Whether to include table permissions in the backup.

        Returns:
            str: The SQL backup script.

        Raises:
            BackupError: If there is an error during the backup process.
        """
        try:
            if self.backup_instance:
                return self.backup_instance.backup(backup_type, tables, include_permissions)
            else:
                return self.backup_instance.backup(backup_type, tables, include_permissions)
        except BackupManagerError as e:
            raise BackupError(f"Failed to perform backup: {str(e)}") from e

    @staticmethod
    def save_backup_data(backup_data: str, output_file: Optional[str] = None, version: Optional[str] = None,
                         db_type: Optional[str] = None) -> None:
        """
        Save the backup data to a file or print it to the console.

        Args:
            backup_data (str): The SQL backup script to save.
            output_file (Optional[str]): The name of the output file. If None, print the backup data.
            version (Optional[str]): The version to append to the file name.
            db_type (Optional[str]): The type of the database. Supported values: 'mysql', 'postgres'.

        Raises:
            BackupError: If there is a permission error while saving the file.
        """
        if output_file:
            if version:
                current_datetime = datetime.now().strftime('%Y-%m-%d_%H-%M')
                new_filename = f"{current_datetime}_v{version}_{output_file}"
            else:
                new_filename = output_file
            try:
                if db_type == 'mysql':
                    with open(os.path.join('./single_backups', new_filename), 'w', encoding='utf-8') as f:
                        backup_data_str = ''.join(''.join(item) for item in backup_data.values())
                        f.write(backup_data_str)
                elif db_type == 'postgres':
                    with open(os.path.join('./single_backups', new_filename), 'w', encoding='utf-8') as f:
                        f.write(backup_data)
            except PermissionError as e:
                raise BackupError("Permission denied") from e
        else:
            print(''.join(''.join(item) for item in backup_data.values()))

    @staticmethod
    def save_multiple_files(backup_data: Dict[str, List[str]]) -> None:
        """
        Save the backup data for multiple tables into separate files.

        Args:
            backup_data (Dict[str, List[str]]): The backup data for each table, categorized by content type.
        """
        os.makedirs('./multiple_backups', exist_ok=True)
        for table, data in backup_data.items():
            for content in data:
                if 'Permissions' in content:
                    filename = f"{table}.permissions.dpl"
                elif 'Table structure' in content:
                    filename = f"{table}.structure.ddl"
                elif 'Data for table' in content:
                    filename = f"{table}.data.dml"
                else:
                    continue
                with open(os.path.join('./multiple_backups', filename), 'w', encoding='utf-8') as f:
                    f.write(content)
