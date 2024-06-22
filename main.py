import argparse
from restore.restore_data import restore_data
from configuration_files.config import read_config, list_config_files
from backup.backup_manager import BackupManager
from configuration_files.exceptions import ConfigError, BackupManagerError


def main() -> None:
    parser = argparse.ArgumentParser(description='Database backup and restore utility')

    config_group = parser.add_argument_group('Configuration')
    config_group.add_argument('--config', help='Path to the configuration file')
    config_group.add_argument('-l', '--list-configs', action='store_true',
                              help='List all configuration files *.cfg')

    db_group = parser.add_argument_group('Database Connection')
    db_group.add_argument('--db_type', choices=['mysql', 'postgres'], help='Database type')
    db_group.add_argument('--host', help='Database host')
    db_group.add_argument('--user', help='Database user')
    db_group.add_argument('--password', help='Database password')
    db_group.add_argument('--db_name', help='Database name')

    backup_group = parser.add_argument_group('Backup')
    backup_group.add_argument('--backup', choices=['structure', 'data', 'structure_data'],
                              help='Type of backup')
    backup_group.add_argument('-s', '-save', action='store_true',
                              help='Do backup in multiple files <name_table>.DDL,'
                                   ' <name_table>.DML, <name_table>.DCL')
    backup_group.add_argument('-o', '--output_file', help='Do backup in a single file')
    backup_group.add_argument('-t', '--tables', nargs='+', help='List of tables to backup')
    backup_group.add_argument('-v', '--version', help='Versioning of database')

    restore_group = parser.add_argument_group('Restore')
    restore_group.add_argument('--restore', help='Name of the database to restore')
    restore_group.add_argument('-f', '--file_data', help='Path to the backup data file')

    args = parser.parse_args()

    if args.list_configs:
        list_config_files()
        return

    try:
        if args.config:
            config = read_config(args.config)
            args.host = config['host']
            args.user = config['user']
            args.password = config['password']
            args.db_name = config['db_name']
            args.db_type = config['db_type']

        if args.restore:
            restore_data(args.db_type, args.host, args.user, args.password, args.restore, args.file_data)

        if args.backup:
            backup_manager = BackupManager(args.db_type, args.host, args.user, args.password, args.db_name)
            backup_data = backup_manager.backup(args.backup, args.tables, args.s)

            if args.output_file:
                backup_manager.save_backup_data(backup_data, args.output_file, args.version, args.db_type)
            elif args.s:
                backup_manager.save_multiple_files(backup_data)
            else:
                print(backup_data)
    except ConfigError as e:
        print(f"Configuration error: {str(e)}")
    except BackupManagerError as e:
        print(f"Backup error: {str(e)}")


if __name__ == '__main__':
    main()
