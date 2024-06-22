# Sql-backup in Python with only sql queris

# Data Backup Utility

## Overview
Develop a utility for performing backups and restorations of MySQL and PostgreSQL databases. The utility should feature a CLI interface for backup and restoration commands and support versioning.

## Functionality

1. **SQL Usage:**
   - The utility will utilize SQL commands for operations.

2. **Backup Features:**
   - Backup database structure.
   - Backup data.
   - Backup both database structure and data.
   - Backup specific tables.
   - Store all backups in a single file.
   - Store backups in separate files:
     - `<table_name>.DDL` for structure
     - `<table_name>.DML` for data
     - `<table_name>.DCL` for permissions

3. **Restoration Features:**
   - Restore database structure.
   - Restore data.
   - Restore specific tables.

4. **Additional Features:**
   - Versioning of backups.
   - *Incremental backup creation.
   - **Decremental backup creation.
   - Indication of backup process progress.

## Requirements

- **Implementation:** The utility should be implemented in Python.
- **Unit Tests:** Implement unit tests for the utility.

## References

1. [SQL DDL, DQL, DML, DCL and TCL Commands](https://www.geeksforgeeks.org/sql-ddl-dql-dml-dcl-tcl-commands/#dcl-data-control-language)

---

*Note: Features marked with an asterisk (*) are optional enhancements.*
