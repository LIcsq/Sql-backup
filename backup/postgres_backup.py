import psycopg2
from collections import defaultdict
from datetime import datetime, date
from tqdm import tqdm
from typing import List, Dict, Optional


def create_foreign_key_statement(foreign_keys: List[tuple]) -> str:
    """Create SQL statement for adding foreign keys."""
    fk_statements = []
    for fk in foreign_keys:
        constraint_name, table_name, column_name, foreign_table_name, foreign_column_name = fk
        fk_stmt = (f"ALTER TABLE ONLY public.{table_name} "
                   f"ADD CONSTRAINT {constraint_name} "
                   f"FOREIGN KEY ({column_name}) REFERENCES public.{foreign_table_name}({foreign_column_name}) "
                   f"ON UPDATE CASCADE ON DELETE RESTRICT;")
        fk_statements.append(fk_stmt)
    return "\n".join(fk_statements)


def create_permission_statements(table: str, permissions: List[tuple]) -> str:
    """Create SQL statements for table permissions."""
    permission_statements = []
    for grantee, privilege_type in permissions:
        permission_stmt = f"GRANT {privilege_type} ON TABLE public.{table} TO {grantee};"
        permission_statements.append(permission_stmt)
    return "\n".join(permission_statements)


def format_value(value) -> str:
    """Format a value for inclusion in SQL statements."""
    if value is None:
        return 'NULL'
    elif isinstance(value, (datetime, date)):
        return f"'{value.strftime('%Y-%m-%d')}'" if isinstance(value, date)\
            else f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
    elif isinstance(value, list):
        return "'{" + ','.join(value) + "}'"
    elif isinstance(value, str):
        return f"'{value.replace('\'', '\'\'')}'"
    elif isinstance(value, memoryview):
        return "'<memory>'"
    else:
        return str(value)


def create_insert_statement(table: str, columns: List[tuple], rows: List[tuple]) -> str:
    """Create SQL insert statements for table data."""
    col_names = [col[0] for col in columns]
    values_list = []
    for row in rows:
        formatted_values = ', '.join([format_value(value) for value in row])
        values_list.append(f"({formatted_values})")
    values_str = ',\n'.join(values_list)
    insert_statement = f"INSERT INTO public.{table} ({', '.join(col_names)}) VALUES \n{values_str};"
    return insert_statement


def create_type_statement(type_name: str, labels: List[str]) -> str:
    """Create SQL statement for recreating a user-defined type."""
    drop_type_stmt = f"DROP TYPE IF EXISTS public.{type_name} CASCADE;\n"
    create_type_stmt = f"CREATE TYPE public.{type_name} AS ENUM (\n"
    create_type_stmt += ",\n".join([f"    '{label}'" for label in labels])
    create_type_stmt += "\n);\n"
    return drop_type_stmt + create_type_stmt


class PostgresBackup:
    def __init__(self, host: str, user: str, password: str, db_name: str):
        """
        Initialize the database connection.

        Args:
            host (str): The host address of the PostgreSQL server.
            user (str): The username for accessing the PostgreSQL server.
            password (str): The password for accessing the PostgreSQL server.
            db_name (str): The name of the database to back up.
        """
        self.host = host
        self.user = user
        self.password = password
        self.db_name = db_name
        self.conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, dbname=self.db_name)
        self.cursor = self.conn.cursor()

    def get_table_definitions(self, table: str) -> List[tuple]:
        """Retrieve table definitions (column names, data types, etc.)."""
        self.cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position;
        """, (table,))
        return self.cursor.fetchall()

    def get_foreign_keys(self, table: str) -> List[tuple]:
        """Retrieve foreign keys for a given table."""
        self.cursor.execute("""
            SELECT tc.constraint_name, tc.table_name, kcu.column_name, 
                   ccu.table_name AS foreign_table_name,
                   ccu.column_name AS foreign_column_name 
            FROM information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
            WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name = %s;
        """, (table,))
        return self.cursor.fetchall()

    def get_primary_keys(self, table: str) -> List[str]:
        """Retrieve primary keys for a given table."""
        self.cursor.execute("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tco
            JOIN information_schema.key_column_usage kcu 
                ON kcu.constraint_name = tco.constraint_name
            WHERE tco.constraint_type = 'PRIMARY KEY'
              AND kcu.table_name = %s
            ORDER BY kcu.ordinal_position;
        """, (table,))
        return [row[0] for row in self.cursor.fetchall()]

    def get_sequences(self) -> List[str]:
        """Retrieve all sequences in the public schema."""
        self.cursor.execute("""
            SELECT sequence_name
            FROM information_schema.sequences
            WHERE sequence_schema = 'public';
        """)
        return [seq[0] for seq in self.cursor.fetchall()]

    def create_sequence_statement(self, sequence: str) -> str:
        """Create SQL statement for recreating a sequence."""
        self.cursor.execute(f"""
            SELECT start_value, increment_by, min_value, max_value, cache_size
            FROM pg_sequences
            WHERE schemaname = 'public' AND sequencename = %s;
        """, (sequence,))
        seq_info = self.cursor.fetchone()
        start_value, increment_by, min_value, max_value, cache_size = seq_info

        drop_sequence_stmt = f"DROP SEQUENCE IF EXISTS public.{sequence} CASCADE;\n"
        create_sequence_stmt = f"CREATE SEQUENCE public.{sequence}\n"
        create_sequence_stmt += f"    START WITH {start_value}\n"
        create_sequence_stmt += f"    INCREMENT BY {increment_by}\n"
        create_sequence_stmt += "    NO MINVALUE\n" if min_value is None else f"    MINVALUE {min_value}\n"
        create_sequence_stmt += "    NO MAXVALUE\n" if max_value is None else f"    MAXVALUE {max_value}\n"
        create_sequence_stmt += f"    CACHE {cache_size};\n"
        return drop_sequence_stmt + create_sequence_stmt

    def create_table_statement(self, table: str, columns: List[tuple]) -> str:
        create_table_stmt = f"DROP TABLE IF EXISTS public.{table} CASCADE;\n"
        create_table_stmt += f"CREATE TABLE public.{table} (\n"
        col_defs = []
        primary_keys = self.get_primary_keys(table)
        for col in columns:
            col_name, data_type, is_nullable, col_default = col
            if data_type == 'USER-DEFINED':
                data_type = "public.mpaa_rating"
                col_default = "'G'::public.mpaa_rating"
            elif 'ARRAY' in data_type:
                data_type = "text[]"
            elif data_type == 'character':
                data_type = "character(100)"
            col_def = f"\t{col_name} {data_type}"
            if col_default:
                if 'nextval(' in col_default:
                    col_default = col_default.replace('nextval(\'', 'nextval(\'public.')
                col_def += f" DEFAULT {col_default}"
            if is_nullable == 'NO':
                col_def += " NOT NULL"
            col_defs.append(col_def)
        if primary_keys:
            col_defs.append(f"\tPRIMARY KEY ({', '.join(primary_keys)})")
        create_table_stmt += ",\n".join(col_defs) + "\n);\n"
        return create_table_stmt

    def get_user_defined_types(self) -> Dict[str, List[str]]:
        """Retrieve user-defined types and their attributes."""
        self.cursor.execute("""
            SELECT n.nspname AS schema, t.typname AS type_name, e.enumlabel AS enum_label
            FROM pg_type t
            JOIN pg_enum e ON t.oid = e.enumtypid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
            WHERE n.nspname = 'public'
            ORDER BY t.typname, e.enumsortorder;
        """)
        types = defaultdict(list)
        for row in self.cursor.fetchall():
            schema, type_name, enum_label = row
            types[type_name].append(enum_label)
        return types

    def get_table_permissions(self, table: str) -> List[tuple]:
        """Retrieve table permissions for a given table."""
        self.cursor.execute("""
              SELECT grantee, privilege_type
              FROM information_schema.role_table_grants
              WHERE table_name = %s;
          """, (table,))
        return self.cursor.fetchall()

    def backup(self, backup_type: str, tables: Optional[List[str]] = None,
               include_permissions: bool = False) -> str:
        """
        Perform the backup of the specified tables.

        Args:
            backup_type (str): Type of backup to perform. Options: 'structure', 'data', 'structure_data'.
            tables (Optional[List[str]]): List of tables to include in the backup. If None, all tables are included.
            include_permissions (bool): Whether to include table permissions in the backup.

        Returns:
            str: The SQL backup script.
        """
        # SQL setup script
        backup_sql = """\
SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;
"""

        # Retrieve list of tables to backup
        if tables is None:
            self.cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='public' AND table_type='BASE TABLE'
            """)
            all_tables = [table[0] for table in self.cursor.fetchall()]
            include_sequences_and_types = True
        else:
            all_tables = tables
            include_sequences_and_types = False

        # Total steps for progress bar
        total_steps = len(all_tables) + len(all_tables)
        if include_sequences_and_types:
            total_steps += (len(all_tables) + len(self.get_sequences()) +
                            len(self.get_user_defined_types().items()))

        # Create a progress bar
        pbar = tqdm(total=total_steps, desc="Backing up database")

        # Dictionaries to hold table definitions and dependencies
        table_definitions = {}
        dependency_map = defaultdict(list)

        # Get table definitions and dependencies
        for table in all_tables:
            columns = self.get_table_definitions(table)
            table_definitions[table] = columns

            # Get foreign keys for the table
            foreign_keys = self.get_foreign_keys(table)
            for fk in foreign_keys:
                dependency_map[table].append(fk[3])  # Add tables referenced by foreign key

            pbar.update(1)  # Update progress bar

        # Retrieve sequences and user-defined types if including all tables
        if include_sequences_and_types:
            sequences = self.get_sequences()
            user_defined_types = self.get_user_defined_types()
            pbar.update(len(sequences))  # Update progress bar
            pbar.update(len(user_defined_types))  # Update progress bar
        else:
            sequences = []
            user_defined_types = {}

        # Order of table creation (simply based on their retrieval order)
        sorted_tables = all_tables

        foreign_keys_to_add = []

        # Generate SQL for creating user-defined types if backing up structure and all tables
        if backup_type in ['structure', 'structure_data'] and include_sequences_and_types:
            for type_name, labels in user_defined_types.items():
                backup_sql += create_type_statement(type_name, labels) + "\n"
                pbar.update(1)  # Update progress bar

        # Generate SQL for creating sequences if backing up structure and all tables
        if backup_type in ['structure', 'structure_data'] and include_sequences_and_types:
            for sequence in sequences:
                backup_sql += self.create_sequence_statement(sequence) + "\n"
                pbar.update(1)  # Update progress bar

        # Generate SQL for creating tables if backing up structure
        if backup_type in ['structure', 'structure_data']:
            for table in sorted_tables:
                columns = table_definitions[table]
                backup_sql += self.create_table_statement(table, columns) + "\n"

                # Add foreign keys after table creation
                foreign_keys = self.get_foreign_keys(table)
                foreign_keys_to_add.append((table, foreign_keys))

                pbar.update(1)  # Update progress bar

        # Generate SQL for table data if backing up data
        if backup_type in ['data', 'structure_data']:
            for table in sorted_tables:
                self.cursor.execute(f"SELECT * FROM {table};")
                rows = self.cursor.fetchall()

                self.cursor.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position;
                """, (table,))
                columns = self.cursor.fetchall()

                if rows:
                    backup_sql += create_insert_statement(table, columns, rows) + "\n"

                pbar.update(1)  # Update progress bar

        # Generate SQL for adding foreign keys if backing up structure
        if backup_type in ['structure', 'structure_data']:
            for table, foreign_keys in foreign_keys_to_add:
                if foreign_keys:
                    backup_sql += create_foreign_key_statement(foreign_keys) + "\n"
                pbar.update(1)  # Update progress bar

        if include_permissions:
            for table in sorted_tables:
                permissions = self.get_table_permissions(table)
                if permissions:
                    backup_sql += create_permission_statements(table, permissions) + "\n"
        # Close the cursor and connection
        self.cursor.close()
        self.conn.close()

        pbar.close()  # Close progress bar

        return backup_sql
