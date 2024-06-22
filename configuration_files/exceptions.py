# exceptions.py

class BackupManagerError(Exception):
    """Base class for exceptions in this module."""
    pass

class DatabaseConnectionError(BackupManagerError):
    """Exception raised for errors in the database connection."""
    def __init__(self, host, user):
        self.host = host
        self.user = user
        self.message = f"Failed to connect to the database at {host} with user {user}."
        super().__init__(self.message)

class BackupError(BackupManagerError):
    """Exception raised for errors during the backup process."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class RestoreError(BackupManagerError):
    """Exception raised for errors during the restore process."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class ConfigError(BackupManagerError):
    """Exception raised for errors in the configuration file."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)
