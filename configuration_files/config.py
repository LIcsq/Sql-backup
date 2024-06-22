import configparser
import glob
from configuration_files.exceptions import ConfigError


def read_config(config_path: str) -> dict:
    """
    Read the configuration file and return a dictionary with database connection parameters.
    """
    try:
        config = configparser.ConfigParser()
        config.read(config_path)
        db_type = config.sections()[0]
        return {
            'host': config.get(db_type, 'host'),
            'user': config.get(db_type, 'user'),
            'password': config.get(db_type, 'password'),
            'db_name': config.get(db_type, 'db_name'),
            'db_type': db_type,
        }
    except configparser.Error as e:
        raise ConfigError(f"Error reading config file: {config_path}") from e
    except IndexError as e:
        raise ConfigError(f"No sections found in config file: {config_path}") from e


def list_config_files() -> None:
    """
    List all configuration files with the .cfg extension in the current directory.
    """
    config_files = glob.glob('*.cfg')
    for config_file in config_files:
        print(config_file)
