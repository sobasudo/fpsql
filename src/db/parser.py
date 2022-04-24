import json
import configparser
from pathlib import Path
from sqlalchemy import create_engine


def __create_engine_func(filepath, func):
    config_path = Path(filepath)
    if config_path.exists() and not config_path.is_dir():
        with config_path.open("r") as file:
            config = func(file)
            try:
                return create_engine(__gen_config_string(config), future=True, echo=config['echo'])
            except KeyError:
                return create_engine(__gen_config_string(config), future=True, echo=False)
    else:
        raise RuntimeError("Path to configuration file is incorrect")


def __gen_config_string(config):
    if config['dbapi'] == 'sqlite':
        try:
            if config['source'] == 'memory':
                return r"sqlite://"
        
            return f"sqlite:///{config['source']}.db"
        except KeyError:
            return r"sqlite://"

    return f"{config['dbapi']}://{config['username']}:{config['password']}@{config['server']}:{config['port']}/{config['database']}"


def __load_ini(file):
    config = configparser.ConfigParser()
    config.read_string(file.read())
    return config['database']


def __load_json(file):
    return json.load(file)


def json_engine(filepath):
    return __create_engine_func(filepath, __load_json)


def config_engine(filepath):
    return __create_engine_func(filepath, __load_ini)
