import json
import configparser
from pathlib import Path
from sqlalchemy import create_engine


def create_engine_func(filepath, func):
    config_path = Path(filepath)
    if config_path.exists() and not config_path.is_dir():
        with config_path.open("r") as file:
            config = func(file)
            try:
                return create_engine(gen_config_string(config), future=True, echo=config['echo'])
            except KeyError:
                return create_engine(gen_config_string(config), future=True, echo=False)
    else:
        raise RuntimeError("Path to configuration file is incorrect")


def gen_config_string(config):
    if config['dbapi'] == 'sqlite':
        return r"sqlite://"

    return f"{config['dbapi']}://{config['username']}:{config['password']}@{config['server']}:{config['port']}/{config['database']}"


def load_ini(file):
    config = configparser.ConfigParser()
    config.read_string(file.read())
    return config['database']


def load_json(file):
    return json.load(file)


def json_engine(filepath):
    return create_engine_func(filepath, load_json)


def config_engine(filepath):
    return create_engine_func(filepath, load_ini)
