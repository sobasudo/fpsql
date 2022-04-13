from parser import json_engine, config_engine
from sqlalchemy import MetaData


class Database(object):
    engine = None

    def __init__(self, json_config=None, config=None):
        if json_config:
            Database.engine = json_engine(json_config)
        elif config:
            Database.engine = config_engine(config)
        else:
            raise RuntimeError("Database not configured")


class Schema(object):

    def __init__(self):
        

    
