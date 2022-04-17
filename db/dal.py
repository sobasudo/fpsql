from db.parser import json_engine, config_engine
from sqlalchemy import Column, MetaData
import sqlalchemy


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
        self.metadata = MetaData()
        self.relations = dict()

    def create_table(self, name):
        if not name:
            raise RuntimeError("Table must have a name!")
        if name in self.relations.keys():
            raise RuntimeError(f"Table names must be unique. {name} exists!")
        self.relations[name] = LazyTable()
        return self.relations[name]

    def try_create(self):
        pass


class LazyTable(object):

    def __init__(self):
        self.columns = list()
        self.evaluated = False

    def col(self, name, data_type, primary_key=False, nullable=False, unique=False, constraints=None):
        self.columns.append(Column(name, getattr(
            sqlalchemy, data_type)(), primary_key=primary_key))
        return self
