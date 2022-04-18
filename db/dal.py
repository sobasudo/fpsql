from db.parser import json_engine, config_engine
from sqlalchemy import Column, MetaData, Table, insert
import sqlalchemy

from utils.linear_collections import Queue


class Database(object):
    def __init__(self, json_config=None, config=None):
        if json_config:
            self.engine = json_engine(json_config)
        elif config:
            self.engine = config_engine(config)
        else:
            raise RuntimeError("Database not configured")
        
        self.__metadata = MetaData()
        self.__relations = dict()

    def create_schema(self, name):
        self.create_table(name)

    def __getitem__(self, key):
        return self.__relations[key]

    def create_table(self, name):
        if not name:
            raise TypeError("Table must have a name!")
        if name in self.__relations.keys():
            raise RuntimeError(f"Table names must be unique. {name} exists!")
        self.__relations[name] = LazyTable()
        return self.__relations[name]

    def try_create(self):
        for name, table in self.__relations.items():
            if not table.evaluated:
                table.evaluated = True
            self.__relations[name] = Table(name, self.__metadata, *table.get_cols())
        self.__metadata.create_all(self.engine)

    def from_dict(self, data):
        with self.engine.connect() as conn:
            conn.execute(insert(self))


class LazyTable(object):

    def __init__(self):
        self.columns = Queue()
        self.evaluated = False

    def col(self, name, data_type, primary_key=False, nullable=False, unique=False, constraints=None):
        self.columns.enqueue(Column(name, getattr(
            sqlalchemy, data_type)(), primary_key=primary_key))
        return self

    def get_cols(self):
        return [column for column in self.columns.gen_iter()]

    def is_committed(self):
        return self.evaluated

    
