from db.parser import json_engine, config_engine
from sqlalchemy import Column, MetaData, Table, insert, select
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
        self.relations = dict()

    def __getitem__(self, key):
        return self.relations[key]

    def create_table(self, name):
        if not name:
            raise TypeError("Table must have a name!")
        if name in self.relations.keys():
            raise RuntimeError(f"Table names must be unique. {name} exists!")
        self.relations[name] = LazyTable(self.engine)
        return self.relations[name]

    def try_create(self):
        for name, table in self.relations.items():
            table.consolidate(self.__metadata, name)
        self.__metadata.create_all(self.engine)            

class LazyTable(object):

    def __init__(self, engine):
        self.columns = Queue()
        self.evaluated = False
        self.engine = engine
        self.table = None

    def col(self, name, data_type, primary_key=False, nullable=True, unique=False, constraints=None):
        self.columns.enqueue(Column(name, getattr(
            sqlalchemy, data_type)(), primary_key=primary_key))
        return self

    def get_cols(self):
        return [column for column in self.columns.gen_iter()]

    def is_committed(self):
        return self.evaluated

    def consolidate(self, metadata, name):
        if not self.evaluated:
                self.evaluated = True
                self.table = Table(name, metadata, *self.get_cols())
        else:
            return
    
    def get_table(self):
        if self.evaluated:
            return self.table
        else:
            self.throwUndefinedStateException()
            
    def throwUndefinedStateException(self):
        raise RuntimeError("Relation undefined and/or staged. Call consolidate to try commit relation to database.")

    def insert(self, data):
        if not self.evaluated:
            self.throwUndefinedStateException()
        with self.engine.connect() as conn:
            conn.execute(insert(self.table), data)
            conn.commit()

    def fetch_all(self):
        if not self.evaluated:
            self.throwUndefinedStateException()
        with self.engine.connect() as conn:
            result = conn.execute(select(self.table))
        return [row._asdict() for row in result]
    
