from enum import Enum
from sqlite3 import IntegrityError
from tkinter import INSERT
from src.db.parser import json_engine, config_engine
from sqlalchemy import Column, MetaData, Integer, Table, inspect, ForeignKey
import sqlalchemy
from src.utils.linear_collections import Queue


class Database(object):
    def __init__(self, json_config=None, config=None, test=False):
        self.test = test
        if json_config:
            self.engine = json_engine(json_config)
        elif config:
            self.engine = config_engine(config)
        else:
            raise RuntimeError("Database not configured")

        self.__metadata = MetaData(bind=self.engine)
        self.__temp = dict()
        self.__relations = dict()

        table_names = inspect(self.engine).get_table_names()
        if len(table_names) > 0:
            for name in table_names:
                self.__relations[name] = Table(name, self.__metadata, autoload=True)

    def __getitem__(self, key):
        try:
            if key in self.__relations.keys():
                return self.__relations[key]
            else:
                return self.__temp[key]
        except:
            raise KeyError(
                f"Table '{key}' is not defined. Call create_table to add this relation.")

    def create_table(self, name, ondelete="NO ACTION", onupdate="NO ACTION"):
        if not name:
            raise TypeError("Table must have a name!")
        if name in self.__temp.keys() or name in self.__relations.keys():
            raise RuntimeError(f"Table names must be unique. {name} exists!")
        self.__temp[name] = LazyTable(self.engine, ondelete, onupdate)
        return self.__temp[name]

    def try_create(self):
        for name, table in self.__temp.items():
            if table.evaluated:
                continue
            table.consolidate(self.__metadata, name)
            self.__relations[name] = table.get_table()

        self.__temp.clear()
        self.__metadata.create_all()

    def clean_up(self):
        if self.test:
            self.__metadata.drop_all()


class LazyTable(object):

    def __init__(self, engine, ondelete, onupdate, table=None, evaluated=False):
        self.columns = Queue()
        self.evaluated = evaluated
        self.engine = engine
        self.table = table
        self.ondelete = ondelete
        self.onupdate = onupdate

    def col(self, name, data_type, primary_key=False, nullable=True, unique=False, references=None):
        if references:
            self.columns.enqueue(
                Column(name, ForeignKey(references, ondelete=self.ondelete, onupdate=self.onupdate), nullable=False))
        else:
            self.columns.enqueue(Column(name, getattr(
                sqlalchemy, data_type)(), nullable=nullable, unique=unique, primary_key=primary_key))
        return self

    def is_committed(self):
        return self.evaluated

    def consolidate(self, metadata, name):
        if self.columns.size() == 0:
            raise RuntimeError(f"Can't create table with no columns. Columns {self.columns}")
        if not self.evaluated:
            self.evaluated = True
            self.table = Table(name, metadata, *[column for column in self.columns.gen_iter()])
            del self.columns
        else:
            return

    def get_table(self):
        if self.evaluated:
            return self.table
        else:
            self.throwUndefinedStateException()

    def throwUndefinedStateException(self):
        raise RuntimeError(
            "Relation undefined and/or staged. Call consolidate to try commit relation to database.")
