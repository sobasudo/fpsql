from enum import Enum
from sqlite3 import IntegrityError
from tkinter import INSERT
from db.parser import json_engine, config_engine
from sqlalchemy import Column, MetaData, Integer, Table, delete, insert, select, inspect, ForeignKey, or_, update
import sqlalchemy
from utils.linear_collections import Queue


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
        self.__relations = dict()

        table_names = inspect(self.engine).get_table_names()
        if len(table_names) > 0:
            for name in table_names:
                self.__relations[name] = LazyTable(self.engine, "NO ACTION", "NO ACTION", table=Table(
                    name, self.__metadata, autoload=True), evaluated=True)

    def __getitem__(self, key):
        try:
            return self.__relations[key]
        except:
            raise KeyError(
                f"Table '{key}' is not defined. Call create_table to add this relation.")

    def create_table(self, name, ondelete="NO ACTION", onupdate="NO ACTION"):
        if not name:
            raise TypeError("Table must have a name!")
        if name in self.__relations.keys():
            raise RuntimeError(f"Table names must be unique. {name} exists!")
        self.__relations[name] = LazyTable(self.engine, ondelete, onupdate)
        return self.__relations[name]

    def try_create(self):
        for name, table in self.__relations.items():
            if table.evaluated:
                continue
            table.consolidate(self.__metadata, name)

        self.__metadata.create_all()

    def clean_up(self):
        if self.test:
            self.__metadata.drop_all()

    def pool(self, tables, op, data):
        table_q = Queue()
        with self.engine.connect() as conn:
            if op == Op.INSERT:
                for table in tables:
                    table_q.enqueue(self.__relations[table].get_table())
                for item in data:
                    conn.execute(insert(table_q.poll()), item)
            conn.commit()


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

    def insert(self, data):
        if not self.evaluated:
            self.throwUndefinedStateException()
        if not data:
            raise IntegrityError("Insert must have values")
        return Thunk(self.engine, self.table, insert(self.table).values(data))

    def fetch_all(self):
        if not self.evaluated:
            self.throwUndefinedStateException()
        return Thunk(self.engine, self.table, select(self.table)).fetch_all()

    def fetch(self, cols=None):
        if not cols:
            return self.fetch_all()
        if not self.evaluated:
            self.throwUndefinedStateException()
        return Thunk(self.engine, self.table, select(*[self.table.c[col] for col in cols])).fetch_all()

    def join(self, right_table):
        return Thunk(self.engine, self.table, self.table.join(right_table))

    def filter(self, predicate):
        return Thunk(self.engine, self.table, select(self.table).where(predicate))

    def either(self, predicate1, predicate2):
        return Thunk(self.engine, self.table, select(self.table).where(or_(predicate1, predicate2)))

    def c(self, column=None):
        if not column:
            return self.table.c.keys()
        return self.table.c[column]

    def change(self, col, to, where):
        stmt = update(self.table).where(where).values({col: to})
        return Thunk(self.engine, self.table, stmt)
    
    def change_all(self, col, to):
        return Thunk(self.engine, self.table, update(self.table).values({col: to}))
    
    def delete(self):
        return Thunk(self.engine, self.table, delete(self.table))

class Thunk(object):
    def __init__(self, engine, table, stmt):
        self.evaluated = False
        self.engine = engine
        self.table = table
        self.stmt = stmt

    def fetch_all(self):
        if not self.evaluated:
            with self.engine.connect() as conn:
                return [dict(row) for row in conn.execute(self.stmt)]
        self.evaluated = True
    
    def and_(self, predicate):
        if not self.evaluated:
            stmt = self.stmt.where(predicate)
        return Thunk(self.engine, self.table, stmt)

    def commit(self):
        with self.engine.connect() as conn:
            conn.execute(self.stmt)
            conn.commit()

    def where(self, predicate):
        self.stmt.where(predicate)
        return self


Op = Enum('Operations', 'INSERT UPDATE DELETE')
