import unittest
from db.dal import Database


class TestModelAPI(unittest.TestCase):

    def setUp(self):
        self.db = Database(config=r".\sample user files\testconfig.ini")

    def test_simple_insert(self):
        self.db.create_table('user')\
            .col('uid', 'Integer', primary_key=True)\
            .col('name', 'String')
        self.db.try_create()
        self.db['user'].insert(data={
            'name': 'Ranmal Dias'
        })
        
        self.assertDictEqual({
            'uid': 1,
            'name': 'Ranmal Dias'
        }, self.db['user'].fetch_all()[0])

    def test_database_no_config_error(self):
        self.assertRaises(RuntimeError, Database)

    def test_table_without_name_error(self):
        self.assertRaises(TypeError, self.db.create_table)
        
    def test_existing_table(self):
        self.db.create_table('user')\
            .col('uid', 'Integer', primary_key=True)\
            .col('name', 'String')
        self.assertRaises(RuntimeError, self.db.create_table, 'user')

    def test_load_tables_at_start(self):
        pass

    def test_foreign_key_constraint(self):
        pass

    def test_nullable_constraint(self):
        pass

    def test_try_access_undefined_table(self):
        pass