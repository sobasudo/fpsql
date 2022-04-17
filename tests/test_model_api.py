import unittest
from db.dal import Database

class TestModelAPI(unittest.TestCase):

    def setUp(self):
        self.db = Database(config=r".\sample user files\testconfig.ini")

    def test_simple_insert(self):
        schema = self.db.create_schema(name='auth')
        schema.create_table('user')\
            .col('uid', 'Integer', primary_key=True)\
            .col('name', 'String')
        schema.try_create()
        schema['user'].from_dict(data={
            'name': 'Ranmal Dias'
        })
        self.assertDictEqual({
            'uid': 1,
            'name': 'Ranmal Dias'
        }, schema['user'].fetch_all())

    def test_database_no_config_error(self):
        self.assertRaises(RuntimeError, Database)

    def test_table_without_name_error(self):
        self.assertRaises(RuntimeError, self.db.create_schema(
            name="test").create_table())
