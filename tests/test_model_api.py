import unittest
from db.dal import Database


class TestModelAPI(unittest.TestCase):

    def setUp(self):
        self.db = Database(config=r".\sample user files\config.ini", test=True)

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

        self.db.clean_up()

    def test_table_without_name_error(self):
        self.assertRaises(TypeError, self.db.create_table)

    def test_existing_table(self):
        self.db.create_table('user')\
            .col('uid', 'Integer', primary_key=True)\
            .col('name', 'String')
        self.db.try_create()
        self.assertRaises(RuntimeError, self.db.create_table, 'user')
        self.db.clean_up()

    def test_load_tables_at_start(self):
        db2 = Database(json_config=r".\sample user files\sri.json")
        self.assertListEqual([{'sid': 1, 'name': 'prabha'}, {
                             'sid': 2, 'name': 'nimesha'}], db2['student'].fetch_all())

    def test_multi_insert_joins(self):
        self.db.create_table('user')\
            .col('uid', 'Integer', primary_key=True)\
            .col('name', 'String')
        self.db.create_table('contact')\
            .col('eid', 'Integer', primary_key=True)\
            .col('email', 'String')\
            .col('uid', 'Integer', references='user')
        self.db.try_create()
        self.db.pool(tables=['user', 'contact'], op=Op.INSERT, data=[{
            'name': 'Ranmal Dias'
        }, {
            'email': 'dev.mendy.das@gmail.com',
            'uid': 1
        }])

        self.assertDictEqual({'uid': 1, 'name': 'Ranmal Dias',
                             'eid': 1, 'email': 'dev.mendy.das@gmail.com', 'uid': 1}, self.db['user'].join('contact', on='user.uid = contact.uid').fetch_all()[0])

    def test_multi_multi_insert(self):
        pass

    def test_nullable_constraint(self):
        pass

    def test_try_access_undefined_table(self):
        pass

    def test_try_filter(self):
        pass

    def test_try_update(self):
        pass

    def test_try_delete(self):
        pass
