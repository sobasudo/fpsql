from sqlite3 import IntegrityError
import unittest
from db.dal import Database, Op
import psycopg2


class TestModelAPI(unittest.TestCase):
    def setUp(self):
        self.db = Database(
            json_config=r".\sample user files\config.json", test=True)

    def test_simple_insert(self):
        try:
            self.db.create_table('user', ondelete='CASCADE')\
                .col('uid', 'Integer', primary_key=True)\
                .col('name', 'String')
            self.db.try_create()
        except:
            pass
        self.db['user'].insert(data={
            'name': 'Ranmal Dias'
        }).commit()

        self.assertDictEqual({
            'uid': 1,
            'name': 'Ranmal Dias'
        }, self.db['user'].fetch_all()[0])

    def test_table_without_name_error(self):
        self.assertRaises(TypeError, self.db.create_table)

    def test_existing_table(self):
        self.assertRaises(RuntimeError, self.db.create_table, 'user')

    def test_load_tables_at_start(self):
        db2 = Database(json_config=r".\sample user files\sri.json")
        self.assertListEqual([{'sid': 1, 'name': 'prabha'}, {
                             'sid': 2, 'name': 'nimesha'}, {'sid': 3, 'name': 'chamari'}], db2['student'].fetch_all())

    def test_multi_insert_joins(self):
        try:
            self.db.create_table('contact').col('eid', 'Integer', primary_key=True).col(
                'email', 'String').col('user_id', 'Integer', references='user.uid')
            self.db.try_create()
        except RuntimeError:
            pass
        self.db.pool(tables=['user', 'contact'], op=Op.INSERT, data=[{
            'name': 'Ranmal Dias'
        }, {
            'email': 'dev.mendy.das@gmail.com',
            'user_id': 1
        }])

        self.assertDictEqual({'uid': 1, 'name': 'Ranmal Dias',
                              'eid': 1, 'email': 'dev.mendy.das@gmail.com',  'user_id': 1}, self.db['user'].join(self.db['contact']).fetch_all()[0])

    def test_multi_multi_insert(self):
        self.db.pool(tables=['user', 'contact'], op=Op.INSERT, data=[
            [
                {'name': 'Jagath Perera'},
                {'name': 'Iyanthi Goonewardene'}
            ], [
                {'email': 'jobs@thewinstonegroup.com',
                 'user_id': 2}, {'email': 'info@thewinstonegroup.com', 'user_id': 3},
                {'email': 'ranmal@thewinstonegroup.com', 'user_id': 1}
            ]
        ])

        pass

    def test_select_cols(self):
        self.assertIn('name', self.db['user'].fetch(cols=['name'])[0].keys())
        self.assertNotIn('uid', self.db['user'].fetch(cols=['name'])[0].keys())

    def test_nullable_constraint(self):
        try:
            self.db.create_table('sample').col('sa_id', 'Integer', primary_key=True).col(
                'text', 'String', nullable=False)
            self.db.try_create()
        except RuntimeError:
            pass
        self.db['sample'].insert({'text': 'hahahah'}).commit()
        self.assertRaises(IntegrityError, self.db['sample'].insert, {})

    def test_try_access_undefined_table(self):
        self.assertRaises(KeyError, self.db.__getitem__, 'random')

    def test_try_filter(self):
        try:
            self.db.create_table('people')\
                .col('nic', 'Integer', primary_key=True)\
                .col('name', 'String')\
                .col('age', 'Integer', nullable=False)
            self.db.try_create()
            self.db['people'].insert([
                {
                    'name': 'Ranmal Dias',
                    'age': 29
                }, {
                    'name': 'Tom Petty',
                    'age': 16
                }, {
                    'name': 'Bobby Das',
                    'age': 31
                }, {
                    'name': 'Peter Pettigrew',
                    'age': 24
                }, {
                    'name': 'Lionel Poorchie',
                    'age': 52
                }]).commit()
        except RuntimeError as error:
            # print(error)
            pass
        self.assertEqual(3, len(self.db['people'].filter(
            self.db['people'].c('age') > 25).fetch_all()))

    def test_filter_and(self):
        self.assertEqual('Tom Petty', self.db['people'].filter(self.db['people'].c('age') < 20).and_(self.db['people'].c('name') == 'Tom Petty').fetch_all()[0]['name'])

    def test_filter_or(self):
        self.assertEqual('Tom Petty', self.db['people'].either(self.db['people'].c('name') == 'Tom Petty', self.db['people'].c('name') == 'Randy Marshall').fetch_all()[0]['name'])

    def test_try_update(self):
        self.db['people'].change(col='name', to='Bobby Dias', where=self.db['people'].c('name') == 'Bobby Das').commit()
        self.assertEqual('Bobby Dias', self.db['people'].filter(self.db['people'].c('name') == 'Bobby Dias').fetch_all()[0]['name'])

    def test_multi_update(self):
        self.db['sample'].insert({'text': 'something'}).commit()
        self.db['sample'].change_all(col='text', to='reddit').commit()
        self.assertEqual('reddit', self.db['sample'].filter(self.db['sample'].c('text') == 'reddit').fetch_all()[0]['text'])

    def test_try_delete(self):
        self.db['sample'].insert({'text': 'To be deleted'}).commit()
        self.db['sample'].delete().where(self.db['sample'].c('text') == 'To be deleted').commit()
        self.assertNotIn({'text': 'To be deleted'}, self.db['sample'].fetch_all())

    def test_get_columns(self):
        self.assertListEqual(self.db['people'].c(), ['nic', 'name', 'age'])

    def test_multi_join(self):
        self.assertIn({'name': 'Sarah', 'age': 46, 'article_name': 'How to become a mangaka', 'dop': '2022-03-13'}, self.db['author'].join(self.db['article_author']).join(self.db['article']).fetch(cols=[['name', 'age'],[{'name': 'article_name'}, 'dop']]))

    def test_cte(self):
        pass

    def test_view(self):
        pass

    def test_functions(self):
        pass

    def test_subquery(self):
        pass

    def test_rollback_actions(self):
        pass
