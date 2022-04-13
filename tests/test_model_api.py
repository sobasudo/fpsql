import unittest

class TestModelAPI(unittest.TestCase):
    
    def test_simple_insert(self):
        db = Database(config=r".\sample user files\testconfig.ini")
        models = db.create_schema(schema='auth')
        models.create_model('user')\
            .col('uid', 'Integer', primary_key=True)\
            .col('name', 'String')
        models.try_create()
        models['user'].from_dict(data={
            'name': 'Ranmal Dias'
        })
        self.assertDictEqual({
            'uid': 1,
            'name': 'Ranmal Dias'
        }, models['user'].fetch_all())