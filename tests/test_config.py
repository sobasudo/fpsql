import unittest
import pathlib
from db import parser


class TestConfigMethods(unittest.TestCase):
    def setUp(self):
        self.path = pathlib.Path(".").joinpath("sample user files")
        self.json_path = self.path.joinpath("config.json")
        self.cfg_path = self.path.joinpath("config.ini")

    def test_load_json(self):
        with self.json_path.open("r") as file:
            config = parser.load_json(file)
            self.assertDictEqual({
                "dbapi": "postgresql",
                "username": "dev",
                "password": "dev123",
                "server": "localhost",
                "port": "5432",
                "database": "sample"
            }, config, "Json parser not working correctly")
    
    def test_load_ini(self):
        with self.cfg_path.open("r") as file:
            config = parser.load_ini(file)
            self.assertDictEqual({
                "dbapi": "postgresql",
                "username": "dev",
                "password": "dev123",
                "server": "localhost",
                "port": "5432",
                "database": "sample"
            }, dict(config), "Json parser not working correctly")

    def test_genstring(self):
        with self.json_path.open("r") as file:
            config = parser.load_json(file)
            self.assertEqual(parser.gen_config_string(config), "postgresql://dev:dev123@localhost:5432/sample")


    def test_json_engine(self):
        engine = parser.json_engine(r'.\sample user files\config.json')
        try:
            engine.connect()
        except:
            self.fail("Engine can't connect to the database")

    def test_config_engine(self):
        engine = parser.config_engine(r'.\sample user files\config.ini')
        try:
            engine.connect()
        except:
            self.fail("Engine can't connect to the database")
