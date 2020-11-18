import unittest
from zeppos_bcpy.sql_configuration import SqlConfiguration

class TestTheProjectMethods(unittest.TestCase):
    def test_constructor_method(self):
        sql_configuration = SqlConfiguration("microsoft", r"localhost\sqlexpress", "master", "dbo", "staging_test", "u1", "p1")
        self.assertEqual(str(type(sql_configuration)), "<class 'zeppos_bcpy.sql_configuration.SqlConfiguration'>")
        self.assertEqual(sql_configuration.server_type, "microsoft")
        self.assertEqual(sql_configuration.server_name, r"localhost\sqlexpress")
        self.assertEqual(sql_configuration.database_name, "master")
        self.assertEqual(sql_configuration.schema_name, "dbo")
        self.assertEqual(sql_configuration.table_name, "staging_test")
        self.assertEqual(sql_configuration.username, "u1")
        self.assertEqual(sql_configuration.password, "p1")

    def test_create_method(self):
        self.assertEqual(
            SqlConfiguration("microsoft", r"localhost\sqlexpress", "master", "dbo", "staging_test")
                .create({"col1", "int"}, use_existing=True), True)


if __name__ == '__main__':
    unittest.main()

