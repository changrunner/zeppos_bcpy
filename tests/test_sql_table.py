import unittest
from zeppos_bcpy.sql_table import SqlTable

import pandas as pd


class TestTheProjectMethods(unittest.TestCase):
    def test_constructor_method(self):
        sql_table = SqlTable("microsoft", r"localhost\sqlexpress", "master", "dbo", "staging_test", "u1", "p1")
        self.assertEqual(str(type(sql_table)), "<class 'zeppos_bcpy.sql_table.SqlTable'>")
        self.assertEqual(sql_table.server_type, "microsoft")
        self.assertEqual(sql_table.server_name, r"localhost\sqlexpress")
        self.assertEqual(sql_table.database_name, "master")
        self.assertEqual(sql_table.schema_name, "dbo")
        self.assertEqual(sql_table.table_name, "staging_test")
        self.assertEqual(sql_table.username, "u1")
        self.assertEqual(sql_table.password, "p1")

    def test_create_method(self):
        self.assertEqual(
            SqlTable("microsoft", r"localhost\sqlexpress", "master", "dbo", "staging_test")
                .create({"col1", "int"}, use_existing=True), True)



if __name__ == '__main__':
    unittest.main()

