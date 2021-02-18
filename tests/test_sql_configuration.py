import unittest
from zeppos_bcpy.sql_configuration import SqlConfiguration

class TestTheProjectMethods(unittest.TestCase):
    def test_constructor_method(self):
        sql_configuration = SqlConfiguration("microsoft", "server", "database", "schema", "table")
        self.assertEqual("<class 'zeppos_bcpy.sql_configuration.SqlConfiguration'>",
                         str(type(sql_configuration)))
        self.assertEqual("microsoft", sql_configuration.server_type)
        self.assertEqual("server", sql_configuration.server_name)
        self.assertEqual("database", sql_configuration.database_name)
        self.assertEqual("schema", sql_configuration.schema_name)
        self.assertEqual("table", sql_configuration.table_name)
        self.assertEqual(None, sql_configuration.username)
        self.assertEqual(None, sql_configuration.password)

    def test_create_method(self):
        sql_configuration = SqlConfiguration("microsoft", r"localhost\sqlexpress", "master", "dbo", "staging_test")
        self.assertEqual(True, sql_configuration.create({"col1", "int"}, use_existing=True))

    def test_1_get_pyodbc_connection_string_method(self):
        sql_configuration = SqlConfiguration("microsoft", r"localhost\sqlexpress", "master", "dbo", "staging_test")
        self.assertEqual("DRIVER={ODBC Driver 17 for SQL Server}; SERVER=localhost\sqlexpress; DATABASE=master; Trusted_Connection=yes;",
                         sql_configuration.get_pyodbc_connection_string())

    def test_1__validate_and_augment_if_needed_sql_configuration_method(self):
       sql_configuration_original = SqlConfiguration(
                                server_type="microsoft",
                                server_name="localhost\\sqlexpress",
                                database_name="master",
                                schema_name="dbo",
                                table_name="test_table"
                            )
       self.assertEqual("test_table",
                        sql_configuration_original.validate_and_augment(file_name_without_extension="temp").table_name)
       self.assertEqual("test_table", sql_configuration_original.table_name)

    def test_2__validate_and_augment_if_needed_sql_configuration_method(self):
        sql_configuration_original = SqlConfiguration(
            server_type="microsoft",
            server_name="localhost\\sqlexpress",
            database_name="master",
            schema_name="dbo",
            table_name=None
        )
        self.assertEqual("temp",
                         sql_configuration_original.validate_and_augment(file_name_without_extension="temp").table_name)
        self.assertEqual(None, sql_configuration_original.table_name)


if __name__ == '__main__':
    unittest.main()
