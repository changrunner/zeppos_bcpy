import unittest
from zeppos_bcpy.sql_table import SqlTable
from zeppos_bcpy.sql_configuration import SqlConfiguration
import pandas as pd
import pyodbc

class TestTheProjectMethods(unittest.TestCase):
    def test_constructor_method(self):
        self.assertEqual(str(type(SqlTable())), "<class 'zeppos_bcpy.sql_table.SqlTable'>")

    def test_1_create_method(self):
        self.assertEqual(True,
                         SqlTable.create(
                             SqlConfiguration("microsoft", r"localhost\sqlexpress", "master", "dbo", "staging_test"),
                             {"col1", "int"}, use_existing=True)
                         )

    def test_2_create_method(self):
        df = pd.DataFrame({'seconds': [3600], 'minutes': [10]}, columns=['seconds', 'minutes'])
        self.assertEqual(True,
                         SqlTable.create(
                             SqlConfiguration("microsoft", r"localhost\sqlexpress", "master", "dbo", "staging_test"),
                             df.dtypes.to_dict(), use_existing=False)
                         )
        df_actual = pd.read_sql("select count(1) as record_count from information_schema.tables where TABLE_NAME = 'staging_test' and TABLE_SCHEMA = 'dbo'",
                                pyodbc.connect(r'DRIVER={ODBC Driver 13 for SQL Server}; SERVER=localhost\sqlexpress; DATABASE=master; Trusted_Connection=yes;'))
        self.assertEqual(1, next(df_actual.iterrows())[1]['record_count'])

    def test__does_table_exist_method(self):
        self.assertEqual(True,
                         SqlTable._does_table_exist(
                             SqlConfiguration("microsoft", r"localhost\sqlexpress", "master", "dbo", "spt_fallback_db"),
                         )
                         )


if __name__ == '__main__':
    unittest.main()

