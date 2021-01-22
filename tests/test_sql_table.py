import unittest
from zeppos_bcpy.sql_table import SqlTable
from zeppos_bcpy.sql_configuration import SqlConfiguration
import pandas as pd
import pyodbc

class TestTheProjectMethods(unittest.TestCase):
    def test_constructor_method(self):
        self.assertEqual(str(type(SqlTable())), "<class 'zeppos_bcpy.sql_table.SqlTable'>")

    def test_1_create_method(self):
        TestTheProjectMethods._execute_sql("drop table if exists dbo.stating_test_1")
        self.assertEqual(True,
                         SqlTable.create(
                             SqlConfiguration("microsoft", r"localhost\sqlexpress", "master", "dbo", "staging_test_1"),
                             pd.DataFrame({'seconds': [3600], 'minutes': [10]}, columns=['seconds', 'minutes']).dtypes.to_dict(),
                             use_existing=True)
                         )
        df = TestTheProjectMethods._get_data("select count(1) as record_count from information_schema.tables where table_schema='dbo' and table_name='staging_test_1'")
        self.assertEqual(1, next(df.iterrows())[1]['record_count'])
        TestTheProjectMethods._execute_sql("drop table if exists dbo.stating_test_1")

    def test_2_create_method(self):
        TestTheProjectMethods._execute_sql("drop table if exists dbo.stating_test_2")
        TestTheProjectMethods._execute_sql("create table dbo.stating_test_2 (test int)")
        self.assertEqual(True,
                         SqlTable.create(
                             SqlConfiguration("microsoft", r"localhost\sqlexpress", "master", "dbo", "staging_test_2"),
                             pd.DataFrame({'seconds': [3600], 'minutes': [10]}, columns=['seconds', 'minutes']).dtypes.to_dict(),
                             use_existing=True)
                         )
        df = TestTheProjectMethods._get_data("select count(1) as record_count from information_schema.tables where table_schema='dbo' and table_name='staging_test_2'")
        self.assertEqual(1, next(df.iterrows())[1]['record_count'])
        TestTheProjectMethods._execute_sql("drop table if exists dbo.stating_test_2")

    def test_3_create_method(self):
        TestTheProjectMethods._execute_sql("drop table if exists dbo.stating_test_3")
        df = pd.DataFrame({'seconds': [3600], 'minutes': [10]}, columns=['seconds', 'minutes'])
        self.assertEqual(True,
                         SqlTable.create(
                             SqlConfiguration("microsoft", r"localhost\sqlexpress", "master", "dbo", "staging_test_3"),
                             df.dtypes.to_dict(), use_existing=False)
                         )
        df_actual = TestTheProjectMethods._get_data("select count(1) as record_count from information_schema.tables where TABLE_NAME = 'staging_test_3' and TABLE_SCHEMA = 'dbo'")
        self.assertEqual(1, next(df_actual.iterrows())[1]['record_count'])
        TestTheProjectMethods._execute_sql("drop table if exists dbo.stating_test_3")

    def test_4_create_method(self):
        TestTheProjectMethods._execute_sql("drop table if exists dbo.stating_test_4")
        TestTheProjectMethods._execute_sql("create table dbo.stating_test_4 (test int)")
        df = pd.DataFrame({'seconds': [3600], 'minutes': [10]}, columns=['seconds', 'minutes'])
        self.assertEqual(True,
                         SqlTable.create(
                             SqlConfiguration("microsoft", r"localhost\sqlexpress", "master", "dbo", "staging_test_4"),
                             df.dtypes.to_dict(), use_existing=False)
                         )
        df_actual = TestTheProjectMethods._get_data("select count(1) as record_count from information_schema.tables where TABLE_NAME = 'staging_test_4' and TABLE_SCHEMA = 'dbo'")
        self.assertEqual(1, next(df_actual.iterrows())[1]['record_count'])
        TestTheProjectMethods._execute_sql("drop table if exists dbo.stating_test_4")

    def test__does_table_exist_method(self):
        self.assertEqual(True,
                         SqlTable._does_table_exist(
                             SqlConfiguration("microsoft", r"localhost\sqlexpress", "master", "dbo", "spt_fallback_db"),
                         )
                         )

    def test__get_column_names(self):
        self.assertEqual(
            SqlTable.get_column_names(
                SqlConfiguration("microsoft", r"localhost\sqlexpress", "master", "dbo", "spt_monitor"),
                "|"),
        "lastrun|cpu_busy|io_busy|idle|pack_received|pack_sent|connections|pack_errors|total_read|total_write|total_errors")

    @staticmethod
    def _execute_sql(sql_statement):
        conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server}; SERVER=localhost\sqlexpress; DATABASE=master; Trusted_Connection=yes;")
        crs = conn.cursor()
        crs.execute(sql_statement)
        crs.commit()
        crs.close()

    @staticmethod
    def _get_data(sql_statement):
        return pd.read_sql(sql_statement,
                           pyodbc.connect(
                               "DRIVER={ODBC Driver 17 for SQL Server}; SERVER=localhost\sqlexpress; DATABASE=master; Trusted_Connection=yes;"))


if __name__ == '__main__':
    unittest.main()

