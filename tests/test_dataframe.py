import unittest
from zeppos_bcpy.dataframe import Dataframe
from zeppos_bcpy.sql_configuration import SqlConfiguration
import pandas as pd
import pyodbc
from pandas._testing import assert_frame_equal
from zeppos_data_manager.df_cleaner import DfCleaner
import os
import shutil

class TestTheProjectMethods(unittest.TestCase):
    def test_to_sqlserer_method(self):
        df_expected = pd.DataFrame({'seconds': ["3600"], 'minutes': ["10"]}, columns=['seconds', 'minutes'])

        sql_configuration = SqlConfiguration(
            server_type="microsoft",
            server_name="localhost\\sqlexpress",
            database_name="master",
            schema_name="dbo",
            table_name="staging_test"
        )
        dataframe = Dataframe.to_sqlserver_creating_instance(df_expected, sql_configuration)
        self.assertEqual("<class 'zeppos_bcpy.dataframe.Dataframe'>",
                         str(type(dataframe)))
        df_actual = pd.read_sql("select SECONDS, MINUTES from dbo.staging_test",
                                pyodbc.connect(r'DRIVER={ODBC Driver 13 for SQL Server}; SERVER=localhost\sqlexpress; DATABASE=master; Trusted_Connection=yes;'))
        df_expected = df_expected[['SECONDS', 'MINUTES']]
        assert_frame_equal(df_actual, df_expected)

    def test_to_sqlserer_additional_static_data_method(self):
        df_expected = pd.DataFrame({'seconds': ["3600"], 'minutes': ["10"]}, columns=['seconds', 'minutes'])

        sql_configuration = SqlConfiguration(
            server_type="microsoft",
            server_name="localhost\\sqlexpress",
            database_name="master",
            schema_name="dbo",
            table_name="staging_test"
        )
        additional_static_data_dict = {'static_field1': 'some info 1', 'static_field2': 'some info 2'}
        dataframe = Dataframe.to_sqlserver_creating_instance(df_expected, sql_configuration, additional_static_data_dict)
        self.assertEqual("<class 'zeppos_bcpy.dataframe.Dataframe'>",
                         str(type(dataframe)))
        df_actual = pd.read_sql("select SECONDS, MINUTES, STATIC_FIELD1, STATIC_FIELD2 from dbo.staging_test",
                                pyodbc.connect(r'DRIVER={ODBC Driver 13 for SQL Server}; SERVER=localhost\sqlexpress; DATABASE=master; Trusted_Connection=yes;'))
        df_expected = df_expected[['SECONDS', 'MINUTES', 'STATIC_FIELD1', 'STATIC_FIELD2']]
        assert_frame_equal(df_actual, df_expected)

    def test_to_csv_method(self):
        sql_configuration = SqlConfiguration(
            server_type="microsoft",
            server_name="localhost\\sqlexpress",
            database_name="master",
            schema_name="dbo",
            table_name="spt_monitor"
        )
        dataframe = Dataframe.to_csv_creating_instance(sql_configuration=sql_configuration, csv_root_directory=r"c:\data")
        self.assertEqual("<class 'zeppos_bcpy.dataframe.Dataframe'>",
                         str(type(dataframe)))



if __name__ == '__main__':
    unittest.main()
