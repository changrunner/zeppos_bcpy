import unittest
from zeppos_bcpy.dataframe import Dataframe
from zeppos_bcpy.sql_configuration import SqlConfiguration
import pandas as pd
import pyodbc
from pandas._testing import assert_frame_equal
from zeppos_data_manager.df_cleaner import DfCleaner

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
        self.assertEqual("<class 'zeppos_bcpy.dataframe.Dataframe'>",
                         str(type(Dataframe.to_sqlserver_creating_instance(df_expected, sql_configuration))))
        df_actual = pd.read_sql("select seconds, minutes from dbo.staging_test",
                                pyodbc.connect(r'DRIVER={ODBC Driver 13 for SQL Server}; SERVER=localhost\sqlexpress; DATABASE=master; Trusted_Connection=yes;'))
        DfCleaner.clean_column_names_in_place(df_actual)
        assert_frame_equal(df_expected, df_actual)


if __name__ == '__main__':
    unittest.main()
