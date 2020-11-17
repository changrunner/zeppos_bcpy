import unittest
from zeppos_bcpy.dataframe import Dataframe
from zeppos_bcpy.sql_table import SqlTable

import pandas as pd


class TestTheProjectMethods(unittest.TestCase):
    def test_to_sqlserer_method(self):
        # df = pd.DataFrame({'seconds': [3600], 'minutes': [10]}, columns=['seconds', 'minutes'])
        # sql_table = SqlTable("microsoft", r"localhost\sqlexpress", "master", 'dbo', 'staging_test')
        # Dataframe.to_sqlserver_creating_instance(df, sql_table)

        sql_table = SqlTable(
            server_type="microsoft",
            server_name="localhost\\sqlexpress",
            database_name="CSMMart",
            schema_name="dbo",
            table_name="staging_spinoff"
        )
        df = pd.read_csv(r"C:\Data\SPINOFF\data\costing\spinoff.csv", low_memory=False)
        Dataframe.to_sqlserver_creating_instance(df, sql_table)


if __name__ == '__main__':
    unittest.main()
