import unittest
from zeppos_bcpy.dataframe import Dataframe
from zeppos_bcpy.sql_configuration import SqlConfiguration

import pandas as pd


class TestTheProjectMethods(unittest.TestCase):
    def test_to_sqlserer_method(self):
        df = pd.DataFrame({'seconds': [3600], 'minutes': [10]}, columns=['seconds', 'minutes'])

        sql_configuration = SqlConfiguration(
            server_type="microsoft",
            server_name="localhost\\sqlexpress",
            database_name="CSMMart",
            schema_name="dbo",
            table_name="staging_test"
        )
        Dataframe.to_sqlserver_creating_instance(df, sql_configuration)


if __name__ == '__main__':
    unittest.main()
