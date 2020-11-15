import unittest
from zeppos_bcpy.sql_statement import SqlStatement
import pandas as pd
import numpy as np

class TestTheProjectMethods(unittest.TestCase):
    def test_get_table_create_statement_method(self):
        df = pd.DataFrame({'seconds': [3600], 'minutes': [10]}, columns=['seconds', 'minutes'])
        self.assertEqual(SqlStatement.get_table_create_statement("dbo", "staging_test", df.dtypes.to_dict()),
                         "create table [dbo].[staging_test] ([seconds] varchar(max), [minutes] varchar(max))")

    def test__get_column_type_method(self):
        df = pd.DataFrame({'seconds': [3600], 'minutes': ["10"]}, columns=['seconds', 'minutes'])
        self.assertEqual(SqlStatement._get_column_type(df.dtypes[0]), "varchar(max)")
        self.assertEqual(SqlStatement._get_column_type(df.dtypes[1]), "varchar(max)")

    def test_get_table_drop_statement_method(self):
        self.assertEqual(SqlStatement.get_table_drop_statement('dbo', 'table'), "drop table if exists [dbo].[table]")

    def test_get_table_drop_and_create_statement_method(self):
        df = pd.DataFrame({'seconds': [3600], 'minutes': [10]}, columns=['seconds', 'minutes'])
        self.assertEqual(SqlStatement.get_table_drop_and_create_statement("dbo", "table", df.dtypes.to_dict()),
                         "drop table if exists [dbo].[table]\ncreate table [dbo].[table] ([seconds] varchar(max), [minutes] varchar(max))")


if __name__ == '__main__':
    unittest.main()
