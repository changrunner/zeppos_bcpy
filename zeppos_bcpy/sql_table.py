from zeppos_bcpy.sql_statement import SqlStatement
from zeppos_bcpy.sql_cmd import SqlCmd
import pandas as pd
import pyodbc

class SqlTable:
    @staticmethod
    def create(sql_configuration, column_dict, use_existing=False):
        if use_existing and SqlTable._does_table_exist(sql_configuration):
            return True

        create_table_sql = \
            SqlStatement.get_table_drop_and_create_statement(
                schema_name=sql_configuration.schema_name,
                table_name=sql_configuration.table_name,
                column_dict=column_dict
            )
        return SqlCmd.execute(
            server_name=sql_configuration.server_name,
            database_name=sql_configuration.database_name,
            command=create_table_sql
        )

    @staticmethod
    def _does_table_exist(sql_configuration):
        df = pd.read_sql(
            SqlStatement.get_does_table_exist_statement(sql_configuration.schema_name, sql_configuration.table_name),
            pyodbc.connect(sql_configuration.get_pyodbc_connection_string())
        )
        return next(df.iterrows())[1]['record_count'] > 0
