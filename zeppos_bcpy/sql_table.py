from zeppos_bcpy.sql_statement import SqlStatement
from zeppos_bcpy.sql_cmd import SqlCmd

class SqlTable:
    @staticmethod
    def create(sql_configuration, column_dict, use_existing=False):
        if use_existing:
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
