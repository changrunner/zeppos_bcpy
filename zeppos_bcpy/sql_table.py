from zeppos_bcpy.sql_statement import SqlStatement
from zeppos_bcpy.sql_cmd import SqlCmd

class SqlTable:
    def __init__(self, server_type, server_name, database_name, schema_name, table_name,
                 username=None, password=None):
        self.server_type = server_type
        self.server_name = server_name
        self.database_name = database_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.username = username
        self.password = password

    def create(self, columns_dict, use_existing=False):
        if use_existing:
            return True

        create_table_sql = \
            SqlStatement.get_table_drop_and_create_statement(self.schema_name, self.table_name, columns_dict)
        return SqlCmd.execute(self.server_name, self.database_name, create_table_sql)

