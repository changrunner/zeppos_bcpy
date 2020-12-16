from zeppos_bcpy.sql_table import SqlTable
from deprecated import deprecated

class SqlConfiguration:
    def __init__(self, server_type, server_name, database_name, schema_name, table_name,
                 username=None, password=None):
        self.server_type = server_type
        self.server_name = server_name
        self.database_name = database_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.username = username
        self.password = password

    @deprecated(version='0.0.20', reason="""
        This method is deprecated
        You should use:
            from zeppos_bcpy.sql_table import SqlTable
            
            SqlTable.create(sql_configuration=SqlConfiguration(...), columns_dict=[...], using_existing=False  
    """)
    def create(self, columns_dict, use_existing=False):
        return \
            SqlTable.create(
                sql_configuration=self,
                column_dict=columns_dict,
                use_existing=use_existing
            )

    def get_pyodbc_connection_string(self, odbc_version=17):
        return f"DRIVER={{ODBC Driver {odbc_version} for SQL Server}}; SERVER={self.server_name}; " \
               f"DATABASE={self.database_name}; Trusted_Connection=yes;"

    @property
    def server_name_clean(self):
        return self.server_name.replace("\\", "_").replace("/", "_")
