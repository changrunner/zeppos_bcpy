from zeppos_bcpy.sql_table import SqlTable
from deprecated import deprecated
import copy


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

    def validate_and_augment(self, file_name_without_extension):
        """
        It is possible the table_name property of the sql_configuration is None.
        If that is the case, we will be using the file name without extension of the csv file
        :param file_name_without_extension: The name of the file without extension.
                                            This will become the table_name if table_name is null or None
        :return: shallow copy of sql_configuration
        """
        return_sql_configuration = copy.copy(self)  # shallow copy needed.
        if not return_sql_configuration.table_name:  # null or None table_name
            return_sql_configuration.table_name = file_name_without_extension
        return return_sql_configuration
