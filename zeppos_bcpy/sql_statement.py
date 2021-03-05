class SqlStatement:
    @staticmethod
    def get_table_create_statement(schema_name, table_name, column_dict, discover_data_type=False):
        columns_string = \
            ', '.join(
                [f"[{k}] {SqlStatement._get_column_type(v, discover_data_type)}"
                 for k, v in column_dict.items()]
            )

        return f"create table [{schema_name}].[{table_name}] ({columns_string})"

    @staticmethod
    def _get_column_type(column_type, discover_data_type=False):
        if not discover_data_type:
            return "nvarchar(max)"

        return "nvarchar(max)"

    @staticmethod
    def get_table_drop_statement(schema_name, table_name):
        return f"drop table if exists [{schema_name}].[{table_name}]"

    @staticmethod
    def get_table_drop_and_create_statement(schema_name, table_name, column_dict, discover_data_type=False):
        return SqlStatement.get_table_drop_statement(schema_name, table_name) + \
               "\n" + \
               SqlStatement.get_table_create_statement(schema_name, table_name, column_dict, discover_data_type)

    @staticmethod
    def get_does_table_exist_statement(schema_name, table_name):
        return f"select count(1) as record_count " \
               f"from INFORMATION_SCHEMA.TABLES " \
               f"where TABLE_SCHEMA = '{schema_name}' and TABLE_NAME = '{table_name}'"

    @staticmethod
    def get_table_column_names(schema_name, table_name, separator):
        return f"SELECT column_names = " \
               f"STUFF( " \
               f"(SELECT '{separator}' + CAST((COLUMN_NAME) AS varchar(550)) [text()]" \
               f"FROM INFORMATION_SCHEMA.columns (nolock) " \
               f"WHERE table_schema = '{schema_name}' " \
               f"AND table_name = '{table_name}'" \
               f"ORDER BY ORDINAL_POSITION " \
               f"FOR XML PATH(''), TYPE) " \
               f".value('.','NVARCHAR(MAX)'),1,1,' ')"

