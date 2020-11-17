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
            return "varchar(max)"

        return "varchar(max)"

    @staticmethod
    def get_table_drop_statement(schema_name, table_name):
        return f"drop table if exists [{schema_name}].[{table_name}]"

    @staticmethod
    def get_table_drop_and_create_statement(schema_name, table_name, column_dict, discover_data_type=False):
        return SqlStatement.get_table_drop_statement(schema_name, table_name) + \
               "\n" + \
               SqlStatement.get_table_create_statement(schema_name, table_name, column_dict, discover_data_type)
