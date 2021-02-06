from zeppos_logging.app_logger import AppLogger
from zeppos_bcpy.sql_security import SqlSecurity

class BcpCmd:
    @staticmethod
    def get_command_for_data_in(sql_table, data_full_file_name, format_full_file_name, batch_size=10000):
        if SqlSecurity.use_integrated_security(sql_table.username, sql_table.password):
            auth = ['-T']  # Trusted Connection
        else:
            auth = ['-U', sql_table.username, '-P', sql_table.password]

        bcp_command = ['bcp', f'{sql_table.database_name}.{sql_table.schema_name}.{sql_table.table_name}',
                       'IN', data_full_file_name,
                       '-f', format_full_file_name,
                       '-S', sql_table.server_name,
                       '-b', str(batch_size),
                       '-F', '2'  # file_has_header_line:
                       ] + auth

        AppLogger.logger.debug(f"bcp command: {' '.join(bcp_command)}")

        return bcp_command

    @staticmethod
    def get_command_for_data_out(sql_table, data_full_file_name, separator="|", batch_size=10000):
        if SqlSecurity.use_integrated_security(sql_table.username, sql_table.password):
            auth = ['-T']  # Trusted Connection
        else:
            auth = ['-U', sql_table.username, '-P', sql_table.password]

        bcp_command = ['bcp', f'{sql_table.database_name}.{sql_table.schema_name}.{sql_table.table_name}',
                       'OUT', data_full_file_name,
                       '-c',
                       f'-t{separator}',
                       '-S', sql_table.server_name,
                       '-b', str(batch_size),
                       '-r \r\n'
                       ] + auth

        AppLogger.logger.debug(f"bcp command: {' '.join(bcp_command)}")

        return bcp_command
