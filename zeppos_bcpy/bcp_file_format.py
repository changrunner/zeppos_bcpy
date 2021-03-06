from os import remove, linesep
from zeppos_data_manager.data_cleaner import DataCleaner
from zeppos_logging.app_logger import AppLogger


class BcpFileFormat:
    def __init__(self, pandas_dataframe, temp_full_file_name, sep='|', quote_char='', line_terminator=linesep):
        self.pandas_dataframe = pandas_dataframe
        self.temp_full_file_name = temp_full_file_name
        self.file_format_full_file_name = f"{temp_full_file_name}.fmt"
        self.sep = sep
        self.quote_char = quote_char
        self.line_terminator = line_terminator

    @staticmethod
    def create_bcp_format_file_instance_from_dataframe(
            pandas_dataframe, temp_full_file_name, sep='|', quote_char='', line_terminator=linesep):
        bcp_file_format = BcpFileFormat(pandas_dataframe, temp_full_file_name, sep, quote_char, line_terminator)
        bcp_file_format.create_bcp_format_file_from_dataframe()
        return bcp_file_format

    def create_bcp_format_file_from_dataframe(self):
        version = "14.0"
        no_of_columns = len(self.pandas_dataframe.columns)

        bcp_format = DataCleaner.strip_content(
            f"""
                {version}
                {no_of_columns}
                {self._get_bcp_format_for_columns()}
            """, remove_last_line_seperator=False
        )
        AppLogger.logger.debug(f"bcp_format: \n{bcp_format}")
        AppLogger.logger.debug(f"Writting file_format file: {self.file_format_full_file_name}")
        with open(self.file_format_full_file_name, 'w') as fl:
            fl.write(bcp_format)

    def _get_bcp_format_for_columns(self):
        column_format = ""
        field_index = 1
        for column in self.pandas_dataframe:
            data_type = "SQLCHAR"
            prefix_len = 0
            data_length = 0
            column_order = field_index
            column_name = column
            column_collation = "SQL_Latin1_General_CP1_CI_AS"

            if field_index < len(self.pandas_dataframe.columns):
                separator = DataCleaner.adjust_escape_character(self.sep)
            else:
                separator = DataCleaner.adjust_escape_character(self.line_terminator)

            column_format += f"{field_index} {data_type} {prefix_len} {data_length} " \
                             f"\"{separator}\" " \
                             f"{column_order} {column_name} {column_collation}\n"
            field_index += 1

        return column_format

    def remove_file(self):
        AppLogger.logger.debug(f"Remove bcp fileformat file: {self.file_format_full_file_name}")
        remove(self.file_format_full_file_name)
