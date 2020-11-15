from zeppos_logging.setup_logger import logger
import csv
from os import remove, linesep

class BcpTempCsvFile:
    def __init__(self, df, bcp_file_format, use_index=False):
        self.df = df
        self.bcp_file_format = bcp_file_format
        self.csv_full_file_name = f"{bcp_file_format.temp_full_file_name}.csv"
        self.use_index = use_index

    @staticmethod
    def write_df_to_csv_creating_instance(df, bcp_file_format, use_index=False):
        bcp_temp_csv_file = BcpTempCsvFile(df, bcp_file_format, use_index)
        bcp_temp_csv_file.write_df_to_csv()
        return bcp_temp_csv_file

    def write_df_to_csv(self):
        logger.debug(f"Create temporary csv file: {self.csv_full_file_name}")
        self.df.to_csv(
            index=self.use_index,
            sep=self.bcp_file_format.sep,
            line_terminator=self.bcp_file_format.line_terminator,
            path_or_buf=self.csv_full_file_name)

        return True

    def remove_file(self):
        remove(self.file_format_full_file_name)
