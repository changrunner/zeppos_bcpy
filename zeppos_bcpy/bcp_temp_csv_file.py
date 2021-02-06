from zeppos_logging.app_logger import AppLogger
from os import remove


class BcpTempCsvFile:
    def __init__(self, pandas_dataframe, bcp_file_format, use_index=False):
        self.pandas_dataframe = pandas_dataframe
        self.bcp_file_format = bcp_file_format
        self.csv_full_file_name = f"{bcp_file_format.temp_full_file_name}.csv"
        self.use_index = use_index

    @staticmethod
    def write_df_to_csv_creating_instance(pandas_dataframe, bcp_file_format, use_index=False):
        bcp_temp_csv_file = BcpTempCsvFile(pandas_dataframe, bcp_file_format, use_index)
        bcp_temp_csv_file._write_df_to_csv()
        return bcp_temp_csv_file

    def _write_df_to_csv(self):
        AppLogger.logger.debug(f"Create temporary csv file: {self.csv_full_file_name} from pandas_dataframe")
        AppLogger.logger.debug(f"Parms: index=[{self.use_index}], sep=[{self.bcp_file_format.sep}], "
                               f"line_terminator=[{self.bcp_file_format.line_terminator}], "
                               f"path_or_buf=[{self.csv_full_file_name}]")
        AppLogger.logger.debug(f"No of Records: {self.pandas_dataframe.shape[0]}")
        self.pandas_dataframe.to_csv(
            index=self.use_index,
            sep=self.bcp_file_format.sep,
            line_terminator=self.bcp_file_format.line_terminator,
            path_or_buf=self.csv_full_file_name)

    def remove_file(self):
        AppLogger.logger.debug(f"Remove bcp temp file: {self.csv_full_file_name}")
        remove(self.csv_full_file_name)
