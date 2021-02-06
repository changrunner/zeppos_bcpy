from zeppos_bcpy.bcp_cmd import BcpCmd
from subprocess import Popen, PIPE
from zeppos_logging.app_logger import AppLogger
import os


class BcpIn:
    def __init__(self, sql_table, bcp_file_format, bcp_temp_csv_file, batch_size=10000):
        self._sql_table = sql_table
        self._bcp_file_format = bcp_file_format
        self._bcp_temp_csv_file = bcp_temp_csv_file
        self._batch_size = batch_size
        self._bcp_cmd = BcpCmd.get_command_for_data_in(sql_table, bcp_temp_csv_file.csv_full_file_name,
                                                       bcp_file_format.file_format_full_file_name, batch_size)

    def execute(self):
        AppLogger.logger.debug(f"--------- BcpIn -- Start Executing")
        if not os.path.exists(self._bcp_file_format.file_format_full_file_name):
            AppLogger.logger.error(f"bcp_file_format file [{self._bcp_file_format.file_format_full_file_name}] does not exist")
            return False

        if not os.path.exists(self._bcp_temp_csv_file.csv_full_file_name):
            AppLogger.logger.error(f"bcp_temp_csv_file file [{self._bcp_temp_csv_file.csv_full_file_name}] does not exist")
            return False

        p = Popen(self._bcp_cmd, stdout=PIPE)
        out, err = p.communicate()
        out_array = out.decode("utf-8").split('\r\n')

        for out_string in out_array:
            if len(out_string.strip()) > 0:
                AppLogger.logger.debug(f"BcpIn execute result: {out_string}")

        AppLogger.logger.debug(f"--------- BcpIn -- End Executing")
        return True
