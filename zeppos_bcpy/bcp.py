from zeppos_bcpy.bcp_cmd import BcpCmd
from subprocess import Popen, PIPE
from zeppos_logging.app_logger import AppLogger

class Bcp:
    def __init__(self, sql_table, bcp_file_format, bcp_temp_csv_file, batch_size=10000):
        self._bcp_cmd = BcpCmd.get_command(sql_table, bcp_temp_csv_file.csv_full_file_name,
                                           bcp_file_format.file_format_full_file_name, batch_size)

    def execute(self):
        p = Popen(self._bcp_cmd, stdout=PIPE)
        out, err = p.communicate()
        out_array = out.decode("utf-8").split('\r\n')

        for out_string in out_array:
            if len(out_string.strip()) > 0:
                AppLogger.logger.debug(f"bcp execute result: {out_string}")

        return True
