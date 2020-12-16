from zeppos_bcpy.bcp_cmd import BcpCmd
from subprocess import Popen, PIPE
from zeppos_logging.app_logger import AppLogger

class BcpOut:
    def __init__(self, sql_table, data_full_file_name, separator="|", batch_size=10000):
        self._bcp_cmd = BcpCmd.get_command_for_data_out(sql_table, data_full_file_name, separator, batch_size)

    def execute(self):
        p = Popen(self._bcp_cmd, stdout=PIPE)
        out, err = p.communicate()
        out_array = out.decode("utf-8").split('\r\n')

        for out_string in out_array:
            if len(out_string.strip()) > 0:
                AppLogger.logger.debug(f"bcp execute result: {out_string}")

        return True
