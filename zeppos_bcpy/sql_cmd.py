from subprocess import Popen, PIPE
from zeppos_logging.setup_logger import logger
from zeppos_bcpy.sql_security import SqlSecurity

class SqlCmd:
    @staticmethod
    def execute(server_name, database_name, command, username=None, password=None):
        if SqlSecurity.use_integrated_security(username, password):
            auth = ['-E']
        else:
            auth = ['-U', username, '-P', password]
        command = 'set nocount on;' + command
        sql_command = ['sqlcmd', '-S', server_name, '-d', database_name, '-b'] + auth + \
                      ['-s,', '-W', '-Q', command]
        logger.debug(f"sql command: {sql_command}")

        p = Popen(sql_command, stdout=PIPE)
        out, err = p.communicate()
        out_array = out.decode("utf-8").split('\r\n')

        logger.debug(f"sql command execute result: {out_array}")

        if len(out_array[0].strip()) > 0:
            logger.error(f"Error executing cmd [sql_command] with error: {out_array}")

        return True