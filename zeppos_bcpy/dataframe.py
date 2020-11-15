import csv
from zeppos_file_manager.temp_file import TempFile
from zeppos_bcpy.bcp_file_format import BcpFileFormat
from zeppos_bcpy.bcp_temp_csv_file import BcpTempCsvFile
from zeppos_bcpy.bcp import Bcp
from os import remove

class Dataframe:
    @staticmethod
    def to_sqlserver(pandas_dataframe, sql_table, index=False, discover_data_type=False, use_existing=False,
                     batch_size=10000):
        bcp_file_format = BcpFileFormat.create_bcp_format_file_instance_from_dataframe(pandas_dataframe, TempFile().temp_full_file_name)
        bcp_temp_csv_file = BcpTempCsvFile.write_df_to_csv_creating_instance(pandas_dataframe, bcp_file_format, index)
        sql_table.create(pandas_dataframe.dtypes.to_dict(), use_existing)
        Bcp(sql_table, bcp_file_format, bcp_temp_csv_file, batch_size).execute()
        bcp_temp_csv_file.remove_file()
        bcp_file_format.remove_file()


