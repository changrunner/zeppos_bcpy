from zeppos_file_manager.temp_file import TempFile
from zeppos_bcpy.bcp_file_format import BcpFileFormat
from zeppos_bcpy.bcp_temp_csv_file import BcpTempCsvFile
from zeppos_bcpy.bcp import Bcp
from zeppos_data_manager.df_cleaner import DfCleaner

class Dataframe:
    def __init__(self, pandas_dataframe, sql_configuration, index=False, discover_data_type=False,
                 use_existing=False, batch_size=10000):
        self.pandas_dataframe = Dataframe._conform_pandas_dataframe(pandas_dataframe)
        self.sql_configuration = sql_configuration
        self.index = index
        self.discover_data_type = discover_data_type
        self.use_existing = use_existing
        self.batch_size = batch_size

    @staticmethod
    def to_sqlserver_creating_instance(
            pandas_dataframe, sql_configuration, index=False, discover_data_type=False, use_existing=False, batch_size=10000):
        dataframe = Dataframe(pandas_dataframe, sql_configuration, index, discover_data_type, use_existing, batch_size)
        dataframe.to_sqlserver()
        return dataframe

    def to_sqlserver(self):
        bcp_file_format = BcpFileFormat.create_bcp_format_file_instance_from_dataframe(self.pandas_dataframe, TempFile().temp_full_file_name)
        bcp_temp_csv_file = BcpTempCsvFile.write_df_to_csv_creating_instance(self.pandas_dataframe, bcp_file_format, self.index)

        self.sql_configuration.create(self.pandas_dataframe.dtypes.to_dict(), self.use_existing)
        Bcp(self.sql_configuration, bcp_file_format, bcp_temp_csv_file, self.batch_size).execute()

        bcp_temp_csv_file.remove_file()
        bcp_file_format.remove_file()

    @staticmethod
    def _conform_pandas_dataframe(pandas_dataframe):
        DfCleaner.clean_column_names_in_place(pandas_dataframe)
        return pandas_dataframe
