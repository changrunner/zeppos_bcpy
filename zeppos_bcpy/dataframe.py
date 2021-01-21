from zeppos_file_manager.temp_file import TempFile
from zeppos_bcpy.bcp_file_format import BcpFileFormat
from zeppos_bcpy.bcp_temp_csv_file import BcpTempCsvFile
from zeppos_bcpy.bcp_in import BcpIn
from zeppos_bcpy.bcp_out import BcpOut
from zeppos_data_manager.df_cleaner import DfCleaner
from zeppos_bcpy.sql_table import SqlTable
from datetime import datetime
from os import path, makedirs, remove
from shutil import copyfileobj, move

class Dataframe:
    def __init__(self, sql_configuration):
        # Intialize properties
        self.pandas_dataframe = None
        self.csv_full_file_name = None
        self.sql_configuration = sql_configuration

    @staticmethod
    def to_sqlserver_creating_instance(
            pandas_dataframe, sql_configuration, additional_static_data_dict=None, index=False, discover_data_type=False, use_existing=False,
            batch_size=10000, audit_date=datetime.utcnow(), csv_full_file_name=None):
        dataframe = Dataframe(sql_configuration)
        dataframe.to_sqlserver(pandas_dataframe, additional_static_data_dict, index, discover_data_type, use_existing, batch_size,
                               audit_date, csv_full_file_name)
        return dataframe

    def to_sqlserver(self, pandas_dataframe, additional_static_data_dict=None, index=False, discover_data_type=False,
                     use_existing=False, batch_size=10000, audit_date=datetime.utcnow(),
                     csv_full_file_name=None):
        # set properties first
        self.pandas_dataframe = \
            Dataframe._conform_pandas_dataframe(
                Dataframe._add_audit_fields(
                    pandas_dataframe=Dataframe._add_additional_static_data(
                        pandas_dataframe=pandas_dataframe,
                        additional_static_data_dict=additional_static_data_dict),
                    audit_date=audit_date,
                    csv_full_file_name=csv_full_file_name
                )
            )
        bcp_file_format = BcpFileFormat.create_bcp_format_file_instance_from_dataframe(self.pandas_dataframe, TempFile().temp_full_file_name)
        bcp_temp_csv_file = BcpTempCsvFile.write_df_to_csv_creating_instance(self.pandas_dataframe, bcp_file_format, index)

        SqlTable.create(self.sql_configuration, self.pandas_dataframe.dtypes.to_dict(), use_existing)
        BcpIn(self.sql_configuration, bcp_file_format, bcp_temp_csv_file, batch_size).execute()

        bcp_temp_csv_file.remove_file()
        bcp_file_format.remove_file()

    @staticmethod
    def to_csv_creating_instance(sql_configuration, csv_root_directory, separator="|", batch_size=10000):
        dataframe = Dataframe(sql_configuration)
        dataframe.to_csv(csv_root_directory, separator, batch_size)
        return dataframe

    def to_csv(self, csv_root_directory, separator="|", batch_size=10000):
        # set properties first
        self.csv_full_file_name = path.join(csv_root_directory, self.sql_configuration.server_name_clean,
                                            self.sql_configuration.database_name,
                                            f'{datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}__'
                                            f'{self.sql_configuration.schema_name}__'
                                            f'{self.sql_configuration.table_name}.csv')
        makedirs(path.dirname(self.csv_full_file_name), exist_ok=True)

        BcpOut(self.sql_configuration, self.csv_full_file_name, separator, batch_size).execute()
        Dataframe._add_header_row(
            header=SqlTable.get_column_names(self.sql_configuration, separator),
            data_full_file_name=self.csv_full_file_name
        )

    @staticmethod
    def _add_header_row(header, data_full_file_name):
        temp_file = data_full_file_name + ".tmp"
        with open(data_full_file_name, 'r') as old:
            with open(temp_file, 'w') as new:
                new.write(header + "\n")
                copyfileobj(old, new)
        remove(data_full_file_name)
        move(temp_file, data_full_file_name)

    @staticmethod
    def _conform_pandas_dataframe(pandas_dataframe):
        DfCleaner.clean_column_names_in_place(pandas_dataframe)
        return pandas_dataframe

    @staticmethod
    def _add_additional_static_data(pandas_dataframe, additional_static_data_dict):
        if additional_static_data_dict:
            for k, v in additional_static_data_dict.items():
                pandas_dataframe[k] = v
        return pandas_dataframe

    @staticmethod
    def _add_audit_fields(pandas_dataframe, audit_date, csv_full_file_name):
        pandas_dataframe['AUDIT_CREATE_UTC_DATETIME'] = audit_date
        if csv_full_file_name:
            pandas_dataframe['CSV_FILE_NAME'] = path.basename(csv_full_file_name)
        return pandas_dataframe
