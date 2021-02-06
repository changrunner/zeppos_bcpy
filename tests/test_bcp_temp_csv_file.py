import unittest
import os
from zeppos_bcpy.bcp_temp_csv_file import BcpTempCsvFile
from zeppos_bcpy.bcp_file_format import BcpFileFormat
import pandas as pd
from pandas._testing import assert_frame_equal
from zeppos_logging.app_logger import AppLogger


class TestTheProjectMethods(unittest.TestCase):
    def test_write_df_to_csv_creating_instance_method(self):
        AppLogger.configure_and_get_logger(
            logger_name='test_simple')
        AppLogger.set_debug_level()

        temp_filename = os.path.join(os.path.dirname(__file__), 'temp', 'test_file1')
        os.makedirs(os.path.dirname(temp_filename), exist_ok=True)
        df = pd.DataFrame({'seconds': ["3600"], 'minutes': ["10"]}, columns=['seconds', 'minutes'])
        bcp_ff = BcpFileFormat(df, temp_filename)
        bcp_temp = BcpTempCsvFile.write_df_to_csv_creating_instance(df, bcp_ff)

        assert_frame_equal(df, bcp_temp.pandas_dataframe)
        self.assertEqual(bcp_ff, bcp_temp.bcp_file_format)
        self.assertEqual(f"{temp_filename}.csv", bcp_temp.csv_full_file_name)
        self.assertEqual(False, bcp_temp.use_index)
        self.assertEqual(True, os.path.exists(f"{temp_filename}.csv"))

        bcp_temp.remove_file()

    def test_remove_file_method(self):
        temp_filename = os.path.join(os.path.dirname(__file__), 'temp', 'test_file1')
        os.makedirs(os.path.dirname(temp_filename), exist_ok=True)
        with open(f"{temp_filename}.csv", 'w') as fl:
            fl.write("test")
        bcp_ff = BcpFileFormat(None, temp_filename)
        bcp_temp = BcpTempCsvFile(None, bcp_ff)
        bcp_temp.remove_file()
        self.assertEqual(False, os.path.exists(temp_filename))


if __name__ == '__main__':
    unittest.main()