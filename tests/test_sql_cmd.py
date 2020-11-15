import unittest
from zeppos_bcpy.sql_cmd import SqlCmd


class TestTheProjectMethods(unittest.TestCase):
    def test_execute_method(self):
        self.assertEqual(SqlCmd.execute(r"localhost\sqlexpress", "master", "drop table if exists #tmp"), True)


if __name__ == '__main__':
    unittest.main()
