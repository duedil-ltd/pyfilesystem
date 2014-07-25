"""

  fs.tests.test_hadoop: TestCases for the HDFS Hadoop Filesystem

This test suite is skipped unless the following environment variables are
configured with valid values.

* PYFS_HADOOP_NAMENODE_ADDR
* PYFS_HADOOP_NAMENODE_PORT [default=50070]
* PYFS_HADOOP_NAMENODE_PATH [default="/"]

All tests will be executed within a subdirectory "pyfs-hadoop" for safety.

"""

import os
import unittest

from fs.tests import FSTestCases, ThreadingTestCases
from fs.path import *

try:
    from fs import hadoop
except ImportError:
    raise unittest.SkipTest("hadoop fs wasn't importable")


class TestHadoopFS(unittest.TestCase, FSTestCases, ThreadingTestCases):

    def setUp(self):
        namenode_host = os.environ.get("PYFS_HADOOP_NAMENODE_ADDR")
        namenode_port = os.environ.get("PYFS_HADOOP_NAMENODE_PORT", "50070")
        base_path = os.environ.get("PYFS_HADOOP_NAMENODE_PATH", "/")

        if not namenode_host or not namenode_port or not base_path:
            raise unittest.SkipTest("Skipping HDFS tests due to lack of config")

        self.fs = hadoop.HadoopFS(
            namenode=namenode_host,
            port=namenode_port,
            base=base_path
        )

    def tearDown(self):

        for dir_path in self.fs.ilistdir(dirs_only=True):
            if dir_path == "/":
                continue
            self.fs.removedir(dir_path, recursive=False, force=True)
        for file_path in self.fs.ilistdir(files_only=True):
            self.fs.remove(file_path)
        self.fs.close()

    @unittest.skip("HadoopFS does not support seek")
    def test_readwriteappendseek(self):
        pass

    @unittest.skip("HadoopFS does not support truncate")
    def test_truncate(self):
        pass

    @unittest.skip("HadoopFS does not support truncate")
    def test_truncate_to_larger_size(self):
        pass

    @unittest.skip("HadoopFS does not support seek")
    def test_write_past_end_of_file(self):
        pass
