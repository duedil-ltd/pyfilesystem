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
import uuid

from fs.tests import FSTestCases, ThreadingTestCases
from fs.path import *

try:
    from fs import hadoop
except ImportError:
    raise unittest.SkipTest("hadoop fs wasn't importable")


class TestHadoopFS(unittest.TestCase, FSTestCases, ThreadingTestCases):

    def __init__(self, *args, **kwargs):

        self.namenode_host = os.environ.get("PYFS_HADOOP_NAMENODE_ADDR")
        self.namenode_port = os.environ.get("PYFS_HADOOP_NAMENODE_PORT",
                                            "50070")

        self.base_path = os.path.join(
            os.environ.get("PYFS_HADOOP_NAMENODE_PATH", "/"),
            "pyfstest-" + str(uuid.uuid4())
        )

        super(TestHadoopFS, self).__init__(*args, **kwargs)

    def setUp(self):

        if not self.namenode_host:
            raise unittest.SkipTest("Skipping HDFS tests (missing config)")

        self.fs = hadoop.HadoopFS(
            namenode=self.namenode_host,
            port=self.namenode_port,
            base=self.base_path
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
