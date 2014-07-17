"""
fs.hadoop
=========

This module provides the class 'HadoopFS', which implements the FS filesystem
interface for files stored on a deployment of the Hadoop Distributed Filesystem.

Note: This filesystem is only compatible with Hadoop 2.0 or above.

The `pywebhdfs` module is used as a wrapper around the Hadoop 2 WebHDFS REST
API (http://hadoop.apache.org/docs/r1.0.4/webhdfs.html) and it's required you
enable WebHDFS for this filesystem to be usable.
"""

import os
from fs.errors import ParentDirectoryMissingError, ResourceNotFoundError, \
    DestinationExistsError, RemoveRootError
from fs.base import FS
from fs.path import recursepath, normpath
import pywebhdfs.webhdfs
import pywebhdfs.errors

#
#  _   _           _                   _____ ____
# | | | | __ _  __| | ___   ___  _ __ |  ___/ ___|
# | |_| |/ _` |/ _` |/ _ \ / _ \| '_ \| |_  \___ \
# |  _  | (_| | (_| | (_) | (_) | |_) |  _|  ___) |
# |_| |_|\__,_|\__,_|\___/ \___/| .__/|_|   |____/
#                               |_|
#


class HadoopFS(FS):

    TYPE_FILE = "FILE"
    TYPE_DIRECTORY = "DIRECTORY"

    def __init__(self, namenode, port="50070", base="/"):
        """
        Initialize an instance of the HadoopFS Filesystem class.

        :param namenode: The namenode hostname or IP
        :param port: The WebHDFS port (defaults to 50070)
        :param base: Base path to namespace this filesystem in
        """

        self.base = base
        self.client = pywebhdfs.webhdfs.PyWebHdfsClient(namenode, port=port)

        if base is not None and len(base) > 1:
            self.makedir(base, recursive=True, allow_recreate=True)

    def open(self, path, mode='r', buffering=-1, encoding=None, errors=None,
             newline=None, line_buffering=False, **kwargs):
        """
        Open the path with the given mode. Depending on the mode given, this
        method will use different techniques to work with the file.

        TODO: Detail those techniques.
        """

        path = self._base(path)

        # Truncate the file
        if "w" in mode:
            pass
        else:
            pass

        # Create the file if needed
        if not self.isfile(path):
            if "w" not in mode and "a" not in mode:
                raise  # Not found
            if not self.isdir(os.path.dirname(path)):
                raise  # Parent directory not found
            # Create the file (? needed ?)

        # Do some magic to support all the things.

    def isfile(self, path):
        """
        Is there a file at the given path?
        """

        status = self._status(self._base(path))
        return status.get("type") == self.TYPE_FILE

    def isdir(self, path):
        """
        Is there a directory at the given path?
        """

        status = self._status(self._base(path))
        return status.get("type") == self.TYPE_DIRECTORY

    def ilistdir(self, path="./", **kwargs):
        """
        List all files and directories at a path. This method returns a
        generator of matching paths as strings.
        """

        path = self._base(path)
        for uri, info in self.ilistdirinfo(path, **kwargs):
            yield uri

    def ilistdirinfo(self, path="./", **kwargs):
        """
        List all files and directories within a given path. This method returns
        a generator of tuples (path, info) where `info` is a dictionary
        of path attributes.
        """

        path = self._base(path)
        for uri, info in self._list(path):
            for matching_path in self._listdir_helper(path, [uri], **kwargs):
                yield matching_path, info

    def listdir(self, *args, **kwargs):
        """
        Return the results of `ilistdir` as a list rather than a generator.
        """

        return list(self.ilistdir(*args, **kwargs))

    def listdirinfo(self, *args, **kwargs):
        """
        Return the results of `ilistdirinfo` as a list rather than a generator.
        """

        return list(self.ilistdirinfo(*args, **kwargs))

    def makedir(self, path, recursive=False, allow_recreate=False):
        """
        Create a directory at the path given. If the `recursive` option is
        set to True, any directories missing in the path will also be created,
        not just the leaf.

        If `allow_recreate` is set to False, an exception will be raised when
        the leaf directory already exists.
        """

        path = self._base(path)
        if recursive:
            for dir_path in recursepath(path):
                directory = dir_path.lstrip("/")
                if len(directory) > 0:
                    try:
                        if not allow_recreate and self.isdir(dir_path):
                            raise DestinationExistsError(dir_path)
                    except ResourceNotFoundError:
                        self.client.make_dir(directory)
                    else:
                        self.client.make_dir(directory)
        else:
            parent_dir, _ = os.path.split(path)
            directory = path.lstrip("/")
            try:
                if not allow_recreate and self.isdir(directory):
                    raise DestinationExistsError(directory)
                self.client.make_dir(directory)
            except ResourceNotFoundError:
                raise ParentDirectoryMissingError(parent_dir)

    def remove(self, path):
        """
        Remove a file at the given path.
        """

        path = self._base(path)
        self.client.delete_file_dir(path, recursive=False)

    def removedir(self, path, recursive=False, force=False):
        """
        Remove a directory. If `recursive` is set to True, all directories
        within will also be removed. When False, an exception will be raised if
        a directory is encountered.

        The `force` argument is ignored in this implementation.
        """

        path = self._base(path)
        if path == "/":
            raise RemoveRootError(path)

        self.client.delete_file_dir(path, recursive=recursive)

    def rename(self, src, dest):
        """
        Rename a file or directory at the given path.
        """

        src_path = self._base(src)
        dest_path = self._base(dest)

        self.client.rename_file_dir(src_path, dest_path)

    def getinfo(self, path):
        """
        Return a dictionary of information about the file or directory at the
        given path.
        """

        return self._status(self._base(path))

    def _base(self, path):
        """
        Return the given path, but prefixed with the filesystem base.
        """

        if self.base:
            return normpath(os.path.join(self.base, path)).lstrip("/")
        return normpath(path).lstrip("/")

    def _status(self, path):
        """
        Return the FileStatus object for a given path.
        """

        try:
            status = self.client.get_file_dir_status(path.lstrip("/"))
            return status["FileStatus"]
        except pywebhdfs.errors.FileNotFound:
            raise ResourceNotFoundError(path)

    def _list(self, path):
        """
        List all files within a given directory.
        """

        ls = self.client.list_dir(path.lstrip("/"))
        return [
            (p["pathSuffix"], p)
            for p in ls.get("FileStatuses", {}).get("FileStatus", [])
        ]
