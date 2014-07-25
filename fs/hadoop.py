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

import fnmatch
import os
import re

from fs.errors import ParentDirectoryMissingError, ResourceNotFoundError, \
    DestinationExistsError, RemoveRootError
from fs.base import FS
from fs.path import recursepath, normpath, pathcombine
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

        for uri, info in self.ilistdirinfo(path, **kwargs):
            yield uri

    def ilistdirinfo(self, path="./", wildcard=None, full=False,
                     absolute=False, dirs_only=False, files_only=False):
        """Generator yielding paths and path info under a given path.

        List all files and directories within a given path. This method returns
        a generator of tuples (path, info) where `info` is a dictionary
        of path attributes.

        :param path: root of the path to list
        :param wildcard: filter paths that match this wildcard
        :param full: If True, return full paths (relative to the root)
        :param absolute: If True, return paths beginning with /
        :param dirs_only: If True, only retrieve directories
        :param files_only: If True, only retrieve files

        :returns: a generator yielding (path, info) tuples, where `info` is a
                  dictionary of path attributes.

        :raises `fs.errors.ResourceNotFoundError`: If the path is not found
        :raises `fs.errors.ResourceInvalidError`: If the path exists, but is
                not a directory
        """

        if wildcard is not None and not callable(wildcard):
            wildcard_re = re.compile(fnmatch.translate(wildcard))
            wildcard = lambda fn: bool(wildcard_re.match(fn))

        if dirs_only and files_only:
            raise ValueError("dirs_only and files_only cannot both be True")

        for uri, info in self._list(self._base(path)):

            if dirs_only and info["type"] != self.TYPE_DIRECTORY:
                continue

            if files_only and info["type"] != self.TYPE_FILE:
                continue

            if wildcard is not None and not wildcard(uri):
                continue

            if full or absolute:
                uri = pathcombine(path, uri).lstrip("./")

            if absolute:
                uri = "/" + uri

            yield uri, info

    def listdir(self, *args, **kwargs):
        """
        Return the results of `ilistdir` as a list rather than a generator.
        """

        return list(self.ilistdir(*args, **kwargs))

    def listdirinfo(self, *args, **kwargs):
        """Results of `ilistdirinfo` as a list rather than a generator."""

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
        """Remove a file from the filesystem.

        :param path: path of the resource to remove
        :type path: string

        :returns: None

        :raises `fs.errors.ParentDirectoryMissingError`: if an intermediate
            directory is missing
        :raises `fs.errors.ResourceInvalidError`: if the path is a directory
        :raises `fs.errors.ResourceNotFoundError`: if the path does not exist
        """

        hdfs_path = self._base(path)
        info = self._status(hdfs_path, safe=False)
        if info.get("type") == self.TYPE_DIRECTORY:
            raise ResourceInvalidError
        self.client.delete_file_dir(hdfs_path, recursive=False)

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
        """Fetch information about the file or directory at the given path.

        The contents of the returned dictionary are whatever the WebHDFS API
        returns, with some fields also mapped to more commonly used keys.

        :param path: a path to retrieve information for

        :returns: a dictionary with path information

        :raises: ResourceNotFoundError if the path does not exist
        """

        return self._status(self._base(path), safe=False)

    def _base(self, path):
        """
        Return the given path, but prefixed with the filesystem base.
        """

        path = path.lstrip("/")
        if self.base:
            return normpath(os.path.join(self.base, path)).lstrip("/")
        return normpath(path)

    def _status(self, hdfs_path, safe=True):
        """Return the FileStatus object for a given hdfs_path.

        :param hdfs_path: absolute remote path
        :param safe: whether or not exceptions should be raised

        :returns: a dictionary with the FileStatus object with a few
            fields also mapped to more commonly used keys.

        :raises: ResourceNotFoundError if the path does not exist
            if `safe` is False
        """

        try:
            response = self.client.get_file_dir_status(hdfs_path.lstrip("/"))
            status = response["FileStatus"]
            status["size"] = status["length"]
            status["accessed_time"] = status["accessTime"]
            status["modified_time"] = status["modificationTime"]
            return status
        except pywebhdfs.errors.FileNotFound:
            if safe:
                return {}
            raise ResourceNotFoundError

    def _list(self, path):
        """
        List all files within a given directory.
        """

        ls = self.client.list_dir(path.lstrip("/"))
        return [
            (p["pathSuffix"], p)
            for p in ls.get("FileStatuses", {}).get("FileStatus", [])
        ]
