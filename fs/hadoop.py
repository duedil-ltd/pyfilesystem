"""
fs.hadoop
=========

This module provides the class `HadoopFS`, which implements the FS filesystem
interface for files stored on a deployment of the Hadoop Distributed
Filesystem.

Note: This filesystem is only compatible with Hadoop 2.0 or above.

The `pywebhdfs` module is used as a wrapper around the Hadoop 2 WebHDFS REST
API (http://hadoop.apache.org/docs/r1.0.4/webhdfs.html) and it's required you
enable WebHDFS for this filesystem to be usable.
"""

import fnmatch
import getpass
import json
import os
import re

import pywebhdfs.webhdfs
import pywebhdfs.errors

import fs.errors
from fs.base import FS
from fs.filelike import FileLikeBase
from fs.path import isprefix, normpath, pathcombine, recursepath

#
#  _   _           _                   _____ ____
# | | | | __ _  __| | ___   ___  _ __ |  ___/ ___|
# | |_| |/ _` |/ _` |/ _ \ / _ \| '_ \| |_  \___ \
# |  _  | (_| | (_| | (_) | (_) | |_) |  _|  ___) |
# |_| |_|\__,_|\__,_|\___/ \___/| .__/|_|   |____/
#                               |_|
#


def hdfs_errors(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except pywebhdfs.errors.PyWebHdfsException, e:
            try:
                error = json.loads(e.msg)
            except:
                error = {}
                pass

            err_msg = error.get("RemoteException", {}).get("message", "")
            exception_str = error.get("RemoteException", {}).get("exception")

            if "is non empty" in err_msg:
                raise fs.errors.DirectoryNotEmptyError(msg=e.msg)

            if "Parent path is not a directory" in err_msg:
                raise fs.errors.ResourceInvalidError(msg=e.msg)

            if exception_str in ("LeaseExpiredException",
                                 "AlreadyBeingCreatedException",
                                 "FileAlreadyExistsException"):
                raise fs.errors.ResourceLockedError(msg=e.msg)

            raise fs.errors.FSError(msg=e.msg)
    return wrapper


class HadoopFS(FS):

    TYPE_FILE = "FILE"
    TYPE_DIRECTORY = "DIRECTORY"

    @hdfs_errors
    def __init__(self, namenode, port="50070", base="/",
                 thread_synchronize=False):
        """Initialize an instance of the HadoopFS Filesystem class.

        Currently, only HDFS deployments with security off are supported, and
        the HDFS user name is set to the current user.

        :param namenode: The namenode hostname or IP
        :param port: The WebHDFS port (defaults to 50070)
        :param base: Base path to namespace this filesystem in. If the base
                     path does not exist, the corresponding directory is
                     created.
        :raises: FSError if the base path cannot be created
        """

        self.base = base
        self.client = pywebhdfs.webhdfs.PyWebHdfsClient(
            namenode,
            port=port,
            user_name=getpass.getuser()
        )

        # Create the HDFS base path if needed. This works as `mkdir -p`. If
        # the remote is an existing file, an exception is thrown. Any
        # authenticated errors will result in an exception here too.
        self.client.make_dir(base.lstrip("/"))

        super(HadoopFS, self).__init__(thread_synchronize=thread_synchronize)

    @hdfs_errors
    def open(self, path, mode='r', buffering=-1, encoding=None, errors=None,
             newline=None, line_buffering=False, **kwargs):
        """Open the given path as a file-like object.

        :param path: a path that should be opened
        :param mode: mode of file to open (supported: 'r', 'w', and 'a')
        :param buffering: size of buffer
        :param encoding: ignored
        :param errors: ignored
        :param newline: ignored
        :param line_buffering: ignored

        :returns: a file-like object

        :raises: ParentDirectoryMissingError if an intermediate directory is
            missing
        :raises: ResourceInvalidError if an intermediate directory is a file
        :raises: ResourceNotFoundError if the path is not found (read mode
            only)
        """

        is_dir, is_file = self._is_dir_file(self._base(path), safe=False)

        if is_dir:
            raise fs.errors.ResourceInvalidError

        if buffering == 1:
            self.buffersize = 4*1024
        elif buffering > 1:
            self.buffersize = buffering

        # Create the file if needed
        if not is_file:

            if "w" not in mode and "a" not in mode:
                raise fs.errors.ResourceNotFoundError

            if not self.isdir(os.path.dirname(path)):
                if self.isfile(os.path.dirname(path)):
                    raise fs.errors.ResourceInvalidError
                raise fs.errors.ParentDirectoryMissingError

            # Create file
            self.client.create_file(self._base(path), "")

        elif "w" in mode:
            # Truncate file
            self.client.create_file(self._base(path), "", overwrite=True)

        return _HadoopFileLike(self._base(path), self.client)

    def isfile(self, path):
        """Check if a path references a file.

        :param path: a path in the filesystem
        :rtype: bool
        """

        status = self._status(self._base(path))
        return status.get("type") == self.TYPE_FILE

    def isdir(self, path):
        """Check if a path references a directory.

        :param path: a path in the filesystem
        :rtype: bool
        """

        status = self._status(self._base(path))
        return status.get("type") == self.TYPE_DIRECTORY

    def _is_dir_file(self, hdfs_path, safe=True):
        """Determine if path is a directory or a file.

        For optimisation purposes, we make a single HTTP call to get the
        path status and determine if the given path is a directory and if
        it is a file.

        :param hdfs_path: absolute remote path
        :param safe: boolean indicating if exceptions should be thrown
        :returns: tuple of booleans (is_dir?, is_file?)
        """

        status = self._status(hdfs_path)
        return (
            status.get("type") == self.TYPE_DIRECTORY,
            status.get("type") == self.TYPE_FILE
        )

    def ilistdir(self, path="./", **kwargs):
        """Generator yielding the files and directories under a given path.

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

            info["is_dir"] = (info["type"] == self.TYPE_DIRECTORY)

            yield uri, info

    def listdir(self, *args, **kwargs):
        """Results of `ilistdir` as a list rather than a generator."""

        return list(self.ilistdir(*args, **kwargs))

    def listdirinfo(self, *args, **kwargs):
        """Results of `ilistdirinfo` as a list rather than a generator."""

        return list(self.ilistdirinfo(*args, **kwargs))

    @hdfs_errors
    def makedir(self, path, recursive=False, allow_recreate=False):
        """Make a directory on the filesystem.

        :param path: path of directory
        :type path: string
        :param recursive: if True, create intermediate directories if needed
        :type recursive: bool
        :param allow_recreate: if True, re-creating a directory is not an error
        :type allow_create: bool

        :raises `fs.errors.DestinationExistsError`: if the path is already a
            directory, and allow_recreate is False
        :raises `fs.errors.ParentDirectoryMissingError`: if a containing
            directory is missing and recursive is False
        :raises `fs.errors.ResourceInvalidError`: if a path is an existing file
        """

        is_dir, is_file = self._is_dir_file(self._base(path), safe=True)

        if is_dir:
            if not allow_recreate:
                raise fs.errors.DestinationExistsError(path)
            return True

        if is_file:
            raise fs.errors.ResourceInvalidError

        parent_dir, _ = os.path.split(path)

        if self.isdir(parent_dir) or recursive:
            self.client.make_dir(self._base(path))
        else:
            raise fs.errors.ParentDirectoryMissingError(parent_dir)

    @hdfs_errors
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
            raise fs.errors.ResourceInvalidError
        self.client.delete_file_dir(hdfs_path, recursive=False)

    @hdfs_errors
    def removedir(self, path, recursive=False, force=False):
        """Remove a directory from the filesystem.

        :param path: path of the directory to remove
        :type path: string
        :param recursive: if True, empty parent directories will be removed
        :type recursive: bool
        :param force: if True, any directory contents will be removed
        :type force: bool

        :raises `fs.errors.DirectoryNotEmptyError`: if the directory is not
            empty and force is False
        :raises `fs.errors.RemoveRootError`: if path is the filesystem root
        :raises `fs.errors.ResourceInvalidError`: if path is not a directory
        :raises `fs.errors.ResourceNotFoundError`: if path does not exist
        """

        hdfs_path = self._base(path)
        if path == "/":
            raise fs.errors.RemoveRootError(hdfs_path)

        info = self._status(hdfs_path, safe=False)

        if info.get("type") != self.TYPE_DIRECTORY:
            raise fs.errors.ResourceInvalidError

        self.client.delete_file_dir(hdfs_path, recursive=force)

        if recursive:
            for dir_path in recursepath(path, reverse=True):
                if dir_path != "/" and self.isdir(dir_path):
                    try:
                        self.client.delete_file_dir(self._base(dir_path),
                                                    recursive=False)
                    except pywebhdfs.errors.PyWebHdfsException:
                        pass

    @hdfs_errors
    def rename(self, src, dest):
        """Rename a file or directory.

        :param src: path to rename
        :type src: string
        :param dst: new name
        :type dst: string

        :raises ParentDirectoryMissingError: if a containing directory is
            missing
        :raises ResourceInvalidError: if the path or a parent path is not a
            directory or src is a parent of dst or one of src or dst is a dir
            and the other is not
        :raises ResourceNotFoundError: if the src path does not exist
        """

        src_hdfs_path = self._base(src)
        dest_hdfs_path = self._base(dest)

        src_is_dir, src_is_file = self._is_dir_file(src_hdfs_path, safe=False)
        dest_is_dir, dest_is_file = self._is_dir_file(dest_hdfs_path,
                                                      safe=False)

        if not self.isdir(os.path.dirname(dest)):
            raise fs.errors.ParentDirectoryMissingError

        is_dirs = (src_is_dir, dest_is_dir)
        dest_exists = (dest_is_dir or dest_is_file)
        if isprefix(src_hdfs_path, dest_hdfs_path) or \
                (dest_exists and any(is_dirs) and not all(is_dirs)):
            raise fs.errors.ResourceInvalidError

        self.client.rename_file_dir(src_hdfs_path, dest_hdfs_path)

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
        """Return the given path, but prefixed with the filesystem base.

        :param path: path in the filesystem instance
        :type path: string

        :returns: absolute remote path
        """

        path = path.lstrip("/")
        if self.base:
            return normpath(os.path.join(self.base, path)).lstrip("/")
        return normpath(path)

    @hdfs_errors
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
            raise fs.errors.ResourceNotFoundError

    @hdfs_errors
    def _list(self, hdfs_path):
        """Helper method to list all files within a given directory.

        :param hdfs_path: absolute remote path

        :returns: a generator yielding (path, info) tuples, where `info` is a
                  dictionary of path attributes.

        :raises: ResourceNotFoundError if the path does not exist
        :raises: ResourceInvalidError if the path is not a directory
        """

        try:
            ls = self.client.list_dir(hdfs_path.lstrip("/"))
        except pywebhdfs.errors.FileNotFound:
            raise fs.errors.ResourceNotFoundError

        # Figure out if we're performing a list operation on a file
        files = ls["FileStatuses"]["FileStatus"]
        if len(files) > 0:
            for fstatus in files:
                if fstatus["pathSuffix"]:
                    break
            else:
                raise fs.errors.ResourceInvalidError

        for p in ls.get("FileStatuses", {}).get("FileStatus", []):
            yield (p["pathSuffix"], p)


class _HadoopFileLike(FileLikeBase):

    def __init__(self, hdfs_path, client):
        """HDFS file-like object constructor.

        :param hdfs_path: absolute remote path
        :param client: `pywebhdfs.webhdfs.PyWebHdfsClient` instance
        """

        self.hdfs_path = hdfs_path
        self.client = client
        self.eof = False

        super(_HadoopFileLike, self).__init__()

    @hdfs_errors
    def _read(self, sizehint=-1):
        """Read entire contents of a HDFS file.

        :param sizehint: ignored

        :returns: string with the entire file contents or None if
            the file has already been read

        :raises: FSError if read was not successful
        """

        if self.eof:
            return None

        contents = self.client.read_file(self.hdfs_path, buffersize=self.buffersize)

        self.eof = True
        return contents

    @hdfs_errors
    def _write(self, data, flushing=False):
        """Write data to the HDFS file.

        :param data: string to be written
        :param flishing: ignored

        :returns: None

        :raises: FSError if write was not successful
        """

        self.client.append_file(self.hdfs_path, data, buffersize=self.buffersize)
        return None
