#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow

import io
import logging
import os
from typing import Any, AnyStr, Callable, Text, Union, Optional

from botocore.response import StreamingBody
from urllib3.response import HTTPResponse

from s3.s3base.s3object import S3Object
from utils import consts
from utils.filewrapperbytesio import FileWrapperBytesIO
from utils.functions import total_len, is_closed, close, next_chunk, read_data_from

DEFAULT_CHUCK_SIZE = consts.KBYTES * 64
DEFAULT_ENCODING = consts.ENCODER

FileWrapperCallbackType = Callable[[Any], Any]
PathType = Union[str, bytes, Text]
FileType = Union[str, bytes, int]


def is_readable(data: Any, encoding: AnyStr = consts.ENCODER) -> Any:
    if hasattr(data, 'read'):
        return data

    return FileWrapperBytesIO(data, encoding)


class FileWrapper(object):

    def __init__(self, file: Union[io.TextIOBase, io.BufferedIOBase, io.FileIO], chunk_size: int = DEFAULT_CHUCK_SIZE,
                 encoding: AnyStr = DEFAULT_ENCODING,
                 callback: Optional[FileWrapperCallbackType] = None):
        self.fd = file
        self.encoding = encoding
        self.total_size = total_len(self.fd)
        self.left_bytes = self.total_size
        self.chunk_size = min(self.total_size, chunk_size)
        self.callback = callback
        self.bytes_read = 0
        self.chunk = self._next_chunk()

    def close(self):
        if not is_closed(self.chunk):
            close(self.chunk)
            self.chunk = None

        if not is_closed(self.fd):
            close(self.fd)
            self.fd = None

    def _next_chunk(self) -> Optional[FileWrapperBytesIO]:
        try:
            return FileWrapperBytesIO(buffer=next_chunk(buffer=self.fd, chunk_size=self.chunk_size),
                                      encoding=self.encoding)
        except Exception as ex:
            logging.error(ex, stack_info=True)

        return None

    @property
    def len(self):
        return total_len(self.fd)

    def read(self, length: int = -1) -> Optional[Union[bytes, memoryview]]:
        buffer = None

        try:
            if not self.chunk.len:
                self.chunk = self._next_chunk()

                if not self.chunk:
                    return None

            if self.chunk.len:
                # length = min(max(length, self.chunk_size), self.chunk.len)
                length = min(max(length, DEFAULT_CHUCK_SIZE), self.chunk.len)
                buffer = read_data_from(self.chunk, length=length)
                read_bytes = total_len(buffer) if buffer else 0

                self.left_bytes -= read_bytes
                self.bytes_read += read_bytes

            if self.callback:
                self.callback(self)

        except Exception as ex:
            logging.error(ex, stack_info=True)
            return None

        return buffer

    def write(self, buffer: Union[bytes, memoryview]):
        self.fd.write(buffer)

    def __iter__(self) -> Optional[Union[bytes, memoryview]]:
        yield self.read()

    @staticmethod
    def create(file: FileType, mode: str = "r", chuck_size=DEFAULT_CHUCK_SIZE, encoding: AnyStr = DEFAULT_ENCODING,
               callback: Optional[FileWrapperCallbackType] = None):
        is_binary: bool = 'b' in mode.lower()

        try:
            file_size = os.path.getsize(file)
        except:
            file_size = 0

        if is_binary:
            if file_size > 0:
                chuck_size = min(file_size, chuck_size)
            fd = open(file=file, mode=mode, buffering=chuck_size)
        else:
            fd = open(file=file, mode=mode, encoding=encoding)

        return FileWrapper(file=fd, chunk_size=chuck_size, encoding=encoding, callback=callback)


def _get_raw_data(data: Any) -> Optional[Union[FileWrapperBytesIO, HTTPResponse]]:
    if isinstance(data, S3Object):
        return data

    if isinstance(data, StreamingBody):
        return data

    if hasattr(data, "raw_data"):
        return data.raw_data

    if hasattr(data, "raw"):
        return data.raw

    return None
