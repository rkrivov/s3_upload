#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow

import contextlib
from common import consts
import io
import logging
import os
import requests

from botocore.response import StreamingBody
from io import BytesIO
from requests import Session
from s3.object import S3Object
from typing import Any, AnyStr, Callable, Text, Union, Optional, Tuple, Dict, List
from urllib3.response import HTTPResponse
from common.urn import Urn


DEFAULT_CHUCK_SIZE = consts.KBYTES * 64
DEFAULT_ENCODING = consts.ENCODER

FileWrapperCallbackType = Callable[[Any], Any]
PathType = Union[str, bytes, Text]
FileType = Union[str, bytes, int]


class FileNotSupportedError(Exception):
    """File not supported error."""

@contextlib.contextmanager
def reset(buffer):
    original_position = buffer.tell()
    buffer.seek(0, 2)
    yield
    buffer.seek(original_position, 0)


class FileWrapperBytesIO(BytesIO):
    def __init__(self, buffer: Any = None, encoding: AnyStr = "utf-8"):
        buffer = encode_with(buffer, encoding=encoding)
        super(FileWrapperBytesIO, self).__init__(buffer)

    def _get_end(self) -> int:
        current_pos = self.tell()
        self.seek(0, 2)
        length = self.tell()
        self.seek(current_pos, 0)
        return length

    @property
    def len(self) -> int:
        length = self._get_end()
        return length - self.tell()

    def append(self, buffer: Union[bytes, memoryview]):
        with reset(self):
            written = self.write(buffer)
        return written

    def smart_truncate(self):
        to_be_read = total_len(self)
        already_read = self._get_end() - to_be_read

        if already_read >= to_be_read:
            old_bytes = self.read()
            self.seek(0, 0)
            self.truncate()
            self.write(old_bytes)
            self.seek(0, 0)  # We want to be at the beginning


def is_callable(obj):
    if hasattr(obj, '__call__'):
        return True

    return callable(obj)


def calling_method(callable_object, *args, **kwargs):

    if is_callable(callable_object):
        return callable_object(*args, **kwargs)

    return None

def encode_with(buffer: AnyStr, encoding: AnyStr) -> Any:

    if buffer is not None:

        if isinstance(buffer, memoryview):
            return buffer

        if isinstance(buffer, io.BytesIO):
            return buffer

        if not isinstance(buffer, bytes):
            return buffer.encode(encoding)

    return buffer


def is_readable(data: Any, encoding: AnyStr = DEFAULT_ENCODING) -> Any:
    if hasattr(data, 'read'):
        return data

    return FileWrapperBytesIO(data, encoding)


def read_data_from(data: Union[bytes, memoryview, io.IOBase], length: int = -1) -> Any:
    if hasattr(data, 'read'):
        return data.read(length)
    return data


def total_len(o):
    if hasattr(o, '__len__'):
        return len(o)

    if hasattr(o, 'len'):
        return o.len

    if hasattr(o, 'headers'):
        value = o.headers.get('content-length', None)
        if value is not None and value.isdigit():
            return int(value)

    if hasattr(o, 'fileno'):
        try:
            fileno = o.fileno()
        except io.UnsupportedOperation:
            pass
        else:
            return os.fstat(fileno).st_size

    if hasattr(o, 'getvalue'):
        # e.g. BytesIO, cStringIO.StringIO
        return len(o.getvalue())


def get_current_position(fobj: Any) -> int:
    if fobj:
        if hasattr(fobj, "tell"):
            if is_callable(fobj.tell):
                return calling_method(fobj.tell)
    return 0


def next_chunk(buffer: Union[bytes, memoryview, io.IOBase], chunk_size: int) -> Optional[Union[bytes, memoryview]]:
    current_position = get_current_position(fobj=buffer)
    total_size = total_len(buffer)

    if chunk_size <= 0:
        chunk_size = 2 << 20

    if total_size > current_position:
        chunk_size = min(chunk_size, total_size - current_position)

        buffer = read_data_from(buffer, chunk_size)

        if buffer:
            return memoryview(buffer)

    return None


def to_list(fields: Union[Tuple[Any], List[Any], Dict[Any, AnyStr]]) -> List[Any]:
    if hasattr(fields, 'items'):
        return list(fields.items())
    return list(fields)


def is_closed(obj: Any) -> bool:
    if obj:
        if hasattr(obj, "closed"):
            if is_callable(obj.closed):
                return obj.closed()
            else:
                return obj.closed
    return True


def close(obj: Any) -> None:
    if obj:
        if hasattr(obj, "close"):
            if is_callable(obj.close):
                obj.close()


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


class FileFromURLWrapper(object):
    left_bytes: int

    def __init__(self,
                 file_url: Union[str, bytes] = None,
                 session: Session = None,
                 response: requests.Response = None,
                 chuck_size: int = None,
                 callback: Callable[[Any], Any] = None):

        if not file_url and not response and not session:
            raise AttributeError("You've to pass one of three parameters")

        self.content_length = None
        self.bytes_read = 0

        if not requests:
            self.session = session or requests.Session()
            requested_file = response or self._request_for_file(file_url)

            self.response = requested_file
            self.raw_data = _get_raw_data(requested_file)
        else:
            self.response = response
            self.raw_data = _get_raw_data(response)

        self.total_size = self.len
        self.left_bytes = self.total_size
        self.chunk_size = min(self.total_size, chuck_size if chuck_size else DEFAULT_CHUCK_SIZE)
        self.chunk = self._next_chunk()

        self.set_callback(callback)

    def _next_chunk(self):

        try:
            if isinstance(self.raw_data, S3Object):
                return FileWrapperBytesIO(buffer=self.raw_data.next())
            else:
                return FileWrapperBytesIO(buffer=next_chunk(self.raw_data, chunk_size=self.chunk_size))
        except Exception as ex:
            logging.error(ex, stack_info=False)

        return None

    def close(self):
        if not is_closed(self.chunk):
            close(self.chunk)
            self.chunk = None

        if not is_closed(self.raw_data):
            close(self.chunk)
            self.chunk = None

        if not is_closed(self.response):
            close(self.response)
            self.response = None

        self.raw_data = None

    @property
    def file_url(self):
        if self.response is not None:
            if hasattr(self.response, "url"):
                urn = Urn(self.response.url)
                return urn.quote()
        return ""

    @property
    def len(self):
        if self.content_length is None:
            if hasattr(self.response, 'content_length'):
                content_length = self.response.content_length
            elif hasattr(self.response, '_content_length'):
                content_length = self.response._content_length
            else:
                content_length = self.response.headers.get('content-length', None)

            if content_length is None:
                error_msg = (
                    f"Data from provided URL {self.file_url} is not supported. Lack of "
                    "content-length Header in requested file response."
                )
                raise FileNotSupportedError(error_msg)
            elif not content_length.isdigit():
                error_msg = (
                    f"Data from provided URL {self.file_url} is not supported. content-length"
                    " header value is not a digit."
                )
                raise FileNotSupportedError(error_msg)

            self.content_length = int(content_length)

        return self.content_length

    def _request_for_file(self, file_url):
        """Make call for file under provided URL."""
        response = self.session.get(file_url, stream=True)
        _ = self.len
        return response

    def read(self, chunk_size: int = -1) -> Optional[Union[bytes, memoryview]]:
        """Read file in chunks."""
        if self.chunk is None:
            self.chunk = self._next_chunk()
            if self.chunk is None:
                return None

        if self.chunk.len == 0:
            self.chunk = self._next_chunk()
            read_size = self.chunk.len if self.chunk else 0
            if read_size == 0:
                return None

        if chunk_size == -1:
            chunk_size = 8192

        try:
            # chunk_size = min(chunk_size, self.chunk.len)
            chunk_size = min(max(chunk_size, DEFAULT_CHUCK_SIZE), self.chunk.len)

            chunk = read_data_from(self.chunk, length=chunk_size)

            read_size = total_len(chunk) if chunk else 0
            self.left_bytes -= read_size
            self.bytes_read += read_size

            if self.callback:
                self.callback(self)

            return chunk
        except Exception as ex:
            logging.error(ex, stack_info=False)

        return None

    def set_callback(self, callback: Callable[[Any], Any]):
        self.callback = callback

    @staticmethod
    def create(body: Union[StreamingBody, HTTPResponse],
               chuck_size: int = None,
            callback: Callable[[Any], Any] = None):
        response = None
        if body is not None:
            if hasattr(body, 'raw_stream'):
                response = body.raw_stream
            else:
                response = body

        if response is None:
            raise ValueError(f'The body incorrect.')

        file_wrapper = FileFromURLWrapper(response=response, chuck_size=chuck_size, callback=callback)

        return file_wrapper