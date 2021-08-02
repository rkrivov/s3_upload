#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow

from common import filewrapper, consts
import io
import os

from botocore.response import StreamingBody
from typing import Dict, Union


class S3Object(object):
    def __init__(self, name: Union[str, bytes], response: Union[Dict, StreamingBody]):
        self._name = name
        self._tell = 0

        if isinstance(response, dict):
            response_body = response.get('Body')
        else:
            response_body = response

        if response_body is None:
            raise Exception('Incorrect response.')

        if not isinstance(response_body, StreamingBody):
            raise Exception('Incorrect response.')

        self._body = response_body

        if hasattr(self._body, '_raw_stream'):
            self._buffer_reader = io.BufferedReader(self._body._raw_stream, buffer_size=consts.BUFFER_SIZE)
        else:
            self._buffer_reader = None

    @property
    def body(self) -> StreamingBody:
        return self._body

    @property
    def content_length(self):
        return self._body._content_length

    @property
    def left_bytes(self):
        return self.len - self.tell

    @property
    def len(self):
        return len(self)

    @property
    def stream(self) -> io.BufferedReader:
        return self._buffer_reader

    @property
    def tell(self):
        return self.stream.tell() if self._buffer_reader is not None else self._body._amount_read

    def close(self):
        if self._buffer_reader is not None:
            self.stream.close()
        elif hasattr(self._body, 'close') and callable(self._body.close):
            self._body.close()

    def read(self, chuck_size = consts.BUFFER_SIZE):
        if self.stream is not None:
            chuck = self.stream.read(chuck_size)
        elif hasattr(self._body, 'read') and callable(self._body.read):
            chuck = self.body.read(chuck_size)
        else:
            return None

        if chuck is None:
            return None

        self._tell += filewrapper.total_len(chuck)

        return chuck

    def next(self, chuck_size = consts.BUFFER_SIZE):
        return self.read(chuck_size)

    def __len__(self):
        return int(self.content_length)

    def __str__(self):
        base_name = os.path.basename(self._name)
        dir_name = os.path.dirname(self._name)

        # if len(dir_name) > 10:
        #     dir_name = dir_name[:3] + '...' + dir_name[len(dir_name) - 3:]

        return os.path.join(dir_name, base_name)


class S3File(S3Object):
    pass