#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
from io import BytesIO
from typing import Any, AnyStr, Union

from utils.functions import encode_with, reset, total_len


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
