#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import errno
import hashlib
import json
import os
from typing import Union, AnyStr, Optional, Any

from s3.s3base.s3object import S3Object
from utils import consts, filewrapper
from utils.convertors import encode_string
from utils.filewrapper import FileWrapper
from utils.functions import print_progress_bar, is_callable


def _get_file_name(o) -> Optional[str]:
    if o is not None:
        if not isinstance(o, str):
            if hasattr(o, 'fd'):
                fd = o.fd
                if hasattr(fd, 'name'):
                    return fd.name
                if hasattr(fd, '__str__'):
                    return str(fd)
            if hasattr(o, '__str__'):
                return str(o)
    return o


def _calc_hash(file_object,
               length: Union[int, float],
               hash_name: AnyStr = consts.MD5_ENCODER_NAME,
               as_base64: bool = False,
               chuck_size: int = consts.BUFFER_SIZE,
               show_progress: bool = True) -> str:
    if not hasattr(file_object, 'read'):
        raise Exception(f"The file object <{type(file_object).__name__}> is unreadable.")

    if not is_callable(file_object.read):
        raise Exception(f"The file object <{type(file_object).__name__}> is unreadable.")

    if not hasattr(file_object, 'close'):
        raise Exception(f"The file object <{type(file_object).__name__}> doesn't close.")

    if not is_callable(file_object.close):
        raise Exception(f"The file object <{type(file_object).__name__}> doesn't close.")

    o_hash = hashlib.new(name=hash_name)
    converted_bytes = 0

    fname = _get_file_name(file_object)
    chuck_size = min(length, chuck_size)

    while file_object.left_bytes > 0:
        buffer = file_object.read(chuck_size)

        if not buffer:
            raise IOError(errno.EIO, os.strerror(errno.EIO), fname)

        o_hash.update(buffer)

        if show_progress:
            converted_bytes += len(buffer)
            print_progress_bar(iteration=converted_bytes,
                               total=length,
                               prefix=f'Calculate hash {hash_name.upper()}')

    result = o_hash.hexdigest()
    return encode_string(result) if as_base64 else result


def _calc_file_hash(file_object: Union[str, bytes, int],
                    hash_name: AnyStr = consts.MD5_ENCODER_NAME,
                    as_base64: bool = False,
                    chuck_size: int = consts.BUFFER_SIZE,
                    show_progress: bool = True) -> str:
    file_wrapper = FileWrapper.create(file=file_object, mode="rb", chuck_size=consts.BUFFER_SIZE,
                                      encoding=consts.ENCODER)

    return _calc_hash(file_object=file_wrapper, length=get_file_size(file_wrapper), hash_name=hash_name,
                      as_base64=as_base64, chuck_size=chuck_size,
                      show_progress=show_progress)


def get_file_size(file_name: Any) -> int:
    if isinstance(file_name, str):
        try:
            return os.stat(file_name).st_size
        except Exception:
            return 0
    else:
        return filewrapper.total_len(file_name)


def calc_file_hash(file_object: Union[str, bytes, int, S3Object],
                   hash_name: AnyStr = consts.MD5_ENCODER_NAME,
                   as_base64: bool = False,
                   chuck_size: int = consts.BUFFER_SIZE,
                   show_progress: bool = True) -> str:
    if isinstance(file_object, S3Object):
        return _calc_hash(
            file_object=file_object,
            length=len(file_object),
            hash_name=hash_name,
            as_base64=as_base64,
            chuck_size=chuck_size,
            show_progress=show_progress
        )
    else:
        return _calc_file_hash(
            file_object,
            hash_name=hash_name,
            as_base64=as_base64,
            chuck_size=chuck_size,
            show_progress=show_progress
        )


def get_file_etag(file_name: Union[str, bytes, int], show_progress: bool = True) -> str:
    if isinstance(file_name, bytes):
        file_name = file_name.decode(json.detect_encoding(file_name))

    file_size = get_file_size(file_name=file_name)
    part_size = min(file_size, consts.BUFFER_SIZE)

    if file_size > consts.FILE_SIZE_LIMIT:
        num_of_parts = (file_size // consts.BUFFER_SIZE)
        if (file_size % consts.BUFFER_SIZE) != 0:
            num_of_parts += 1
    else:
        num_of_parts = 1

    # md5_hash = hashlib.new('md5')
    md5_digests = []

    if file_size > 0:
        converted_bytes = 0
        with open(file_name, 'rb') as f:
            for chunk in iter(lambda: f.read(part_size), b''):
                if show_progress:
                    converted_bytes += len(chunk)
                    print_progress_bar(iteration=converted_bytes,
                                       total=file_size,
                                       prefix=f'Calculate MD5 etag')

                md5_digests.append(hashlib.md5(chunk).digest())

    etag = hashlib.md5(b''.join(md5_digests)).hexdigest()

    if num_of_parts > 1:
        etag = etag + '-' + str(num_of_parts)

    return etag
