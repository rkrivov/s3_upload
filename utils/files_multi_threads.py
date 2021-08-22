#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import asyncio
import aiofiles
import aiofiles.os
from aiofiles.threadpool import open as aioopen
import hashlib
import math
import os
import threading
import time
from queue import Queue, Full, Empty
from threading import Thread
from typing import Tuple, Any, Optional, Dict, List

from utils.app_logger import get_logger
from utils.consts import MAX_QUEQUE_SIZE, BUFFER_SIZE, FILE_SIZE_LIMIT, THREADS_IN_POOL, THREAD_TIMEOUT
from utils.convertors import append_end_path_sep, remove_start_path_sep
from utils.files import get_file_size

logger = get_logger(__name__)


class ScanFolderException(threading.ThreadError):
    pass


async def calculate_hash_async(file_name: str, file_offset: int = 0, block_size: int = BUFFER_SIZE):
    ret = None

    try:
        file_stat = os.stat(file_name)

        file_size = file_stat.st_size

        if file_size > 0:
            block_size = min(block_size, file_size - file_offset)

            if block_size > 0:
                logger.debug(f"{file_name}: {file_size=}")

                if file_size > FILE_SIZE_LIMIT:
                    num_of_parts = int(math.ceil(float(file_size) / float(BUFFER_SIZE)))
                    part_index = file_offset // BUFFER_SIZE
                else:
                    num_of_parts = 1
                    part_index = 0

                logger.debug(f"{file_name}: {part_index=} / {num_of_parts=}")

                if block_size > 0:
                    with open(file_name, mode='rb') as file:
                        file.seek(file_offset)
                        block = file.read(block_size)

                    if file_size > FILE_SIZE_LIMIT:
                        hash_digest = hashlib.md5(block).digest()
                        ret = file_name, part_index, hash_digest
                    else:
                        hash_object = hashlib.new('md5')
                        hash_object.update(block)
                        ret = file_name, part_index, hash_object.hexdigest()

                    logger.debug(f"{file_name}: {ret=}")
            else:
                hash_object = hashlib.new('md5')
                hash_object.update(b'')
                ret = file_name, 0, hash_object.hexdigest()
    finally:
        logger.debug(f"[FINISH] {ret=}")

    return ret


async def scan_folder_async(folder: str, *, loop=None) -> Dict[str, str]:
    result = {}
    tasks_list = []

    logger.debug(f"{folder=}")

    if loop is None:
        loop = asyncio.get_event_loop()
        loop.set_debug(True)

    for root, dirs, files in os.walk(folder):
        for file in files:
            if not file.startswith('.') and not file.startswith('~'):
                local_file_path = os.path.join(
                    append_end_path_sep(root),
                    remove_start_path_sep(file)
                )

                file_size = get_file_size(file_name=local_file_path)
                logger.debug(f"{local_file_path=}, {file_size=}")
                if file_size > FILE_SIZE_LIMIT:
                    file_offset = 0
                    while file_offset < file_size:
                        block_size = min(file_size - file_offset, BUFFER_SIZE)
                        task = asyncio.ensure_future(calculate_hash_async(local_file_path, file_offset, block_size), loop=loop)
                        tasks_list.append(task)
                        file_offset += block_size
                else:
                    task = asyncio.ensure_future(calculate_hash_async(local_file_path, 0, file_size), loop=loop)
                    tasks_list.append(task)

    if len(tasks_list) > 0:
        result_blocks = {}

        for future in asyncio.as_completed(tasks_list):
            res = await future

            logger.debug(f"{res=}")
            if res is not None and isinstance(res, tuple) and len(res) == 3:
                file_name, part_index, block = res

                blocks = result_blocks.setdefault(file_name, [])
                while len(blocks) <= part_index:
                    blocks.append(None)
                blocks[part_index] = block

        if len(result_blocks) > 0:
            for file_name, blocks in result_blocks.items():
                if blocks is not None:
                    if len(blocks) == 1 and isinstance(blocks[0], str):
                        result[file_name] = blocks[0]
                    elif len(blocks) > 1:
                        incorrect_elements = [item for item in blocks if item is None]
                        if len(incorrect_elements) > 0:
                            raise ScanFolderException('Something is wrong...')
                        result[file_name] = f"{hashlib.md5(b''.join(blocks)).hexdigest()}-{len(blocks)}"

    return result

