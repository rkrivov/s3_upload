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
from utils.convertors import append_end_path_sep, remove_start_path_sep, size_to_human, time_to_string
from utils.files import get_file_size, get_file_size_async, get_file_etag_async, get_file_hash_async
from utils.functions import get_string

logger = get_logger(__name__)


class ScanFolderException(threading.ThreadError):
    pass


async def calculate_hash_async(file_name: str, show_progress: bool = True) -> Tuple[str, str]:
    hash_result = None

    file_size = await get_file_size_async(file_name=file_name)
    if file_size > 0:
        start_time = time.time()
        logger.debug("[START]: {} ({})".format(file_name, size_to_human(file_size)))
        try:
            if file_size >= FILE_SIZE_LIMIT:
                hash_result = await get_file_etag_async(file_name=file_name, show_progress=show_progress)
            else:
                hash_result = await get_file_hash_async(file_name=file_name, show_progress=show_progress)
        finally:
            logger.debug(
                "[FINISH]: {} ({}), hash = {}, Elapsed is {}".format(
                    file_name,
                    size_to_human(file_size),
                    hash_result,
                    time_to_string(time.time() - start_time, use_milliseconds=True)
                )
            )
    return file_name, hash_result


async def scan_folder_async(folder: str, *, loop=None) -> Dict[str, str]:
    result = {}
    tasks_list = []

    logger.debug(f"{folder=}")

    if loop is None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.get_event_loop()

    for root, dirs, files in os.walk(folder):
        for file in files:
            if not file.startswith('.') and not file.startswith('~'):
                local_file_path = os.path.join(
                    append_end_path_sep(root),
                    remove_start_path_sep(file)
                )

                task = asyncio.ensure_future(
                    calculate_hash_async(
                        file_name=local_file_path,
                        show_progress=False
                    ),
                    loop=loop
                )

                tasks_list.append(task)

    if len(tasks_list) > 0:
        for future in asyncio.as_completed(tasks_list):
            logger.debug(f"{future=}")

            file_name, file_hash = await future
            file_name = get_string(file_name)
            file_hash = get_string(file_hash)

            if file_hash is not None:
                result[file_name] = file_hash

    return result

