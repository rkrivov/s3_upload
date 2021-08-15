#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import hashlib
import math
import os
import threading
import time
from queue import Queue, Full, Empty
from threading import Thread
from typing import Tuple, Any, Optional, Dict, List

from utils.app_logger import get_logger
from utils.consts import MAX_QUEQUE_SIZE, CPU_COUNT, BUFFER_SIZE, FILE_SIZE_LIMIT, THREADS_IN_POOL, THREAD_TIMEOUT
from utils.convertors import append_end_path_sep, remove_start_path_sep
from utils.files import get_file_size

logger = get_logger(__name__)


class ScanFileThread(Thread):

    def __init__(self, parent: object, name=None):
        self.parent = parent
        self._is_running = False
        self._is_terminated = False
        self._lock = threading.Lock()
        super(ScanFileThread, self).__init__(name=name)

    @property
    def queue(self) -> Queue:
        q = None
        if hasattr(self.parent, 'queue'):
            q = self.parent.queue
        return q

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def is_terminated(self) -> bool:
        return self._is_terminated

    @is_terminated.setter
    def is_terminated(self, value: bool):
        with self._lock:
            self._is_terminated = value

    def _scan_file(self, file: Tuple[str, int, int]):
        file_name, file_offset, block_size = file

        file_size = get_file_size(file_name=file_name)
        block_size = min(block_size, file_size - file_offset)
        part_index = file_offset // BUFFER_SIZE

        if file_size > FILE_SIZE_LIMIT:
            num_of_parts = int(math.ceil(float(file_size) / float(BUFFER_SIZE)))
        else:
            num_of_parts = 1

        try:
            if block_size > 0:
                with open(file_name, mode='rb') as file:
                    file.seek(file_offset)
                    block = file.read(block_size)
                    if file_size > FILE_SIZE_LIMIT:
                        hash_digest = hashlib.md5(block).digest()
                        self.append(file_name, part_index, hash_digest)
                    else:
                        hash_object = hashlib.new('md5')
                        hash_object.update(block)
                        self.append(file_name, part_index, hash_object.hexdigest())
        finally:
            percent = (file_offset + block_size) / float(file_size) if file_size > 0 else 0
            logger.debug(
                f"[FINISH] "
                f"file_name={file_name}, "
                f"file_offset={file_offset} ({percent:.2%}), "
                f"block_size={block_size}, "
                f"part_index={part_index} / {num_of_parts}, "
                f"thread={self.name}"
            )

    def get(self) -> Optional[Any]:
        while not self._is_terminated:
            try:
                file = self.queue.get(block=False)

                return file
            except Empty:
                time.sleep(THREAD_TIMEOUT)
        return None

    def start(self) -> None:
        self._is_running = False
        self._is_terminated = False
        super(ScanFileThread, self).start()

    def run(self) -> None:
        logger.debug(f"{self.name} is running.")
        try:
            while not self._is_terminated:
                file = self.get()

                if file is not None:
                    self._is_running = True
                    try:
                        self._scan_file(file)
                        self.queue.task_done()
                    finally:
                        self._is_running = False
        finally:
            logger.debug(f"{self.name} is stopped.")

    def append(self, file_name: str, part_index: int, hash_digest: Any = None):
        append_func = None
        if hasattr(self.parent, 'append'):
            append_func = self.parent.append
        if append_func is not None:
            append_func(file_name, part_index, hash_digest)


class ScanFolderException(threading.ThreadError):
    pass


class ScanFolder(Thread):

    def __init__(self):
        self._queue = Queue(maxsize=MAX_QUEQUE_SIZE)
        self._threads = []

        for ix in range(THREADS_IN_POOL):
            thread = ScanFileThread(parent=self, name=f'ScanFileThread_{ix + 1}')
            thread.daemon = True
            self._threads.append(thread)

        self._lock = threading.Lock()
        self._files_list = {}
        self._folder = None
        self._is_terminated = False
        self._is_running = False
        super(ScanFolder, self).__init__(name="ScanFolder")

    def __len__(self):
        return len(self._files_list)

    def __iter__(self):
        return self._files_list.items()

    @property
    def queue(self) -> Queue:
        return self._queue

    @property
    def files_list(self) -> Dict[str, List[Any]]:
        return self._files_list

    @property
    def folder(self) -> str:
        return self._folder

    @folder.setter
    def folder(self, value: str):
        self._folder = value

    def _start_all_threads(self):
        for thread in self._threads:
            logger.debug(f"{thread=}")
            if not thread.is_alive():
                thread.start()

    def _stop_suspended_theads(self):
        for thread in self._threads:
            logger.debug(f"{thread=}")
            if thread.is_alive() and not thread.is_running:
                thread.is_terminated = True
                thread.join()

    def _stop_all_threads(self):
        for thread in self._threads:
            logger.debug(f"{thread=}")
            if thread.is_alive():
                thread.is_terminated = True
                thread.join()

    def _execute(self, folder: str):
        logger.debug(f"{folder=}")
        if len(self._files_list) > 0:
            self._files_list.clear()

        for root, dirs, files in os.walk(folder):
            for file in files:
                if not file.startswith('.') and not file.startswith('~'):
                    if self._is_terminated:
                        return

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
                            self.put_to_queue((local_file_path, file_offset, block_size,))
                            file_offset += block_size
                    else:
                        self.put_to_queue((local_file_path, 0, file_size,))

    def _wait_all_task_done(self):
        while self._queue.unfinished_tasks > 0:
            time.sleep(2.25)
            running_threads = [thread for thread in self._threads if thread.is_alive() and thread.is_running]
            if self._queue.unfinished_tasks < len(running_threads):
                self._stop_suspended_theads()

        self._queue.join()

        if not self.queue.unfinished_tasks:
            self._stop_all_threads()

    def _wait_until_running(self):
        while self._is_running:
            time.sleep(THREAD_TIMEOUT)

    def append(self, file_name: str, part_index: int, hash_digest: Any = None):
        with self._lock:
            blocks = self._files_list.setdefault(file_name, [])
            while len(blocks) < (part_index + 1):
                blocks.append(None)
            blocks[part_index] = hash_digest

    def put_to_queue(self, file: Tuple[str, int, int]) -> None:
        while True:
            try:
                self.queue.put(file, block=False)
                return
            except Full:
                time.sleep(THREAD_TIMEOUT)

    def get_hash(self, file_name: str) -> Optional[str]:
        if file_name in self._files_list:
            blocks = self._files_list.get(file_name, None)

            if blocks is not None:
                if len(blocks) == 1 and isinstance(blocks[0], str):
                    return blocks[0]
                elif len(blocks) > 1:
                    incorrect_elements = [item for item in blocks if item is None]
                    if len(incorrect_elements) > 0:
                        raise ScanFolderException('Something is wrong...')
                    hash = hashlib.md5(b''.join(blocks)).hexdigest()
                    return f"{hash}-{len(blocks)}"

        return None

    def run(self):
        logger.debug(f"{self._folder=}")
        if self._folder is not None:
            self._start_all_threads()
            try:
                logger.debug(f"Start scanning folder {self._folder}...")
                try:
                    self._is_running = True
                    self._execute(self._folder)

                    self._wait_all_task_done()
                finally:
                    logger.debug(f"Scan folder {self._folder} completed.")
                    self._is_running = False
                    self._folder = None
            finally:
                self._stop_all_threads()

    def start(self) -> None:
        self._is_terminated = False
        self._is_running = False
        super(ScanFolder, self).start()

    def __call__(self, folder: str):
        self._wait_until_running()
        self.folder = folder
        # self.start()
        self.run()

    def wait(self):
        self._wait_until_running()
        # self.join()
