#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow

import asyncio
import time
from typing import Any

from utils.app_logger import get_logger
from utils.convertors import time_to_string

logger = get_logger(__name__)


class AsyncObjectHandler(object):
    _tasks_list = []
    _loop = None

    def __init__(self):
        logger.debug('-' * 4 + f" Constructor object {self.__class__.__name__}" + '-' * 40)

    def __del__(self):
        logger.debug('-' * 4 + f" Destroyer object {self.__class__.__name__}" + '-' * 40)
        if len(self.__class__._tasks_list) > 0:
            self.__class__._run_until_complete(fs=self.__class__._tasks_list)
            self.__class__._tasks_list.clear()

    @property
    def default_loop(self):
        return self.__class__._get_default_loop()

    def append_task_to_list(self, *, future, loop = None):
        self.__class__.append_task(coro_or_future=future, loop=loop)

    def run_tasks(self, *, loop=None):
        return self.__class__.run_task_list(loop=loop)

    @classmethod
    def _get_default_loop(cls):
        if cls._loop is None:
            try:
                logger.debug("Get running loop...")
                cls._loop = asyncio.get_running_loop()
            except RuntimeError:
                logger.debug("Get event loop...")
                cls._loop = asyncio.get_event_loop()

        return cls._loop

    @classmethod
    def append_task(cls, *, coro_or_future: Any, loop = None) -> None:
        logger.debug(
            "Adding the task {!r} to the list of the tasks of the class {!r} (loop: {!r}).".format(
                coro_or_future,
                cls,
                loop
            )
        )
        future = asyncio.ensure_future(coro_or_future, loop=loop)
        cls._tasks_list.append(future)

    @classmethod
    def _run_until_complete(cls, *, fs, loop = None):
        result = None

        if fs is not None and len(fs) > 0:
            if loop is None:
                loop = cls._get_default_loop()

            start_time = time.time()
            try:
                logger.debug(f"Execute {len(fs)} task(s)")
                result = loop.run_until_complete(asyncio.wait(fs))
            finally:
                logger.debug(f"Completed elapsed {time_to_string(time.time() - start_time, human=True)}")

        if result is not None:
            if isinstance(result, tuple) and len(result) == 2 and isinstance(result[1], set):
                result = result[0]

        return result

    @classmethod
    def run_task_list(cls, *, fs = None, loop = None):
        if fs is None:
            result = cls._run_until_complete(fs=cls._tasks_list,loop=loop)
            cls._tasks_list.clear()
        else:
            result = cls._run_until_complete(fs=fs, loop=loop)
        return result
