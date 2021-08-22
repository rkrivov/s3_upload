#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow

import asyncio
import time
from typing import Any

from utils.app_logger import get_logger
from utils.convertors import time_to_string

logger = get_logger(__name__)


class AsyncObjectHandler(object):

    def __init__(self):
        self._operations_task_list = []
        self._main_event_loop = asyncio.get_event_loop()
        self._main_event_loop.set_debug(False)

    def __del__(self):
        if hasattr(self, '_operations_task_list'):
            if len(self._operations_task_list) > 0:
                if self._main_event_loop.is_running():
                    self._main_event_loop.run_forever()
                if not self._main_event_loop.is_closed():
                    self._main_event_loop.close()
                self._operations_task_list.clear()
            del self._operations_task_list
        del self._main_event_loop

    @property
    def main_loop(self):
        return self._main_event_loop

    def append_task_to_list(self, coro_or_future: Any) -> None:
        future = asyncio.ensure_future(coro_or_future, loop=self._main_event_loop)
        self._operations_task_list.append(future)

    def run_task_list(self):
        if len(self._operations_task_list) > 0:
            start_time = time.time()
            try:
                logger.info(f"Execute {len(self._operations_task_list)} task(s)")
                self._main_event_loop.run_until_complete(asyncio.wait(self._operations_task_list))
            finally:
                logger.info(f"Completed elapsed {time_to_string(time.time() - start_time, human=True)}")
                self._operations_task_list.clear()
