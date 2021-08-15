#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow

import asyncio

class AsyncObjectHandler(object):

    def __init__(self):
        self.__main_event_loop = asyncio.get_event_loop()

    def __del__(self):
        if hasattr(self, '__operations_task_list'):
            if len(self.__operations_task_list) > 0:
                if self.__main_event_loop.is_running():
                    self.__main_event_loop.run_forever()
                if not self.__main_event_loop.is_closed():
                    self.__main_event_loop.close()
                self.__operations_task_list.clear()
            del self.__operations_task_list
        del self.__main_event_loop

    def append_task_to_list(self, future):
        if not hasattr(self, '__operations_task_list'):
            self.__operations_task_list = []

        self.__operations_task_list.append(future)

    def run_task_list(self):
        if hasattr(self, '__operations_task_list'):
            if len(self.__operations_task_list) > 0:
                self.__main_event_loop.run_until_complete(asyncio.wait(self.__operations_task_list))
                self.__operations_task_list.clear()
            del self.__operations_task_list