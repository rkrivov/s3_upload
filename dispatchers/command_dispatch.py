#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
from functools import wraps
from typing import Callable

from dispatchers.command_info import CommandInfo
from utils.app_logger import get_logger
from utils.functions import is_callable

logger = get_logger(__name__)


class CommandDispatchException(Exception):
    pass


class CommandDispatch(object):
    def __init__(self, shortname, longname):
        self.shortname = shortname
        self.longname = longname

    def __call__(self, func):
        @wraps(func)
        def wrapped_func(wself, *args, **kwargs):
            logger.debug(f'wrapped_func {func.__name__}, args:{args}, kwargs: {args}')
            func(wself, *args, **kwargs)

        ci = CommandInfo
        ci.commands += [ci(shortname=self.shortname, longname=self.longname, func=func)]
        return wrapped_func

    @staticmethod
    def func(name) -> Callable:
        logger.debug(f'CommandDispatch.func({name})')

        for ci in CommandInfo.commands:
            if ci.shortname == name or ci.longname == name:
                return ci.func

        raise RuntimeError('unknown command')

    @classmethod
    def execute(cls, name: str, *args, **kwargs):
        func = cls.func(name=name)

        if func is not None and is_callable(func):
            result = func(*args, **kwargs)
            return result
        # try:
        #     result = func(*args, **kwargs)
        #     return result
        # except:
        #     raise CommandDispatchException(f'The function {name}() run_process failed.') from None

        raise CommandDispatchException(f'The function {name}() could not be found.')
