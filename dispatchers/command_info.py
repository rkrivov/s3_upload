#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
from typing import Callable

from utils.app_logger import get_logger

logger = get_logger(__name__)


class CommandInfo(object):
    commands = []

    def __init__(self, shortname: str, longname: str, func: Callable):
        logger.debug(f'CommandInfo: shortname={shortname}, longname={longname}, func={func}')
        self.shortname = shortname
        self.longname = longname
        self.func = func
