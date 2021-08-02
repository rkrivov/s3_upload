# coding: utf-8

#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow

import logging
import os
import re
import sys
from logging import Formatter
from logging.handlers import RotatingFileHandler

from common import consts
from common.utils import append_end_path_sep


LOG_FILE_FMT = "%(levelname)s - %(asctime)s - %(name)s - %(module)s.%(funcName)s - %(threadName)s - %(message)s in %(pathname)s:%(lineno)d"
LOG_CONSOLE_FMT = "[%(levelname)s]: %(message)s"


class DecorateStreamHandler(logging.StreamHandler):
    def __init__(self, stream=None):
        class DecorageFormatter(logging.Formatter):

            def __init__(self, fmt=None, datefmt=None, style='%', validate=True):
                super().__init__(fmt=fmt, datefmt=datefmt, style=style, validate=validate)
                self.__colors = {}
                # self.__colors[logging.getLevelName(logging.DEBUG)] = (consts.DGRAY+ consts.BOLD, consts.NBOLD + consts.DEF)
                # self.__colors[logging.getLevelName(logging.INFO)] = (consts.CYAN + consts.BOLD, consts.NBOLD + consts.DEF)
                self.__colors[logging.getLevelName(logging.WARNING)] = (
                consts.YELLOW + consts.BOLD, consts.NBOLD + consts.DEF)
                self.__colors[logging.getLevelName(logging.ERROR)] = (
                consts.RED + consts.BOLD, consts.NBOLD + consts.DEF)
                self.__colors[logging.getLevelName(logging.CRITICAL)] = (
                    consts.CYAN + consts.BGRED, consts.BGDEF + consts.DEF)
                self.__colors[logging.getLevelName(logging.FATAL)] = (
                    consts.YELLOW + consts.BGRED + consts.BOLD,
                    consts.NBOLD + consts.BGDEF + consts.DEF)

            def format(self, record: logging.LogRecord):
                msg = super().format(record)

                msg = re.sub(r"(Except(ion)?)\s+(\w+)\s*\:\s*(.*)$",
                             consts.RED + r"\1" " " + consts.BOLD + r"\3" + consts.NBOLD + consts.DEF + ": " + consts.BLACK + r"\4" + consts.DEF,
                             msg, flags=re.I or re.X)

                if record.levelname in self.__colors:
                    color_begin, color_end = self.__colors[record.levelname]
                    msg = color_begin + record.levelname + color_end + ': ' + msg

                return '\r' + msg

        super().__init__(stream=stream)

        self.setFormatter(DecorageFormatter())


class CustomFilter(logging.Filter):
    pass


class CustomFilterOnlyFor(CustomFilter):
    def __init__(self, level , name=''):
        self._level = level
        super(CustomFilterOnlyFor, self).__init__(name)

    def filter(self, record):
        if record.levelname == logging.getLevelName(self._level):
            return super(CustomFilter, self).filter(record=record)
        return False

def get_logger(name: str):
    log_dir = os.path.dirname(sys.argv[0])
    log_dir = append_end_path_sep(log_dir)
    log_dir = os.path.join(log_dir, 'log/')

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    console_handler = DecorateStreamHandler()
    console_handler.setLevel(logging.INFO)


    exp_info_file_handler = RotatingFileHandler('{}info.log'.format(log_dir), maxBytes=consts.MAX_FILE_LOG_SIZE,
                                                backupCount=consts.MAX_LOG_BACKUP_COUNT)
    exp_info_file_handler.setLevel(logging.INFO)
    exp_info_file_handler.addFilter(CustomFilterOnlyFor(level=logging.INFO))
    exp_info_file_handler.setFormatter(Formatter(LOG_FILE_FMT))


    exp_debug_file_handler = RotatingFileHandler('{}debug.log'.format(log_dir), maxBytes=consts.MAX_FILE_LOG_SIZE,
                                                 backupCount=consts.MAX_LOG_BACKUP_COUNT)
    exp_debug_file_handler.setLevel(logging.DEBUG)
    exp_debug_file_handler.addFilter(CustomFilterOnlyFor(level=logging.DEBUG))
    exp_debug_file_handler.setFormatter(Formatter(LOG_FILE_FMT))


    exp_errors_file_handler = RotatingFileHandler('{}errors.log'.format(log_dir), maxBytes=consts.MAX_FILE_LOG_SIZE,
                                                  backupCount=consts.MAX_LOG_BACKUP_COUNT)
    exp_errors_file_handler.setLevel(logging.WARNING)
    exp_errors_file_handler.addFilter(CustomFilter())
    exp_errors_file_handler.setFormatter(Formatter(LOG_FILE_FMT))

    logger.addHandler(console_handler)
    logger.addHandler(exp_info_file_handler)
    logger.addHandler(exp_debug_file_handler)
    logger.addHandler(exp_errors_file_handler)

    return logger
