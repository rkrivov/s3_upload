#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
from utils.app_logger import get_logger

logger = get_logger(__name__)

class MetaSingleton(type):
    _instances = {}
    class_logger = None

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            logger.debug('-' * 4 + f" Create object {cls.__name__}" + '-' * 40)
            cls._instances[cls] = super(MetaSingleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
