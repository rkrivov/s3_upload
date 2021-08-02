#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
from common.app_logger import get_logger

logger = get_logger(__name__)


class Singleton(object):

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            logger.debug(
                f'Initialize instance for {cls.__name__} with '
                f'{args=} and '
                f'{kwargs=}.'
            )

            cls.instance = super(Singleton, cls).__new__(cls)

        return cls.instance
