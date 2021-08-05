#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
from typing import AnyStr, Any, Union, Tuple, List

from common.utils import get_parameter


class Arguments(object):

    def __init__(self, *args, **kwargs):
        self.__args = args
        self.__kwargs = kwargs
        self.__current_index = 0
        self.__arguments_count = len(self.__args)

    def get(self, argument_type: Union[Any, Union[Tuple[Any], List[Any]]] = None, throw_error: bool = True, default: Any = None) -> Any:
        if self.__current_index < self.__arguments_count:
            result = get_parameter(
                self.__args,
                self.__current_index,
                argument_type=argument_type,
                throw_error=throw_error)

            if result is not None:
                self.__current_index += 1

            return result
        return default

    def get_all_for(self, argument_type: Union[Any, Union[Tuple[Any], List[Any]]] = None):
        ret = None
        index = self.__current_index
        while index < self.__arguments_count:
            if argument_type is not None:
                if isinstance(self.__args[index], argument_type):
                    if ret is None:
                        ret = []
                    ret.append(self.__args[index])
            else:
                if ret is None:
                    ret = []
                ret.append(self.__args[index])
            index += 1

        if ret is not None:
            ret = tuple(ret)

        return ret

    def pop(self, key: AnyStr, default: Any = None) -> Any:
        return self.__kwargs.pop(key, default=default)

    def __str__(self):
        return f"{self.__args}"
