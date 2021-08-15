#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import json
from typing import AnyStr, Any, Union, Tuple, List

from utils.functions import get_parameter


class Arguments(object):

    def __init__(self, *args, **kwargs):
        self.__args = args
        self.__kwargs = kwargs
        self.__current_index = 0
        self.__arguments_count = len(self.__args)

    def get(self,
            argument_type: Union[Any, Union[Tuple[Any], List[Any]]] = None,
            throw_error: bool = True,
            default: Any = None) -> Any:
        if 0 <= self.__current_index < self.__arguments_count:
            result = get_parameter(
                self.__args,
                argument_index=self.__current_index,
                argument_type=argument_type,
                throw_error=throw_error)

            if result is not None:
                self.__current_index += 1

            return result
        return default

    def get_by_index(self,
                     argument_index: AnyStr,
                     argument_type: Union[Any, Union[Tuple[Any], List[Any]]] = None,
                     throw_on_error: bool = True,
                     default: Any = None) -> Any:

        return get_parameter(self.__args,
                             argument_index=argument_index,
                             argument_type=argument_type,
                             throw_error=throw_on_error,
                             default=default)

    def get_by_name(self,
                    argument_name: AnyStr,
                    argument_type: Union[Any, Union[Tuple[Any], List[Any]]] = None,
                    throw_on_error: bool = True,
                    default: Any = None) -> Any:

        ret_value = default

        if isinstance(argument_name, bytes):
            argument_name = argument_name.decode(json.detect_encoding(argument_name))

        if argument_name in self.__kwargs:
            ret_value = self.__kwargs.get(argument_name, default=default)

            if not isinstance(ret_value, argument_type):
                if throw_on_error:
                    if isinstance(argument_type, (tuple, list)):
                        types_list = ""
                        for i in range(len(argument_type)):
                            if i == len(argument_type) - 1:
                                if len(types_list) > 0:
                                    types_list += " or "
                            else:
                                if len(types_list) > 0:
                                    types_list += ", "
                                types_list += argument_type[i].__name__
                    else:
                        types_list = argument_type.__name__

                    raise TypeError(
                        f"Argument with name \"{argument_name}\" has incorrect argument_type. "
                        f"The argument_type must be {types_list}."
                    )
                ret_value = default
        else:
            if throw_on_error:
                raise AttributeError(f"Parameter with name \"{argument_name}\" could not be found.")

        return ret_value

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
