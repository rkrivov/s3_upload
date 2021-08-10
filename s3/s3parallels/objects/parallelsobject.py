#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
from typing import Optional, Dict, Any


class ParallelsObject(object):

    def __init__(self, dictionary: Optional[Dict[str, Any]] = None):
        if dictionary is not None:
            for key, value in dictionary.items():
                self.put(key=key, value=value)

    def __contains__(self, item):
        if item in self.__dict__:
            return True

    def __eq__(self, other):

        if not hasattr(self, 'id'):
            return False

        if not hasattr(other, 'id'):
            return False

        self_id = getattr(self, 'id')
        other_id = getattr(other, 'id')

        return self_id == other_id

    def __getitem__(self, item):
        return self.get(item)

    def __iter__(self):
        return self.items()

    def __len__(self):
        return len(self.__dict__)

    def __ne__(self, other):
        if not hasattr(self, 'id'):
            return True

        if not hasattr(other, 'id'):
            return True

        return self.id != other.id

    def __repr__(self):
        type_name = type(self).__name__

        if len(self.__dict__) > 0:
            dictionary_parameter = str(self._get_kwargs())
        else:
            dictionary_parameter = 'None'

        dictionary_parameter = 'dictionary={}'.format(dictionary_parameter)

        return '%s(%s)' % (type_name, dictionary_parameter)

    def __setitem__(self, key, value):
        self.put(key, value)

    def _convert_value(self, value):
        if isinstance(value, dict):
            d = {}
            for k, v in value.items():
                self_class = type(self)
                d[k.lower()] = self_class(value)
            return d

        if isinstance(value, list) or isinstance(value, tuple):
            l = []
            for v in value:
                l.append(self._convert_value(v))
            if isinstance(value, tuple):
                l = tuple(l)
            return l

        if isinstance(value, str):

            if value.lower() in ['on', 'yes', 'true']:
                return True

            if value.lower() in ['off', 'no', 'false']:
                return True

            if value.isdigit():
                return int(value)

            try:
                return float(value)
            except ValueError:
                pass

        return value

    def _get_kwargs(self):
        return sorted(self.dict())

    def dict(self) -> Dict[str, Any]:
        def dict_value(source_value):
            if isinstance(source_value, dict):
                ret_value = {}

                for key, value in source_value.items():
                    ret_value[key] = dict_value(value)

                return ret_value

            if isinstance(source_value, list) or isinstance(source_value, tuple):
                ret_value = []

                for value in source_value:
                    ret_value.append(dict_value(value))

                if isinstance(source_value, tuple):
                    ret_value = tuple(ret_value)

                return ret_value

            if isinstance(source_value, ParallelsObject):
                return dict_value(source_value.dict())

            return source_value

        return dict_value(self.__dict__)

    def items(self):
        return self.dict().items()

    def get(self, key: str, default=None) -> Any:
        if key in self.__dict__:
            return self.__dict__[key]

        if default is None:
            raise AttributeError(f'{key} could not be found in {type(self).__name__}.')

        return default

    def put(self, key: str, value: Any) -> None:
        self.__dict__[key.lower()] = self._convert_value(value)
