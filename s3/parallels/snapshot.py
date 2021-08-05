#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
from typing import Optional, Dict, Any


class ParallelsSnapshot(object):
    def __init__(self, dictionary: Optional[Dict[str, Any]] = None):
        if dictionary is not None:
            for key, value in dictionary.items():
                self.__dict__[key] = value

    def __getitem__(self, item):
        return self.get(item)

    def __iter__(self):
        return self.__dict__.items()

    def __len__(self):
        return len(self.__dict__)

    def __setitem__(self, key, value):
        self.put(key, value)

    def get(self, key: str, default=None) -> Any:
        if key in self.__dict__:
            return self.__dict__[key]

        if default is None:
            raise AttributeError(f'{key} could not be found in {type(self).__name__}.')

        return default

    def put(self, key: str, value: Any) -> None:
        self.__dict__[key] = value

    def __eq__(self, other):

        if not hasattr(self, id):
            return False

        if not hasattr(other, 'id'):
            return False

        return self.id == other.id

    def __ne__(self, other):
        if not hasattr(self, id):
            return True

        if not hasattr(other, 'id'):
            return True

        return self.id != other.id

