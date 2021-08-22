#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow


class MetaSingleton(type):
    _instances = {}
    class_logger = None

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(MetaSingleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
