#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow

class Closer(object):
    def __init__(self, obj):
        self.obj = obj

    def __enter__(self):
        return self.obj

    def __exit__(self, exception_type, exception_val, trace):
        if self.obj is not None:
            try:
                self.obj.close()
            except AttributeError:
                pass

        return True
