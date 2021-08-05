#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow

class ExecuteCommandException(Exception):
    def __init__(self, *args):
        super(ExecuteCommandException, self).__init__(*args)
