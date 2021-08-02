#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
from typing import AnyStr, Union

from common.utils import get_terminal_width, get_parameter, print_progress_bar


class ProgressBar(object):
    iteration = 0
    total = 100
    prefix = ''

    def __init__(self, caption: AnyStr = '', total: Union[int, float] = 100.0):
        super(ProgressBar, self).__init__()

        self.total = total
        self.prefix = caption

    def __call__(self, *args, **kwargs):
        iteration = get_parameter(args, 1, argument_type=(int, float), throw_error=False)
        total = get_parameter(args, 1, argument_type=(int, float), throw_error=False)

        if iteration is not None:
            iteration = self.iteration

        if total is None:
            total = self.total

        iteration = max(0, iteration)
        iteration = min(iteration, total)

        print_progress_bar(iteration=iteration, total=total, prefix=self.prefix)

    def __new__(cls, *args, **kwargs):
        instance = super(ProgressBar, cls).__new__(*args, **kwargs)
        return instance
