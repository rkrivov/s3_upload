#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import time
from typing import AnyStr, Union

from utils.arguments import Arguments
from utils.convertors import size_to_human, time_to_short_string, speed_to_mbps, time_to_string
from utils.functions import print_progress_bar


class ProgressBar(object):

    def __init__(self, caption: AnyStr = '', total: Union[int, float] = 100.0):
        super(ProgressBar, self).__init__()

        self.iteration = 0
        self.total = total
        self.prefix = caption
        self.suffix = f'from {size_to_human(size=self.total, use_iec=False)}'
        self.current_step = 0
        self.start_time = time.time()
        self.speed = None

    def __call__(self, *args, **kwargs):
        arguments = Arguments(*args, **kwargs)
        iteration = arguments.get(argument_type=(int, float,), throw_error=False, default=0)
        total = arguments.get(argument_type=(int, float,), throw_error=False, default=self.total)

        if iteration is None:
            iteration = self.iteration

        if total is None:
            total = self.total

        iteration = max(0, iteration)
        iteration = min(iteration, total)

        self.iteration += iteration

        end_time = time.time()
        difference = end_time - self.start_time

        step = int(difference) // 10

        if self.iteration < total:
            if self.current_step != step:
                self.current_step = step

                self.suffix = f'/ {size_to_human(size=self.total, use_iec=False)}'
                speed = self.iteration / difference

                if speed > 0:
                    elapsed_time = (total - self.iteration) / speed if speed != 0 else 0
                    if elapsed_time > 0:
                        self.suffix += f' and {time_to_short_string(elapsed_time)} left'
                        self.suffix += f' ({speed_to_mbps(speed)})'
                    else:
                        self.suffix += f' {speed_to_mbps(speed)}'

            print_progress_bar(iteration=self.iteration,
                               total=total,
                               prefix=self.prefix,
                               suffix=self.suffix,
                               length=80)
        else:

            print_progress_bar(iteration=self.iteration,
                               total=total,
                               prefix=self.prefix,
                               suffix=f' The process was completed in {time_to_string(time_value=difference, human=True, use_milliseconds=True)}',
                               length=80)
