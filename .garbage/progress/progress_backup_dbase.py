#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
from typing import Union

from progress.progress_bar import ProgressBar

from common.utils import get_parameter


class ProgressBackupDatabase(ProgressBar):
    def __init__(self, caption: Union[str, bytes]):
        super(ProgressBackupDatabase, self).__init__(caption=caption, max_value=100)

    def __call__(self, *args, **kwargs):
        value = get_parameter(args, 0, (int, float))
        min_value = get_parameter(args, 1, (int, float))
        max_value = get_parameter(args, 2, (int, float))

        self.min_value = min_value
        self.max_value = max_value
        self.value = value
        self._display()
