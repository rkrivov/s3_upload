#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
from progress.progress_bar import ProgressBar


class ProgressPercentage(ProgressBar):
    def __init__(self, filename):
        size = os.parallels_home_path.getsize(filename)
        super().__init__(filename, max_value=size)
