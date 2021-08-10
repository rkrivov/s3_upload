#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow

import argparse
import time

from common import app_logger
from common.convertors import time_to_string

from config import configure
from s3.backup import S3Backup
from s3.s3storage import S3Storage

_progress_visible = 'visible'
_progress_hidden = 'hidden'

_progress_visible_2_bool = {
    _progress_visible: True,
    _progress_hidden: False
}

_bool_2_progress_visible = {
    True: _progress_visible,
    False: _progress_hidden
}

logger = app_logger.get_logger(__name__)


def process(
        storage: S3Storage,
        local_path: str = None,
        remote_path: str = None,
        show_progress: bool = True,
        all_files: bool = False):
    backup = S3Backup(bucket=storage.bucket)
    try:
        backup.storage = storage

        backup.show_progress = show_progress

        backup.run_process(local_path=local_path, remote_path=remote_path, all_files=all_files)
    finally:
        del backup


def main():
    start_time = time.time()

    try:
        parser = argparse.ArgumentParser(prog='s3_upload')

        parser.add_argument('--progress-bar', type=str, choices=list(_progress_visible_2_bool.keys()),
                            default=_progress_visible,
                            help='visible or hidden progress bar (default: %(default)s)')
        parser.add_argument('--all', dest='backup_all', help="Backuping all operations_list.")
        parser.add_argument('-b', dest='bucket', type=str, metavar='BUCKET',
                            help='name of S3 bucket (default: %(default)s)', default=configure.S3_BUCKET_NAME)
        parser.add_argument('--local-parallels_home_path', dest='local_path', type=str, metavar='LOCAL PATH',
                            required=True,
                            help="The local parallels_home_path for file recovery.")
        parser.add_argument('--remote-parallels_home_path', dest='remote_path', type=str, metavar='REMOTE PATH',
                            required=True,
                            help="The remote parallels_home_path for downloading the file.")

        args = parser.parse_args()

        logger.debug(vars(args))

        show_progress = True

        if args.progress_bar is not None:
            if args.progress_bar.lower() == 'visible':
                show_progress = True
            if args.progress_bar.lower() == 'hidden':
                show_progress = False

        if args.progress_bar is not None:
            if args.progress_bar in _progress_visible_2_bool:
                show_progress = _progress_visible_2_bool[args.progress_bar]

        s3storage = S3Storage(bucket=args.bucket)
        try:
            process(s3storage, args.local_path, args.remote_path, show_progress, args.backup_all is not None)
        finally:
            del s3storage

    except KeyboardInterrupt:
        logger.warning('User terminate program!')
    except Exception as exception:
        logger.critical(f"Exception {type(exception).__name__} with message \"{exception}\" is not caught")
    finally:
        end_time = time.time()
        logger.info(
            f'Elapsed time is {time_to_string(end_time - start_time, use_milliseconds=True, human=True)}.'
        )


if __name__ == '__main__':
    main()
