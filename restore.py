#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow

import argparse
import time

from common import app_logger
from common import utils

import configure

from s3.restore import S3Restore
from s3.storage import S3Storage


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


def main():
    start_time = time.time()

    try:
        parser = argparse.ArgumentParser(prog='s3_upload')

        parser.add_argument('-p', '--progress',
                            dest='progress_bar', type=str,
                            choices=list(_progress_visible_2_bool.keys()),
                            default=_progress_visible,
                            help='visible or hidden progress bar (default: %(default)s)')

        parser.add_argument('-b', '--bucket',
                            dest='bucket', type=str, metavar='BUCKET',
                            help='name of S3 bucket (default: %(default)s)', default=configure.S3_BUCKET_NAME)

        parser.add_argument('-l', '--local-path',
                            dest='local_path', type=str, metavar='LOCAL PATH', required=True,
                            help="The local path for file recovery.")

        parser.add_argument('-r', '--remote-path',
                            dest='remote_path', type=str, metavar='REMOTE PATH', required=True,
                            help="The remote path for downloading the file.")

        parser.add_argument('-f', '--force',
                            dest='force', default=argparse.SUPPRESS,
                            action='store_true',
                            help='Backup without check operations.')


        args = parser.parse_args()

        logger.debug(vars(args))

        restore = S3Restore(bucket=args.bucket)
        try:
            restore.force = args.force if 'force' in args and args.force is not None else False
            restore.show_progress = _progress_visible_2_bool[args.progress_bar]

            restore.process(local_path=args.local_path, remote_path=args.remote_path)
        finally:
            del restore

    except KeyboardInterrupt:
        logger.warning('User terminate program!')
    except Exception as exception:
        logger.critical(f"Exception {type(exception).__name__} with message \"{exception}\" is not caught")
    finally:
        end_time = time.time()
        logger.info(
            f'Elapsed time is {utils.time_to_string(end_time - start_time, use_milliseconds=True, human=True)}.'
        )


if __name__ == '__main__':
    main()
