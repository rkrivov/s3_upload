#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow

import argparse
import sys
import time
import uuid

from common import app_logger
from common import utils
from common.convertors import time_to_string
from config import configure
from dispatchers.command_dispatch import CommandDispatch

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


@CommandDispatch(shortname='backup', longname='do_backup_vm')
def do_backup_vm(*args, **kwargs):
    logger.debug(f'Start backup Virtual Machines: ({args}) ({kwargs})')

    from s3.parallels.backup import S3ParallelsBackup

    S3ParallelsBackup.execute(*args, **kwargs)


@CommandDispatch(shortname='restore', longname='do_restore_vm')
def do_restore_vm(*args, **kwargs):
    logger.debug(f'Start restore Virtual Machines: ({args}) ({kwargs})')

    from s3.parallels.restore import S3ParallelsRestore

    S3ParallelsRestore.execute(S3ParallelsRestore, *args, **kwargs)


def main():
    start_time = time.time()

    try:
        parser = argparse.ArgumentParser(prog='s3_upload')

        parser.add_argument(
            'operation',
            type=str,
            metavar='OPERATION',
            choices=['backup', 'restore'],
            help="Operation with Parallels Virtual Machines (%(choices)s).")

        parser.add_argument('-p', '--progress',
                            dest='progress_bar', type=str,
                            choices=list(_progress_visible_2_bool.keys()),
                            default=_progress_visible,
                            help='visible or hidden progress bar (default: %(default)s)')

        parser.add_argument('-i', '--id',
                            dest='vm_id', type=uuid.UUID, metavar='VMUUID',
                            help='Parallels Virtual Machine Id.')

        parser.add_argument('-b', '--bucket-name',
                            dest='bucket_name', type=str, metavar='BUCKET',
                            help='name of S3 bucket (default: %(default)s)', default=configure.S3_BUCKET_NAME)

        parser.add_argument('-f', '--force',
                            dest='force', default=argparse.SUPPRESS,
                            action='store_true',
                            help='Backup without check operations_list.')

        parser.add_argument('--archive',
                            dest='archive', default=argparse.SUPPRESS,
                            action='store_true',
                            help='Archiving Virtual Machive before backup.')
        parser.add_argument('--pack',
                            dest='pack', default=argparse.SUPPRESS,
                            action='store_true',
                            help='Packing Virtual Machine before backup')

        args = parser.parse_args()

        CommandDispatch.execute(args.operation,
                                bucket=args.bucket_name,
                                archive=args.archive if 'archive' in args else False,
                                force=args.force if 'force' in args else False,
                                pack=args.pack if 'pack' in args else False,
                                vm_id=args.vm_id if 'vm_id' in args else None,
                                show_progress=_progress_visible_2_bool[args.progress_bar])
    except KeyboardInterrupt:
        logger.warning('User terminate program!')
    # except Exception as exception:
    #     logger.critical(
    #         f"Exception {type(exception).__name__} with message \"{exception}\" is not caught",
    #         exc_info=1,
    #         stack_info=True,
    #         stacklevel=10
    #     )
    finally:
        end_time = time.time()
        logger.info(
            f'Elapsed time is {time_to_string(end_time - start_time, use_milliseconds=True, human=True)}.'
        )


if __name__ == '__main__':
    main()
