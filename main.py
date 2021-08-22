#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow

import argparse
import asyncio
import time
import uuid
from argparse import Namespace

from config import configure
from dispatchers.command_dispatch import CommandDispatch
from utils.app_logger import get_logger
from utils.consts import APP_NAME
from utils.convertors import time_to_string

logger = get_logger(__name__)

_progress_is_visible = 'visible'
_progress_is_hidden = 'hidden'

_progress_status_2_boolean = {
    _progress_is_visible: True,
    _progress_is_hidden: False
}

_boolean_2_progress_status = {
    True: _progress_is_visible,
    False: _progress_is_hidden
}


@CommandDispatch(shortname='backup', longname='do_backup_vm')
def do_backup_vm(*args, **kwargs):
    logger.debug(f'Start backup Virtual Machines({kwargs})')
    from s3.s3parallels.backup import S3ParallelsBackup
    S3ParallelsBackup.start(*args, **kwargs)


@CommandDispatch(shortname='restore', longname='do_restore_vm')
def do_restore_vm(*args, **kwargs):
    logger.debug(f'Start restore Virtual Machines ({kwargs})')
    from s3.s3parallels.restore import S3ParallelsRestore
    S3ParallelsRestore.start(*args, **kwargs)


# async def main():
def main():
    start_time = time.time()

    try:
        parser = argparse.ArgumentParser(prog=APP_NAME)

        parser.add_argument(
            'operation',
            type=str,
            metavar='OPERATION',
            choices=['backup', 'restore'],
            help="Operation with Parallels Virtual Machines (%(choices)s).")

        parser.add_argument('-p', '--progress',
                            dest='progress_bar', type=str,
                            choices=list(_progress_status_2_boolean.keys()),
                            default=_progress_is_visible,
                            help='visible or hidden progress bar (default: %(default)s)')

        parser.add_argument('-i', '--id',
                            dest='virtual_machine_id', type=uuid.UUID, metavar='UUID',
                            help='Parallels Virtual Machine Id.')

        parser.add_argument('-b', '--bucket-name',
                            dest='bucket_name', type=str, metavar='BUCKET',
                            help='name of S3 bucket (default: %(default)s)', default=configure.S3_BUCKET_NAME)

        parser.add_argument('-f', '--force',
                            dest='force', default=argparse.SUPPRESS,
                            action='store_true',
                            help='Backup without check operations_list.')

        parser.add_argument('--delete-removed',
                            dest='delete_removed', default=argparse.SUPPRESS,
                            action='store_true',
                            help='Delete removed files from S3 Storage Bucket.')

        args: Namespace = parser.parse_args()

        kwargs = {
            'backup_name': args.bucket_name,
            'show_progress': _progress_status_2_boolean[args.progress_bar]
        }

        if 'archive' in args:
            kwargs.update(archive=True)

        if 'force' in args:
            kwargs.update(force=True)

        if 'pack' in args:
            kwargs.update(pack=True)

        if 'delete_removed' in args:
            kwargs.update(delete_removed=True)

        if 'virtual_machine_id' in args:
            kwargs.update(virtual_machine_id=args.virtual_machine_id)

        CommandDispatch.execute(args.operation, **kwargs)
    except KeyboardInterrupt:
        logger.warning('User terminate program!')
    except Exception as exception:
        logger.exception(exception)
    finally:
        end_time = time.time()
        logger.info(
            f'Elapsed time is {time_to_string(end_time - start_time, use_milliseconds=True, human=True)}.'
        )


if __name__ == '__main__':
    # asyncio.run(main())
    main()
