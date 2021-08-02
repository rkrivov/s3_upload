#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow

import argparse
import time
import uuid
from functools import wraps
from typing import Callable

from common import app_logger
from common import utils
import configure
from s3.parallels import S3ParallelsOperation, S3ParallelsBackup, S3ParallelsRestore
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


class CommandInfo(object):
    commands = []

    def __init__(self, shortname: str, longname: str, func: Callable):
        logger.debug(f'CommandInfo: shortname={shortname}, longname={longname}, func={func}')
        self.shortname = shortname
        self.longname = longname
        self.func = func


class CommandDispatch(object):
    def __init__(self, shortname, longname):
        self.shortname = shortname
        self.longname = longname

    def __call__(self, func):
        @wraps(func)
        def wrapped_func(wself, *args, **kwargs):
            logger.debug(f'wrapped_func {func.__name__}, args:{args}, kwargs: {args}')
            func(wself, *args, **kwargs)

        ci = CommandInfo
        ci.commands += [ci(shortname=self.shortname, longname=self.longname, func=func)]
        return wrapped_func

    @staticmethod
    def func(name) -> Callable:
        logger.debug(f'CommandDispatch.func({name})')

        for ci in CommandInfo.commands:
            if ci.shortname == name or ci.longname == name:
                return ci.func

        raise RuntimeError('unknown command')

    @classmethod
    def execute(cls, name: str, *args, **kwargs):
        func = cls.func(name=name)
        result = func(*args, **kwargs)
        return result


@CommandDispatch(shortname='backup', longname='do_backup_vm')
def do_backup_vm(*args, **kwargs):
    logger.debug(f'Start backup Virtual Machines: ({args}) ({kwargs})')
    S3ParallelsBackup.execute(*args, **kwargs)


@CommandDispatch(shortname='restore', longname='do_restore_vm')
def do_restore_vm(*args, **kwargs):
    logger.debug(f'Start restore Virtual Machines: ({args}) ({kwargs})')
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
                            help='Backup without check operations.')

        parser.add_argument('--pack',
                            dest='pack_vm', default=argparse.SUPPRESS,
                            action='store_true',
                            help='Packing Virtual Machine before backup')

        args = parser.parse_args()

        force = False
        packing = False

        if 'force' in args:
            force = args.force

        if 'pack_vm' in args:
            packing = args.pack_vm

        CommandDispatch.execute(args.operation,
                                bucket=args.bucket_name,
                                force=force,
                                pack=packing,
                                vm_id=args.vm_id if 'vm_id' in args else None,
                                show_progress=_progress_visible_2_bool[args.progress_bar])
    except KeyboardInterrupt:
        logger.warning('User terminate program!')
    # except Exception as exception:
    #     logger.critical(f"Exception {type(exception).__name__} with message \"{exception}\" is not caught", stack_info=True)
    finally:
        end_time = time.time()
        logger.info(
            f'Elapsed time is {utils.time_to_string(end_time - start_time, use_milliseconds=True, human=True)}.'
        )


if __name__ == '__main__':
    main()
