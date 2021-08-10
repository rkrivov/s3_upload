#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import os
from datetime import datetime
from functools import wraps
from typing import Any, Dict, Optional, Callable

from dispatchers.command_dispatch import CommandDispatchException
from dispatchers.command_info import CommandInfo
from s3.s3base.s3baseobject import OP_BACKUP, INFO_FIELD_NAME, INFO_LOCAL, INFO_REMOTE, INFO_OP, \
    OP_DELETE, OP_UPDATE, OP_INSERT
from s3.s3base.s3consts import VM_SNAPSHOT_DAYS_COUNT, VM_SNAPSHOT_COUNT
from s3.s3base.s3typing import VirtualMachineID
from s3.s3parallels.errors import VMError
from s3.s3parallels.objects.virtualmachine import ParallelsVirtualMachine
from s3.s3parallels.operation import S3ParallelsOperation
from utils.app_logger import get_logger
from utils.convertors import remove_start_path_sep, append_end_path_sep, size_to_human
from utils.files import get_file_size
from utils.functions import is_callable

logger = get_logger(__name__)


class S3ParallelsBackupDispatch(object):
    def __init__(self, name):
        self.name = name

    def __call__(self, func):
        @wraps(func)
        def wrapped_func(wself, *args, **kwargs):
            logger.debug(f'wrapped_func {wrapped_func.__name__}, args:{args}, kwargs: {args}')
            return func(wself, *args, **kwargs)

        ci = CommandInfo
        ci.commands.append(ci(shortname=self.name, longname=self.name, func=func))
        return wrapped_func

    @staticmethod
    def get_function(name) -> Callable:
        logger.debug(f'CommandDispatch.func({name})')

        for ci in CommandInfo.commands:
            if ci.shortname == name or ci.longname == name:
                return ci.func

        raise RuntimeError('unknown command')

    @classmethod
    def execute(cls, name: str, wself, *args, **kwargs):
        func = cls.get_function(name=name)

        if func is not None and is_callable(func):
            result = func(wself, *args, **kwargs)
            return result
            # try:
            #     result = func(*args, **kwargs)
            #     return result
            # except:
            #     raise CommandDispatchException(f'The function {name}() run_process failed.') from None

        raise CommandDispatchException(f'The function {name}() could not be found.')


class S3ParallelsBackup(S3ParallelsOperation):

    def _compare_files(self, file_info_remote: Dict[str, Any], file_info_local: Dict[str, Any]) -> Optional[str]:
        if self.force:
            return OP_BACKUP
        return super(S3ParallelsBackup, self)._compare_files(file_info_remote, file_info_local)

    @S3ParallelsBackupDispatch(name=OP_INSERT)
    def _do_insert_file(self, **kwargs):
        local_file_info: Dict[str, Any] = kwargs.pop('local_file_info', None)

        if local_file_info is None:
            raise VMError('Local file info can not be None.')

        local_file_name = local_file_info.get(INFO_FIELD_NAME, None)
        if local_file_name is None:
            raise VMError('Incorrect local file name.')

        remote_file_name = local_file_name
        if remote_file_name.startswith(self._local_path):
            remote_file_name = remote_file_name[len(self._local_path):]
            remote_file_name = remove_start_path_sep(remote_file_name)
            remote_file_name = os.path.join(append_end_path_sep(self._archive_path), remote_file_name)
        else:
            raise VMError('Incorrect local file name.')

        logger.info(f"Upload new file {remote_file_name} ({size_to_human(get_file_size(local_file_name))})")
        self.storage.upload_file(local_file_path=local_file_name, remote_file_path=remote_file_name, show_progress=self.show_progress)

    @S3ParallelsBackupDispatch(name=OP_UPDATE)
    def _do_update_file(self, **kwargs):
        local_file_info: Dict[str, Any] = kwargs.pop('local_file_info', None)
        remote_file_info: Dict[str, Any] = kwargs.pop('remote_file_info', None)

        if local_file_info is None:
            raise VMError('Local file info can not be None.')

        if remote_file_info is None:
            raise VMError('Remote file info can not be None.')

        local_file_name = local_file_info.get(INFO_FIELD_NAME, None)
        if local_file_name is None:
            raise VMError('Incorrect local file name.')

        remote_file_name = remote_file_info.get(INFO_FIELD_NAME, None)
        if remote_file_name is None:
            raise VMError('Incorrect remote file name.')

        logger.info(f"Upload changed file {remote_file_name} ({size_to_human(get_file_size(local_file_name))})")
        self.storage.upload_file(local_file_path=local_file_name, remote_file_path=remote_file_name, show_progress=self.show_progress)

    @S3ParallelsBackupDispatch(name=OP_DELETE)
    def _do_delete_file(self, **kwargs):
        remote_file_info: Dict[str, Any] = kwargs.pop('remote_file_info', None)

        if remote_file_info is None:
            raise VMError('Remote file info can not be None.')

        remote_file_name = remote_file_info.get(INFO_FIELD_NAME, None)
        if remote_file_name is None:
            raise VMError('Incorrect remote file name.')

        logger.info(f'Delete removed file {remote_file_name}')
        self.storage.delete_file(remote_file_path=remote_file_name)

    def _do_file_operation(self,
                           operation_id: str,
                           local_file_info: Dict[str, Any],
                           remote_file_info: Dict[str, Any]):

        if operation_id in [OP_DELETE, OP_UPDATE, OP_INSERT]:
            S3ParallelsBackupDispatch.execute(operation_id, self,
                                              local_file_info=local_file_info,
                                              remote_file_info=remote_file_info)
        else:
            raise VMError(f'Incorrect operation ("{operation_id}").')

    def operation(self, files: Dict[str, Any]):
        for _, operation_info in files.items():
            local_file = operation_info.get(INFO_LOCAL)
            remote_file = operation_info.get(INFO_REMOTE)
            operation_id = operation_info.get(INFO_OP)

            self._do_file_operation(operation_id=operation_id, local_file_info=local_file, remote_file_info=remote_file)

    def _process_with_virtual_machine(self, virtual_machine: ParallelsVirtualMachine):
        virtual_machine_id = virtual_machine.id

        if virtual_machine_id is not None:
            self._update_snapshots(virtual_machine_id=virtual_machine_id)
            super(S3ParallelsBackup, self)._process_with_virtual_machine(virtual_machine=virtual_machine)

    def _update_snapshots(self, virtual_machine_id: VirtualMachineID) -> bool:
        snapshot_list = self.parallels.get_snapshot_list(virtual_machine_id=virtual_machine_id)
        snapshot_for_delete_list = [snapshot for snapshot in snapshot_list
                                    if snapshot.get('days', 0) >= VM_SNAPSHOT_DAYS_COUNT]

        need_create_snapshot = False

        if len(snapshot_for_delete_list) > 0:
            for snapshot in snapshot_list:
                if snapshot.days >= VM_SNAPSHOT_DAYS_COUNT:
                    self.parallels.delete_snapshot(virtual_machine_id=virtual_machine_id, snapshot_id=snapshot.id)
            snapshot_list = self.parallels.get_snapshot_list(virtual_machine_id=virtual_machine_id)

        if 0 <= len(snapshot_list) < VM_SNAPSHOT_COUNT:
            if len(snapshot_list) > 0:
                last_snapshot = snapshot_list[-1]
                snapshot_date: datetime = last_snapshot.date
                if snapshot_date.date() < datetime.now().date():
                    need_create_snapshot = True
            else:
                need_create_snapshot = True

            if need_create_snapshot:
                self.parallels.create_snapshot(virtual_machine_id=virtual_machine_id)

        return need_create_snapshot