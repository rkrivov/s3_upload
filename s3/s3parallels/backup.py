#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
from datetime import datetime
from typing import Any, Dict, Optional, List, Tuple

from s3.s3base.s3baseobject import INFO_FIELD_NAME, INFO_LOCAL, INFO_REMOTE, INFO_OP, \
    OP_DELETE, OP_UPDATE, OP_INSERT
from s3.s3base.s3consts import VM_SNAPSHOT_DAYS_COUNT, VM_SNAPSHOT_COUNT
from s3.s3base.s3typing import VirtualMachineID
from s3.s3parallels.errors import VMError
from s3.s3parallels.objects.snapshot import ParallelsSnapshot
from s3.s3parallels.objects.virtualmachine import ParallelsVirtualMachine
from s3.s3parallels.operation import S3ParallelsOperation
from utils.app_logger import get_logger
from utils.convertors import size_to_human, make_string_from_template, \
    remove_end_path_sep
from utils.files import get_file_size

logger = get_logger(__name__)


# class S3ParallelsBackupDispatch(object):
#     def __init__(self, name):
#         self.name = name
#
#     def __call__(self, func):
#         @wraps(func)
#         def wrapped_func(wself, *args, **kwargs):
#             logger.debug(f'wrapped_func {wrapped_func.__name__}, args:{args}, kwargs: {args}')
#             return func(wself, *args, **kwargs)
#
#         ci = CommandInfo
#         ci.commands.append(ci(shortname=self.name, longname=self.name, func=func))
#         return wrapped_func
#
#     @staticmethod
#     def get_function(name) -> Callable:
#         logger.debug(f'CommandDispatch.func({name})')
#
#         for ci in CommandInfo.commands:
#             if ci.shortname == name or ci.longname == name:
#                 return ci.func
#
#         raise RuntimeError('unknown command')
#
#     @classmethod
#     async def execute_async(cls, name: str, wself, *args, **kwargs):
#         func = cls.get_function(name=name)
#
#         if func is not None and is_callable(func):
#             result = await func(wself, *args, **kwargs)
#             return result
#
#         raise CommandDispatchException(f'The function {name}() could not be found.')
#
#     @classmethod
#     def execute(cls, name: str, wself, *args, **kwargs):
#         func = cls.get_function(name=name)
#
#         if func is not None and is_callable(func):
#             result = func(wself, *args, **kwargs)
#             return result
#
#         raise CommandDispatchException(f'The function {name}() could not be found.')

class S3ParallelsBackupException(VMError):
    pass


class S3ParallelsBackup(S3ParallelsOperation):

    def __init__(self, bucket: str, **kwargs):
        super().__init__(bucket, **kwargs)

    def __del__(self):
        super().__del__()

    def _compare_files(self, file_info_remote: Dict[str, Any], file_info_local: Dict[str, Any]) -> Optional[str]:
        if file_info_remote is not None and file_info_local is not None:
            return OP_UPDATE
        elif file_info_remote is None and file_info_local is not None:
            return OP_INSERT
        elif file_info_remote is not None and file_info_local is None and self.delete_removed:
            return OP_DELETE
        else:
            return super(S3ParallelsBackup, self)._compare_files(file_info_remote, file_info_local)

    # @S3ParallelsBackupDispatch(name=OP_INSERT)
    async def _do_insert_file(self, **kwargs):
        local_file_info: Dict[str, Any] = kwargs.pop('local_file_info', None)

        if local_file_info is None:
            raise VMError('Local file info can not be None.')

        local_file_name = local_file_info.get(INFO_FIELD_NAME, None)

        if local_file_name is None:
            raise VMError('Incorrect local file name.')

        remote_file_name = local_file_name

        local_file_name = make_string_from_template(local_file_name, path=remove_end_path_sep(self._local_path))
        remote_file_name = make_string_from_template(remote_file_name, path=remove_end_path_sep(self._archive_path))

        logger.debug(f"Upload new file {remote_file_name} ({size_to_human(get_file_size(local_file_name))})")
        await self.storage.upload_file_async(local_file_name, remote_file_name, show_progress=self.show_progress)
        await self._insert_file_to_dbase_async(file_info=local_file_info)
        logger.info(f"File {remote_file_name} ({size_to_human(get_file_size(local_file_name))}) was uploaded.")

    # @S3ParallelsBackupDispatch(name=OP_UPDATE)
    async def _do_update_file(self, **kwargs):
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

        local_file_name = make_string_from_template(local_file_name, path=remove_end_path_sep(self._local_path))
        remote_file_name = make_string_from_template(remote_file_name, path=remove_end_path_sep(self._archive_path))

        logger.debug(f"Upload changed file {remote_file_name} ({size_to_human(get_file_size(local_file_name))})")
        await self.storage.upload_file_async(local_file_name, remote_file_name, show_progress=self.show_progress)
        await self._update_file_in_dbase_async(file_info=local_file_info)
        logger.info(f"File {remote_file_name} ({size_to_human(get_file_size(local_file_name))}) was uploaded.")

    # @S3ParallelsBackupDispatch(name=OP_DELETE)
    async def _do_delete_file(self, **kwargs):
        remote_file_info: Dict[str, Any] = kwargs.pop('remote_file_info', None)

        if remote_file_info is None:
            raise VMError('Remote file info can not be None.')

        remote_file_name = remote_file_info.get(INFO_FIELD_NAME, None)
        if remote_file_name is None:
            raise VMError('Incorrect remote file name.')

        remote_file_name = make_string_from_template(remote_file_name, path=remove_end_path_sep(self._archive_path))

        logger.debug(f'Delete removed file {remote_file_name}')
        await self.storage.delete_file_async(remote_file_name)
        await self._delete_file_from_dbase_async(file_info=remote_file_info)
        logger.info(f'File {remote_file_name} was deleted')

    def _do_file_operation(self,
                           operation_id: str,
                           local_file_info: Dict[str, Any],
                           remote_file_info: Dict[str, Any]):

        if operation_id in [OP_DELETE, OP_UPDATE, OP_INSERT]:
            if not hasattr(self, '_operation_tasks'):
                self._operation_tasks = []

            if operation_id == OP_INSERT:
                self.append_task_to_list(
                    future=self._do_insert_file(
                        local_file_info=local_file_info
                    )
                )
            elif operation_id == OP_UPDATE:
                self.append_task_to_list(
                    future=self._do_update_file(
                        local_file_info=local_file_info,
                        remote_file_info=remote_file_info
                    )
                )
            elif operation_id == OP_DELETE:
                self.append_task_to_list(
                    future=self._do_delete_file(
                        remote_file_info=remote_file_info
                    )
                )
        else:
            raise VMError(f'Incorrect operation ("{operation_id.upper()}").')

    def operation(self, files: Dict[str, Any], virtual_machine: Optional[ParallelsVirtualMachine] = None):
        for _, operation_info in files.items():
            local_file = operation_info.get(INFO_LOCAL)
            remote_file = operation_info.get(INFO_REMOTE)
            operation_id = operation_info.get(INFO_OP)

            self._do_file_operation(operation_id=operation_id,
                                    local_file_info=local_file,
                                    remote_file_info=remote_file)

        self.run_tasks()

    def _process_with_virtual_machine(self, virtual_machine: ParallelsVirtualMachine):
        virtual_machine_id = virtual_machine.id

        if virtual_machine_id is not None:
            self._update_snapshots(virtual_machine_id=virtual_machine_id)
            super(S3ParallelsBackup, self)._process_with_virtual_machine(virtual_machine=virtual_machine)

    def _update_snapshots(self, virtual_machine_id: VirtualMachineID) -> bool:
        def find_last_snapshot(snapshots_list: List[ParallelsSnapshot]) -> Tuple[int, ParallelsSnapshot]:
            if len(snapshots_list) == 1:
                return 0, snapshots_list[0]

            last_index = -1
            last_date = datetime.min

            for snapshot_index, snapshot in enumerate(snapshots_list):
                if snapshot.date > last_date:
                    last_date = snapshot.date
                    last_index = snapshot_index

            return last_index, snapshots_list[last_index] if 0 <= last_index < len(snapshots_list) else None

        snapshot_list = self.parallels.get_snapshot_list(virtual_machine_id=virtual_machine_id)
        snapshot_for_delete_list = [snapshot for snapshot in snapshot_list
                                    if snapshot.get('days', 0) >= VM_SNAPSHOT_DAYS_COUNT]

        if len(snapshot_for_delete_list) == len(snapshot_list):
            last_index, _ = find_last_snapshot(snapshot_for_delete_list)
            if 0 <= last_index < len(snapshot_for_delete_list):
                snapshot_for_delete_list = snapshot_for_delete_list[:last_index - 1]

        if len(snapshot_for_delete_list) > 0:
            for snapshot in snapshot_list:
                if snapshot.days >= VM_SNAPSHOT_DAYS_COUNT:
                    self.parallels.delete_snapshot(virtual_machine_id=virtual_machine_id, snapshot_id=snapshot.id)
            snapshot_list = self.parallels.get_snapshot_list(virtual_machine_id=virtual_machine_id)

        need_create_snapshot = False

        if 0 <= len(snapshot_list) < VM_SNAPSHOT_COUNT:
            if len(snapshot_list) == 0:
                need_create_snapshot = True
            else:
                _, last_snapshot = find_last_snapshot(snapshot_list)
                snapshot_date: datetime = last_snapshot.date
                snapshot_days: int = last_snapshot.days
                snapshot_days_max: int = int(VM_SNAPSHOT_DAYS_COUNT // VM_SNAPSHOT_COUNT)
                need_create_snapshot = (snapshot_date.date() < datetime.now().date() and snapshot_days >= snapshot_days_max)

            if need_create_snapshot:
                self.parallels.create_snapshot(virtual_machine_id=virtual_machine_id)

        return need_create_snapshot
