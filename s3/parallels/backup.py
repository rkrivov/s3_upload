#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import os
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from common.app_logger import get_logger
from common.convertors import remove_start_path_sep, append_end_path_sep
from s3._base._base import OP_BACKUP, INFO_FIELD_NAME, INFO_NEW, INFO_OLD
from s3._base._consts import VM_SNAPSHOT_DAYS_COUNT, VM_SNAPSHOT_COUNT
from s3._base._typing import VM_UUID
from s3.parallels.operation import S3ParallelsOperation

logger = get_logger(__name__)


class S3ParallelsBackup(S3ParallelsOperation):

    def _compare_files(self, file_info_old: Dict[str, Any], file_info_new: Dict[str, Any]) -> Optional[str]:
        if self.force:
            return OP_BACKUP
        return super(S3ParallelsBackup, self)._compare_files(file_info_old, file_info_new)

    def _do_file_operation(self, local_file_info: Dict[str, Any], remote_file_info: Dict[str, Any]):

        if local_file_info is None:
            print('What the fuck?')

        local_file_name = local_file_info.get('name')

        remote_file_name = None
        if remote_file_info is not None:
            remote_file_name = remote_file_info.get('name', None)

        if remote_file_name is None:
            remote_file_name = local_file_name
            if remote_file_name.startswith(self._local_path):
                remote_file_name = remote_file_name[len(self._local_path):]
                remote_file_name = remove_start_path_sep(remote_file_name)
                remote_file_name = os.path.join(append_end_path_sep(self._archive_path), remote_file_name)

        logger.info(f'Upload file {remote_file_name}...')
        self.storage.upload_file(local_file_path=local_file_name, remote_file_path=remote_file_name)

    def _do_operation(self, files: Dict[str, Any]):
        remote_files_list = _get_remote_files_list(files)
        if len(remote_files_list) > 0:
            objects_for_delete = [{'Key': info.get(INFO_FIELD_NAME)} for info in remote_files_list]

            if len(objects_for_delete) > 0:
                self.storage.delete_objects(objects=objects_for_delete)

        for _, operation_info in files.items():
            local_file = operation_info.get(INFO_NEW)
            remote_file = operation_info.get(INFO_OLD)

            self._do_file_operation(local_file_info=local_file, remote_file_info=remote_file)

    def _run_process(self, vm_info: Dict[str, Any]):
        vm_id = vm_info.get('id', uuid.uuid4())
        if not self._is_packed(vm_id=vm_id):
            self._do_update_snapshots(vm_id=vm_id)
        super(S3ParallelsBackup, self)._run_process(vm_info=vm_info)

    def _do_update_snapshots(self, vm_id: VM_UUID) -> bool:
        snapshot_list = self.parallels.get_snapshot_list(vm_id=vm_id)

        snapshot_list = [spanshot for _, spanshot in snapshot_list.items()]
        snapshot_list.sort(key=lambda item: item.get('date', datetime.min))

        while len(snapshot_list) > VM_SNAPSHOT_COUNT:
            snapshot = snapshot_list[0]

            snapshot_list = snapshot_list[1:]

            self.parallels.delete_snapshot(vm_id=vm_id, snap_id=snapshot.get('id', ''))

        if len(snapshot_list) > 0 and snapshot_list[0].get('days', 0) > VM_SNAPSHOT_DAYS_COUNT:
            snapshot = snapshot_list[0]

            snapshot_list = snapshot_list[1:]

            self.parallels.delete_snapshot(vm_id=vm_id, snap_id=snapshot.get('id', ''))

        if len(snapshot_list) < VM_SNAPSHOT_COUNT:
            new_snapshot_id = self.parallels.create_snapshot(vm_id=vm_id)
            if new_snapshot_id is not None:
                self.parallels.switch_snapshot(vm_id=vm_id, snapshot_id=new_snapshot_id)

                return True

        return False
