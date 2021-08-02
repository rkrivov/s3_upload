#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from common.app_logger import get_logger
from s3._base._base import OP_BACKUP, INFO_FIELD_NAME, INFO_NEW, INFO_OLD
from s3._base._consts import VM_SNAPSHOT_DAYS_COUNT
from s3._base._typing import VM_UUID
from s3.parallels.operation import S3ParallelsOperation

logger = get_logger(__name__)


class S3ParallelsBackup(S3ParallelsOperation):

    def _compare_files(self, file_info_old: Dict[str, Any], file_info_new: Dict[str, Any]) -> Optional[str]:
        if self.force:
            return OP_BACKUP
        return super(S3ParallelsBackup, self)._compare_files(file_info_old, file_info_new)

    def _do_file_operation(self, local_file_info: Dict[str, Any], remote_file_info: Dict[str, Any]):
        local_file_name = local_file_info.get('name')
        remote_file_name = remote_file_info.get('name')
        logger.info(f'Upload file {remote_file_name}...')
        self.storage.upload_file(local_file_path=local_file_name, remote_file_path=remote_file_name)

    def _do_operation(self, files: Dict[str, Any]):
        remote_files_list = self._get_remote_files_list(files)
        if len(remote_files_list) > 0:
            objects_for_delete = [{'Key': info.get(INFO_FIELD_NAME)} for info in remote_files_list]

            if len(objects_for_delete) > 0:
                self.storage.delete_objects(objects=objects_for_delete)

        for _, operation_info in files.items():
            local_file = operation_info.get(INFO_NEW)
            remote_file = operation_info.get(INFO_OLD)

            self._do_file_operation(local_file_info=local_file, remote_file_info=remote_file)

    def _run_process(self, vm_info: Dict[str, Any]):
        super(S3ParallelsBackup, self)._run_process(vm_info=vm_info)
        vm_id = vm_info.get('id', uuid.uuid4())
        if not self._is_packed(vm_id=vm_id):
            self._do_update_snapshots(vm_id=vm_id)

    def _do_update_snapshots(self, vm_id: VM_UUID):
        snapshot_list = self.parallels.get_snapshot_list(vm_id=vm_id)

        need_create_new_shapshot = True

        if len(snapshot_list) > 0:
            need_create_new_shapshot = False

            snapshot_parent = None
            snapshot_current = None
            snapshot_first = None
            snapshot_last = None

            for _, snapshot in snapshot_list.items():
                if snapshot.get('parent', '') == '':
                    snapshot_parent = snapshot

                if snapshot.get('current', False):
                    snapshot_current = snapshot

                if snapshot_first is None:
                    snapshot_first = snapshot
                elif snapshot_first.get('date', datetime.min) > snapshot.get('date', datetime.max):
                    snapshot_first = snapshot

                if snapshot_last is None:
                    snapshot_last = snapshot
                elif snapshot_last.get('date', datetime.max) < snapshot.get('date', datetime.min):
                    snapshot_last = snapshot

            if snapshot_first is not None:
                if snapshot_first.get('days', 0) > VM_SNAPSHOT_DAYS_COUNT:
                    self.parallels.delete_snapshot(vm_id=vm_id,
                                                   snap_id=snapshot_first.get('id', ''),
                                                   delete_child=snapshot_first.get('parent', '') == '')

                    if snapshot_first.get('parent') == '':
                        snapshot_parent = None
                        snapshot_current = None
                        snapshot_first = None
                        snapshot_last = None

                        need_create_new_shapshot = True
                    else:
                        if snapshot_current.get('id') == snapshot_first.get('id'):
                            snapshot_current = None

                        if snapshot_parent.get('id') == snapshot_first.get('id'):
                            snapshot_parent = None

            if snapshot_current is None and snapshot_last is not None:
                self.parallels.switch_snapshot(vm_id=vm_id, snapshot_id=snapshot_last.get('id'))
                snapshot_current = snapshot_last

        if need_create_new_shapshot:
            self.parallels.create_snapshot(vm_id=vm_id)
