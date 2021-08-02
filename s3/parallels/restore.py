#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import os
from typing import Any, Dict, Optional

from common.app_logger import get_logger
from s3._base._base import OP_RESTORE, INFO_FIELD_NAME, INFO_NEW, INFO_OLD
from s3._base._typing import VM_UUID
from s3.parallels.operation import S3ParallelsOperation

logger = get_logger(__name__)


class S3ParallelsRestore(S3ParallelsOperation):

    def _copy_object(self, src: str, dst: str) -> None:
        return

    def _compare_files(self, file_info_old: Dict[str, Any], file_info_new: Dict[str, Any]) -> Optional[str]:
        if self.force:
            return OP_RESTORE

        return super(S3ParallelsRestore, self)._compare_files(file_info_old, file_info_new)

    def _do_file_operation(self, local_file_info: Dict[str, Any], remote_file_info: Dict[str, Any]):
        local_file_name = local_file_info.get('name')
        remote_file_name = remote_file_info.get('name')
        logger.info(f'Download file {remote_file_name}...')
        self.storage.download_file(local_file_path=local_file_name, remote_file_path=remote_file_name)

    def _do_operation(self, files: Dict[str, Any]):
        local_files_list = self._get_local_files_list(files)
        local_files_list = [info.get(INFO_FIELD_NAME) for info in local_files_list]

        for local_file in local_files_list:
            if os.path.exists(local_file):
                os.remove(local_file)

        for _, operation_info in files:
            local_file = operation_info.get(INFO_NEW)
            remote_file = operation_info.get(INFO_OLD)

            self._do_file_operation(local_file_info=local_file, remote_file_info=remote_file)

    def _pack_vm(self, vm_id: VM_UUID):
        return

    def _unpack_vm(self, vm_id: VM_UUID):
        return
