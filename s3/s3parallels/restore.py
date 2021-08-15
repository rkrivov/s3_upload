#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import os
from typing import Any, Dict, Optional

from s3.s3base.s3baseobject import OP_RESTORE
from s3.s3parallels.objects.virtualmachine import ParallelsVirtualMachine
from s3.s3parallels.operation import S3ParallelsOperation
from utils.app_logger import get_logger
from utils.convertors import remove_start_path_sep, append_end_path_sep

logger = get_logger(__name__)


class S3ParallelsRestore(S3ParallelsOperation):

    def _compare_files(self, file_info_remote: Dict[str, Any], file_info_local: Dict[str, Any]) -> Optional[str]:
        if self.force:
            return OP_RESTORE

        return super(S3ParallelsRestore, self)._compare_files(file_info_remote, file_info_local)

    def _do_file_operation(self, local_file_info: Dict[str, Any], remote_file_info: Dict[str, Any]):
        remote_file_name = remote_file_info.get('name')

        if local_file_info is not None:
            local_file_name = local_file_info.get('name')
        else:
            local_file_name = remote_file_name

            if local_file_name.startswith(self._archive_path):
                local_file_name = local_file_name[len(self._archive_path):]
                local_file_name = remove_start_path_sep(local_file_name)
                local_file_name = os.path.join(append_end_path_sep(self._local_path), local_file_name)

        if not os.path.exists(os.path.dirname(local_file_name)):
            os.makedirs(os.path.dirname(local_file_name))
            logger.info(f'New directory {os.path.dirname(local_file_name)} was created.')

        logger.info(f'Download file {remote_file_name}...')
        self.storage.download_file(local_file_path=local_file_name,
                                   remote_file_path=remote_file_name,
                                   show_progress=self.show_progress)

    def operation(self, files: Dict[str, Any], virtual_machine: Optional[ParallelsVirtualMachine] = None):
        raise NotImplementedError()
        # TODO Create recovery files from S3 storage
        # local_files_list = _get_local_files_list(files)
        # local_files_list = [info.get(INFO_FIELD_NAME) for info in local_files_list]
        #
        # for local_file in local_files_list:
        #     if os.path.exists(local_file):
        #         os.remove(local_file)
        #
        # for _, operation_info in files:
        #     local_file = operation_info.get(INFO_LOCAL)
        #     remote_file = operation_info.get(INFO_REMOTE)
        #
        #     self._do_file_operation(local_file_info=local_file, remote_file_info=remote_file)
