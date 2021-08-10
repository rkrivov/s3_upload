#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import os
from typing import Dict, Any, Optional

from common.app_logger import get_logger
from common.convertors import remove_start_path_sep, append_end_path_sep
from common.functions import get_parameter

from s3.s3base.s3baseobject import S3Base, INFO_REMOTE, INFO_LOCAL, OP_BACKUP, INFO_FIELD_NAME

logger = get_logger(__name__)


class S3Backup(S3Base):

    def __init__(self, bucket: str, local_path: Optional[str] = None, remote_path: Optional[str] = None):
        super(S3Backup, self).__init__(bucket, local_path, remote_path)
        self._all_files = False

    def _check_files(self, files: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if self.force:
            return files

        return super(S3Backup, self)._check_files(files)

    def _compare_files(self, file_info_remote: Dict[str, Any], file_info_local: Dict[str, Any]) -> Optional[str]:
        if self.force:
            return OP_BACKUP

        return super(S3Backup, self)._compare_files(file_info_remote=file_info_remote, file_info_local=file_info_local)

    def operation(self, files: Dict[str, Any]):
        remote_files = _get_remote_files_list(files)

        if len(remote_files) > 0:
            remote_files = [{'Key': file_info.get(INFO_FIELD_NAME)} for file_info in remote_files]
            self.storage.delete_objects(remote_files)

        operations_list = [operation for _, operation in files.items() if operation.get(INFO_LOCAL, None) is not None]

        for operation_info in operations_list:
            local_file_info = operation_info.get(INFO_LOCAL)
            remote_file_info = operation_info.get(INFO_REMOTE)

            local_file_name = local_file_info.get('name')

            if remote_file_info is not None:
                remote_file_name = remote_file_info.get('name')
            else:
                remote_file_name = local_file_name
                if remote_file_name.startswith(self._local_path):
                    remote_file_name = remote_file_name[len(self._local_path):]
                    remote_file_name = remove_start_path_sep(remote_file_name)
                    remote_file_name = os.path.join(append_end_path_sep(self._archive_path), remote_file_name)

            logger.info(f'Upload file {remote_file_name}...')
            self.storage.upload_file(local_file_path=local_file_name, remote_file_path=remote_file_name)

    def run_process(self, *args, **kwargs) -> None:
        local_path = get_parameter(args=args, index=1, type=str, throw_error=False)
        remote_path = get_parameter(args=args, index=2, type=str, throw_error=False)

        if local_path is None:
            local_path = kwargs.pop('local_path', None)
        if remote_path is None:
            remote_path = kwargs.pop('remote_path', None)

        self._all_files = kwargs.pop('all_files', False)
        if local_path is not None:
            self.set_local_path(local_path=local_path)

        if remote_path is not None:
            self.set_archive_path(remote_path=remote_path)

        super(S3Backup, self).run_process(*args, **kwargs)
