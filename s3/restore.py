#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import os
import time
from datetime import datetime
from typing import Dict, Any, Optional

import pytz

from common import app_logger
from common import consts
from common import utils
from s3.base import S3Base, INFO_OLD, INFO_NEW, INFO_FIELD_NAME, INFO_FIELD_SIZE, \
    INFO_FIELD_MTIME, INFO_FIELD_HASH, OP_RESTORE

logger = app_logger.get_logger(__name__)


class S3Restore(S3Base):

    def __init__(self, bucket: str, local_path: Optional[str] = None, remote_path: Optional[str] = None):
        super(S3Restore, self).__init__(bucket, local_path, remote_path)
        self._all_files = False

    def _check_files(self, files: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if self.force:
            return files
        return super(S3Restore, self)._check_files(files)

    def _process_pre(self):
        return

    def _process_post(self):
        return

    def _compare_files(self, file_info_old: Dict[str, Any], file_info_new: Dict[str, Any]) -> Optional[str]:
        if self.force:
            return OP_RESTORE

        return super(S3Restore, self)._compare_files(file_info_old=file_info_old, file_info_new=file_info_new)

    def _do_operation(self, files: Dict[str, Any]):
        local_files = self._get_local_files_list(files)
        local_files = [file_info.get(INFO_FIELD_NAME) for file_info in local_files]

        for local_file_name in local_files:
            if os.path.exists(local_file_name):
                os.remove(local_file_name)
                logger.info(f'The file {local_file_name} was deleted.')

        files = [operation for _, operation in files.items() if operation.get(INFO_OLD, None) is not None]

        for operation in files:
            local_file_info = operation.get(INFO_NEW)
            remote_file_info = operation.get(INFO_OLD)

            remote_file_name = remote_file_info.get('name')

            if local_file_info is not None:
                local_file_name = local_file_info.get('name')
            else:
                local_file_name = remote_file_name

                if local_file_name.startswith(self._archive_path):
                    local_file_name = local_file_name[len(self._archive_path):]
                    local_file_name = utils.remove_start_path_sep(local_file_name)
                    local_file_name = os.path.join(utils.append_end_path_sep(self._local_path), local_file_name)

            if not os.path.exists(os.path.dirname(local_file_name)):
                os.makedirs(os.path.dirname(local_file_name))
                logger.info(f'New directory {os.path.dirname(local_file_name)} was created.')

            logger.info(f'Download file {remote_file_name}...')
            self.storage.download_file(local_file_path=local_file_name, remote_file_path=remote_file_name)

    def process(self, *args, **kwargs) -> None:
        local_path = utils.get_parameter(args=args, index=1, argument_type=str, throw_error=False)
        remote_path = utils.get_parameter(args=args, index=2, argument_type=str, throw_error=False)

        if local_path is None:
            local_path = kwargs.pop('local_path', None)
        if remote_path is None:
            remote_path = kwargs.pop('remote_path', None)

        self._all_files = kwargs.pop('all_files', False)

        if local_path is not None:
            self.set_local_path(local_path=local_path)

        if remote_path is not None:
            self.set_archive_path(remote_path=remote_path)

        super(S3Restore, self).process(*args, **kwargs)