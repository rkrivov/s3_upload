#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import json
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, Optional, List, AnyStr

import psutil
import pytz
from dateutil import tz

from s3.s3storage import S3Storage
from utils import consts
from utils.app_logger import get_logger
from utils.convertors import remove_end_path_sep, make_template_from_string, size_to_human, encode_string, \
    append_end_path_sep, append_start_path_sep, convert_value_to_type, \
    make_string_from_template, get_string_case, remove_start_path_sep
from utils.files import get_file_size, get_file_etag, calc_file_hash
from utils.functions import print_progress_bar, get_terminal_width
from utils.metasingleton import MetaSingleton

logger = get_logger(__name__)

INFO_FIELD_ID = 'id'
INFO_FIELD_NAME = 'name'
INFO_FIELD_SIZE = 'size'
INFO_FIELD_MTIME = 'mtime'
INFO_FIELD_HASH = 'hash'

INFO_REMOTE = 'remote'
INFO_LOCAL = 'local'

INFO_OP = 'operation'

OP_INSERT = 'insert'
OP_UPDATE = 'update'
OP_DELETE = 'delete'

OP_BACKUP = 'backup'
OP_RESTORE = 'restore'


def get_name_hash(name: str) -> str:
    name_hash = hash(name)
    name_hash = name_hash & ((1 << 64) - 1)
    bit_length: int = name_hash.bit_length()
    bytes_length: int = (bit_length // 8) + (1 if (bit_length % 8) != 0 else 0)

    return name_hash.to_bytes(bytes_length, sys.byteorder).hex()


def get_file_info(file: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(file):
        return None

    mtime = datetime.fromtimestamp(os.stat(file).st_mtime, tz=tz.UTC).replace(tzinfo=pytz.UTC)
    mtime = datetime.fromtimestamp(time.mktime(mtime.timetuple()))

    file_info = {
        INFO_FIELD_NAME: file,
        INFO_FIELD_SIZE: os.stat(file).st_size,
        INFO_FIELD_MTIME: mtime
    }

    return file_info


def get_remote_files_list(files: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = [operation_info.get(INFO_REMOTE, None) for _, operation_info in files.items()]
    result = [file_info for file_info in result if file_info is not None]

    return result


def get_local_files_list(files: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = [operation_info.get(INFO_LOCAL, None) for _, operation_info in files.items()]
    result = [file_info for file_info in result if file_info is not None]

    return result


class S3Base(metaclass=MetaSingleton):
    instance = None

    def __init__(self, bucket: str, local_path: Optional[str] = None, remote_path: Optional[str] = None):
        self._bucket_name = bucket
        self._local_path = local_path
        self._chuck_size = int(float(psutil.virtual_memory().free) * 0.75)
        self._hash_cache = {}
        self._inserted_new_files = 0
        self._updated_files = 0
        self._deleted_files = 0

        self._storage = None

        self._force = False
        self._need_archive = False
        self._need_pack = False
        self._delete_removed = False
        self._show_progress = True

        self._archive_path = None
        self._bak_archive_path = None

        if local_path is not None:
            self.set_local_path(local_path=local_path)

        if remote_path is not None:
            self.set_archive_path(remote_path=remote_path)

        logger.debug(f'Bucket: {self.bucket_hash} ({self.bucket_id})')
        logger.debug(f"Chuck size is {size_to_human(self._chuck_size)}")

    def __del__(self):
        pass

    def __call__(self, *args, **kwargs):
        self.run_process(*args, **kwargs)

    @property
    def bucket(self):
        return self._bucket_name

    @bucket.setter
    def bucket(self, bucket_name: AnyStr) -> None:
        if isinstance(bucket_name, bytes):
            bucket_name = bucket_name.decode(json.detect_encoding(bucket_name))
        self._bucket_name = bucket_name
        self._init_storage_control()

    @property
    def bucket_id(self):
        message = f'{self._archive_path}@{self._bucket_name}'
        return message

    @property
    def bucket_hash(self):
        return encode_string(self.bucket_id)

    @property
    def delete_removed(self):
        return self._delete_removed

    @delete_removed.setter
    def delete_removed(self, value):
        self._delete_removed = value

    @property
    def force(self) -> bool:
        return self._force

    @force.setter
    def force(self, value: bool):
        self._force = value

    @property
    def need_archive(self) -> bool:
        return self._need_archive

    @need_archive.setter
    def need_archive(self, value: bool):
        self._need_archive = value

    @property
    def need_pack(self) -> bool:

        return self._need_pack

    @need_pack.setter
    def need_pack(self, value: bool):
        self._need_pack = value

    @property
    def show_progress(self) -> bool:
        return self._show_progress

    @show_progress.setter
    def show_progress(self, value: bool):
        self._show_progress = value

    @property
    def storage(self) -> S3Storage:
        if not hasattr(self, '_storage'):
            self._storage = None
        if self._storage is None:
            self._init_storage_control()
        return self._storage

    @storage.setter
    def storage(self, storage: S3Storage):
        self._storage = storage

    def _calc_hash(self, file_path: str) -> str:
        file_hash = self._hash_cache.get(file_path, None)

        if file_hash is None:
            templates = {
                'LOCAL_PATH': remove_end_path_sep(self._local_path),
                'ARCHIVE_PATH': remove_end_path_sep(self._archive_path),
                'CONTAINER': remove_end_path_sep(consts.CONTAINERS_FOLDER),
                'LIBRARY': remove_end_path_sep(consts.LIB_FOLDER),
                'HOME': remove_end_path_sep(consts.HOME_FOLDER),
                'WORK': remove_end_path_sep(consts.WORK_FOLDER),
                'TEMP': remove_end_path_sep(consts.TEMP_FOLDER),
            }

            logger.debug(
                f"Calculate hash {consts.MD5_ENCODER_NAME.upper()} "
                f"for {make_template_from_string(file_path, **templates)}"
            )

            if get_file_size(file_name=file_path) > consts.FILE_SIZE_LIMIT:
                file_hash = get_file_etag(file_name=file_path, show_progress=self._show_progress)
            else:
                file_hash = calc_file_hash(file_object=file_path, show_progress=self._show_progress)

            self._hash_cache[file_path] = file_hash

            logger.debug("Calculate completed.")

        return file_hash

    def _compare_files(self, file_info_remote: Dict[str, Any], file_info_local: Dict[str, Any]) -> Optional[str]:

        if file_info_local is not None or file_info_remote is not None:

            if file_info_remote is not None and file_info_local is not None:
                messages = []

                file_name_new = file_info_local.get(INFO_FIELD_NAME)
                file_size_new = file_info_local.get(INFO_FIELD_SIZE, 0)
                file_mtime_new = file_info_local.get(INFO_FIELD_MTIME, datetime.min)
                file_mtime_new = file_mtime_new.replace(tzinfo=pytz.UTC)
                file_mtime_new = datetime.fromtimestamp(time.mktime(file_mtime_new.timetuple()))
                file_hash_new = file_info_local.get(INFO_FIELD_HASH, '')

                file_size_old = file_info_remote.get(INFO_FIELD_SIZE, 0)
                file_mtime_old = file_info_remote.get(INFO_FIELD_MTIME, datetime.min)
                file_mtime_old = file_mtime_old.replace(tzinfo=pytz.UTC)
                file_mtime_old = datetime.fromtimestamp(time.mktime(file_mtime_old.timetuple()))
                file_hash_old = file_info_remote.get(INFO_FIELD_HASH, '')

                if file_mtime_old == file_mtime_new:
                    return None

                messages.append(
                    "The size of the remote file "
                    f"({consts.CYAN + consts.BOLD}{size_to_human(file_size_old)}{consts.NBOLD + consts.DEF})"
                    " differs from the size of the local file "
                    f"({consts.CYAN + consts.BOLD}{size_to_human(file_size_new)}{consts.NBOLD + consts.DEF})"
                )

                if file_size_old == file_size_new:
                    return None

                messages.append(
                    f"The time when the remote file was last modified "
                    f"({consts.CYAN + consts.BOLD}{file_mtime_old}{consts.NBOLD + consts.DEF})"
                    " differs from the time when the local file was last modified "
                    f"({consts.CYAN + consts.BOLD}{file_mtime_new}{consts.NBOLD + consts.DEF})"
                )

                if file_hash_new == '':
                    local_file_name = self._make_local_file(file_name_new)
                    file_hash_new = self._calc_hash(file_path=local_file_name)
                    file_info_local[INFO_FIELD_HASH] = file_hash_new

                if file_hash_old == file_hash_new:
                    return None

                messages.append(
                    f"The ETag of the remote file "
                    f"({consts.CYAN + consts.BOLD}{file_hash_old}{consts.NBOLD + consts.DEF})"
                    " differs from the ETag of the local file "
                    f"({consts.CYAN + consts.BOLD}{file_hash_new}{consts.NBOLD + consts.DEF})"
                )

                for ix, value in enumerate(messages):
                    if ix == 0:
                        value = value[0].upper() + value[1:]
                    else:
                        value = value[0].lower() + value[1:]
                    messages[ix] = value

                message = ', and '.join(messages)

                # logger.warning(
                #     "The file "
                #     f"{consts.GRAY + consts.UNDERLINE}{file_name_new}{consts.NUNDERLINE + consts.DEF}"
                #     " was changed: "
                #     f"{message}"
                # )

                logger.info(f"File {file_info_remote.get(INFO_FIELD_NAME)} was upgraded.")
                return OP_UPDATE
            elif file_info_remote is None and file_info_local is not None:
                logger.info(f"File {file_info_local.get(INFO_FIELD_NAME)} was added.")
                return OP_INSERT
            elif file_info_remote is not None and file_info_local is None and self.delete_removed:
                logger.info(f"File {file_info_remote.get(INFO_FIELD_NAME)} was removed.")
                return OP_DELETE

        return None

    def _check_files(self, files: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        result = {}

        for name, operation in files.items():
            file_info_remote = operation.get(INFO_REMOTE, None)
            file_info_local = operation.get(INFO_LOCAL, None)

            operation_name = self._compare_files(file_info_remote=file_info_remote, file_info_local=file_info_local)

            if operation_name is not None:
                op_info = result.setdefault(name, {})

                op_info[INFO_REMOTE] = file_info_remote
                op_info[INFO_LOCAL] = file_info_local
                op_info[INFO_OP] = operation_name

        return result

    def fetch_files(self) -> Dict[str, Any]:
        files = {}

        files = self._fetch_local_files_list(operations_list=files)
        files = self._fetch_remote_files_list(operations_list=files)

        return files

    def _fetch_local_files_list(self, local_path: str = None,
                                operations_list: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:

        if local_path is None:
            local_path = self._local_path

        if operations_list is None:
            operations_list = {}

        if os.path.isfile(local_path):
            local_file_name = local_path
            self._local_path = os.path.dirname(local_file_name)

            name = local_file_name

            if name.startswith(local_path):
                name = name[len(local_path):]
                name = append_start_path_sep(name)

            name_hash = get_name_hash(name)

            file_info = get_file_info(file=local_file_name)

            operation_info = operations_list.setdefault(name_hash, {})
            operation_info[INFO_LOCAL] = file_info
            operation_info[INFO_OP] = OP_INSERT
        else:
            for root, dirs, files in os.walk(self._local_path):
                for file in files:
                    if not file.startswith('.') and not file.startswith('~'):
                        local_file_path = os.path.join(
                            append_end_path_sep(root),
                            remove_start_path_sep(file)
                        )

                        name = local_file_path

                        if name.startswith(local_path):
                            name = name[len(local_path):]
                            name = append_start_path_sep(name)

                        name_hash = get_name_hash(name)

                        file_info = get_file_info(file=local_file_path)

                        operation_info = operations_list.setdefault(name_hash, {})
                        operation_info[INFO_LOCAL] = file_info
                        operation_info[INFO_OP] = OP_INSERT

        return operations_list

    def _fetch_remote_files_list(self, remote_path: str = None,
                                 operations_list: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:

        if remote_path is None:
            remote_path = self._archive_path

        if operations_list is None:
            operations_list = {}

        prefix = append_end_path_sep(remote_path)
        objects_count = self.storage.get_objects_count(prefix=prefix)

        if objects_count > 0:
            completed_objects_count = 0

            for remote_object in self.storage.fetch_bucket_objects(prefix=prefix):
                if self.show_progress:
                    completed_objects_count += 1
                    print_progress_bar(iteration=completed_objects_count,
                                       total=objects_count,
                                       prefix='Fetching remote objects.py',
                                       length=get_terminal_width())

                file_name = convert_value_to_type(remote_object.get('Key', None), to_type=str)

                name = file_name
                if name.startswith(prefix):
                    name = name[len(prefix):]
                    name = append_start_path_sep(name)

                name_hash = get_name_hash(name)

                file_size = convert_value_to_type(remote_object.get('Size', None), to_type=int)
                file_mtime = convert_value_to_type(remote_object.get('LastModified', None), to_type=datetime)
                file_mtime = file_mtime.replace(tzinfo=pytz.UTC)
                file_mtime = datetime.fromtimestamp(time.mktime(file_mtime.timetuple()))
                file_hash = convert_value_to_type(remote_object.get('ETag', None), to_type=str)

                if file_hash is not None:
                    if file_hash.startswith('"'):
                        file_hash = file_hash[1:]
                    if file_hash.endswith('"'):
                        file_hash = file_hash[:-1]

                file_info = {
                    INFO_FIELD_NAME: file_name,
                    INFO_FIELD_SIZE: file_size,
                    INFO_FIELD_MTIME: file_mtime,
                    INFO_FIELD_HASH: file_hash
                }

                operation_info = operations_list.setdefault(name_hash, {})
                operation_info[INFO_REMOTE] = file_info
                operation_info[INFO_OP] = OP_DELETE

        return operations_list

    def _init_storage_control(self) -> S3Storage:

        if hasattr(self, '_storage'):
            delattr(self, '_storage')

        self._storage = S3Storage(bucket=self.bucket)

        return self._storage

    def _make_local_file(self, file_name: str) -> str:
        return make_string_from_template(file_name, path=remove_end_path_sep(self._local_path))

    def _make_remote_file(self, file_name: str) -> str:
        return make_string_from_template(file_name, path=remove_end_path_sep(self._archive_path))

    def operation(self, files: Dict[str, Any]):
        pass

    def process(self):
        files = {}

        files = self._fetch_remote_files_list(operations_list=files)
        files = self._fetch_local_files_list(operations_list=files)

        if len(files) > 0:
            files = self._check_files(files)

            if len(files) > 0:
                self.operation(files)

    def post_process(self):
        self.show_statistics()

    def pre_process(self):
        self.storage.abort_all_multipart(for_path=self._archive_path)

    def show_statistics(self):
        statistics = []
        if self._inserted_new_files > 0:
            statistics.append(
                f'{self._inserted_new_files} '
                f'{get_string_case(self._inserted_new_files, "file was", "files were")} inserted'
            )

        if self._updated_files > 0:
            statistics.append(
                f'{self._updated_files} '
                f'{get_string_case(self._updated_files, "file was", "files were")} updated'
            )

        if self._deleted_files > 0:
            statistics.append(
                f'{self._deleted_files} '
                f'{get_string_case(self._deleted_files, "file was", "files were")} deleted'
            )

        if len(statistics) > 0:
            statistics_message = ', '.join(statistics)
            logger.info(f'{statistics_message}.')

    def _upload_file(self, operation: str, file_info_remote: Dict[str, Any], file_info_local: Dict[str, Any]) -> None:

        if file_info_remote is not None:
            file_path = file_info_remote[INFO_FIELD_NAME]
        else:
            file_path = file_info_local[INFO_FIELD_NAME]

        local_file_path = self._make_local_file(file_name=file_path)
        remote_file_path = self._make_remote_file(file_name=file_path)

        try:
            logger.debug(f"{operation=}")
            logger.debug(f"{local_file_path=}")
            logger.debug(f"{remote_file_path=}")

            if operation == OP_INSERT:
                logger.info(f"Upload new file to {remote_file_path}")
                self.storage.upload(
                    local_path=local_file_path,
                    remote_path=remote_file_path,
                    show_progress=self._show_progress
                )
            elif operation == OP_UPDATE:
                logger.info(f"Upload changed file to {remote_file_path}")
                self.storage.upload(
                    local_path=local_file_path,
                    remote_path=remote_file_path,
                    show_progress=self._show_progress)
            elif operation == OP_DELETE:
                logger.info(f"Delete old file from {remote_file_path}")
                self.storage.delete_file(remote_file_path)
        except Exception as ex:
            logger.error(f'Exception {type(ex).__name__} with message: {str(ex)}.')

    def _update_parameters(self, **kwargs):
        backup_name = kwargs.pop('backup_name', None)

        if backup_name is not None:
            self._bucket_name = backup_name
            self._init_storage_control()

        self._show_progress = kwargs.pop('show_progress', False)
        self._need_archive = kwargs.pop('archive', False)
        self._force = kwargs.pop('force', False)
        self._need_pack = kwargs.pop('pack', False)
        self._delete_removed = kwargs.pop('delete_removed', False)

    def execute(self, *args, **kwargs) -> None:
        self._update_parameters()
        self.pre_process()
        try:
            self.process()
        finally:
            self.post_process()

    def set_local_path(self, local_path: str):
        self._local_path = local_path
        logger.debug(f'LOCAL PATH: {self._local_path}')

    def set_archive_path(self, remote_path: str):
        self._archive_path = remove_end_path_sep(remote_path)
        logger.debug(f'ARCHIVE_PATH: {self._archive_path}')

    def set_backup_archive_path(self, remote_path: str):
        self._bak_archive_path = remove_end_path_sep(remote_path)
        logger.debug(f'BACKUP PATH: {self._bak_archive_path}')

    @classmethod
    def get_instance(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(S3Base, cls).__new__()
        return cls.instance
