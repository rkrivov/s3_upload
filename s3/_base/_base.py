#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow

import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, Optional, List

import psutil
import pytz
from dateutil import tz

from common import app_logger, consts, utils
from common.consts import FILE_SIZE_LIMIT
from common.convertors import remove_end_path_sep, make_template_from_string, size_to_human, remove_start_separator, \
    append_end_path_sep, append_start_path_sep, remove_start_path_sep, convert_value_to_type, make_string_from_template, \
    get_string_case, encode_string
from common.files import get_file_size, get_file_etag, calc_file_hash
from common.metasingleton import MetaSingleton
from common.singleton import Singleton
from common.utils import print_progress_bar, get_terminal_width
from s3.storage import S3Storage

logger = app_logger.get_logger(__name__)

INFO_FIELD_ID = 'id'
INFO_FIELD_NAME = 'name'
INFO_FIELD_SIZE = 'size'
INFO_FIELD_MTIME = 'mtime'
INFO_FIELD_HASH = 'hash'

INFO_OLD = 'old'
INFO_NEW = 'new'

INFO_OP = 'operation'

OP_INSERT = 'insert'
OP_UPDATE = 'update'
OP_DELETE = 'delete'

OP_BACKUP = 'backup'
OP_RESTORE = 'restore'


def _get_name_hash(name: str) -> str:
    name_hash = hash(name)
    name_hash = name_hash & ((1 << 64) - 1)
    bit_length: int = name_hash.bit_length()
    bytes_length: int = (bit_length // 8) + (1 if (bit_length % 8) != 0 else 0)

    return name_hash.to_bytes(bytes_length, sys.byteorder).hex()


def _get_file_info(file: str) -> Optional[Dict[str, Any]]:
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


class S3Base(metaclass=MetaSingleton):

    def __init__(self, bucket: str, local_path: Optional[str] = None, remote_path: Optional[str] = None):
        self._bucket_name = bucket
        self._local_path = local_path
        self._chuck_size = int(float(psutil.virtual_memory().free) * 0.75)
        self._hash_cache = {}
        self._inserted_new_files = 0
        self._updated_files = 0
        self._deleted_files = 0

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

    def __call__(self):
        self.process()

    @property
    def bucket(self):
        return self._bucket_name

    @property
    def bucket_id(self):
        message = f'{self._archive_path}@{self._bucket_name}'
        return message

    @property
    def bucket_hash(self):
        return encode_string(self.bucket_id)

    @property
    def force(self) -> bool:
        force = False

        if hasattr(self, '_force'):
            force = self._force
        elif hasattr(self, '_all_files'):
            force = self._all_files

        return force

    @force.setter
    def force(self, value: bool):
        self._force = value

    @property
    def pack(self) -> bool:
        pack = False

        if hasattr(self, '_pack_vm'):
            pack = self._pack

        return pack

    @pack.setter
    def pack(self, value: bool):
        self._pack = value

    @property
    def show_progress(self) -> bool:
        show_progress = False

        if hasattr(self, '_show_progress'):
            show_progress = self._show_progress

        return show_progress

    @show_progress.setter
    def show_progress(self, value: bool):
        self._show_progress = value

    @property
    def storage(self) -> S3Storage:
        storage = None

        if hasattr(self, '_storage'):
            storage = self._storage

        if storage is None:
            storage = S3Storage(bucket=self.bucket)
            self._storage = storage

        return storage

    @storage.setter
    def storage(self, storage: S3Storage):
        self._storage = storage

    def _archive_old_files(self):
        if self._storage.is_exist(self._archive_path):
            self._copy_object(src=self._archive_path, dst=self._bak_archive_path)

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

            if get_file_size(file_name=file_path) > FILE_SIZE_LIMIT:
                file_hash = get_file_etag(file_name=file_path, show_progress=self._show_progress)
            else:
                file_hash = calc_file_hash(file_object=file_path, show_progress=self._show_progress)

            self._hash_cache[file_path] = file_hash

            logger.debug("Calculate completed.")

        return file_hash

    def _compare_files(self, file_info_old: Dict[str, Any], file_info_new: Dict[str, Any]) -> Optional[str]:

        if file_info_new is not None or file_info_old is not None:

            if file_info_old is not None and file_info_new is not None:
                messages = []

                file_name_new = file_info_new.get(INFO_FIELD_NAME)
                file_size_new = file_info_new.get(INFO_FIELD_SIZE, 0)
                file_mtime_new = file_info_new.get(INFO_FIELD_MTIME, datetime.min)
                file_mtime_new = file_mtime_new.replace(tzinfo=pytz.UTC)
                file_mtime_new = datetime.fromtimestamp(time.mktime(file_mtime_new.timetuple()))
                file_hash_new = file_info_new.get(INFO_FIELD_HASH, '')

                file_name_old = file_info_old.get(INFO_FIELD_NAME)
                file_size_old = file_info_old.get(INFO_FIELD_SIZE, 0)
                file_mtime_old = file_info_old.get(INFO_FIELD_MTIME, datetime.min)
                file_mtime_old = file_mtime_old.replace(tzinfo=pytz.UTC)
                file_mtime_old = datetime.fromtimestamp(time.mktime(file_mtime_old.timetuple()))
                file_hash_old = file_info_old.get(INFO_FIELD_HASH, '')

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
                    file_info_new[INFO_FIELD_HASH] = file_hash_new

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

                logger.warning(
                    "The file "
                    f"{consts.GRAY + consts.UNDERLINE}{file_name_new}{consts.NUNDERLINE + consts.DEF}"
                    " was changed: "
                    f"{message}"
                )

                return OP_INSERT
            elif file_info_old is not None and file_info_new is None:
                return OP_INSERT

        return None

    def _check_files(self, files: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        result = {}

        for name, operation in files.items():
            file_info_old = operation.get(INFO_OLD, None)
            file_info_new = operation.get(INFO_NEW, None)

            operation_name = self._compare_files(file_info_old=file_info_old, file_info_new=file_info_new)

            if operation_name is not None:
                op_info = result.setdefault(name, {})

                op_info[INFO_OLD] = file_info_old
                op_info[INFO_NEW] = file_info_new

                op_info[INFO_OP] = operation_name

        return result

    def _copy_object(self, src: str, dst: str) -> None:
        copied_files = 0

        logger.info(f"Copy objects from {src} to {dst}")

        objects_count = self.storage.get_objects_count(prefix=self._archive_path)
        completed_objects_count = 0

        for fetch_object in self.storage.fetch_bucket_objects(prefix=src):
            if self._show_progress:
                completed_objects_count += 1
                print_progress_bar(iteration=completed_objects_count,
                                   total=objects_count,
                                   prefix='Copying objects',
                                   length=get_terminal_width())

            src_name = fetch_object.get('Key')
            dst_name = src_name
            if dst_name.startswith(src):
                dst_name = os.path.join(
                    append_end_path_sep(dst),
                    remove_start_separator(dst_name[len(src):])
                )

            if src_name and dst_name and src_name != dst_name:
                self.storage.copy_object(src_remote_path=src_name, dst_remote_path=dst_name)
                copied_files += 1

        if copied_files > 0:
            logger.info(f'Copied {copied_files} object(s).')

    def _fetch_files(self) -> Dict[str, Any]:
        files = {}

        files = self._fetch_local_files_list(opetations=files)
        files = self._fetch_remote_files_list(operations=files)

        return files

    def _fetch_local_files_list(self, local_path: str = None, opetations: Optional[Dict[str, Any]] = None) -> Dict[
        str, Any]:

        if local_path is None:
            local_path = self._local_path

        if opetations is None:
            opetations = {}

        if os.path.isfile(local_path):
            local_file_name = local_path
            self._local_path = os.path.dirname(local_file_name)

            name = local_file_name

            if name.startswith(local_path):
                name = name[len(local_path):]
                name = append_start_path_sep(name)

            name_hash = _get_name_hash(name)

            file_info = _get_file_info(file=local_file_name)

            operation_info = opetations.setdefault(name_hash, {})
            operation_info[INFO_NEW] = file_info
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

                        name_hash = _get_name_hash(name)

                        file_info = _get_file_info(file=local_file_path)

                        operation_info = opetations.setdefault(name_hash, {})
                        operation_info[INFO_NEW] = file_info
                        operation_info[INFO_OP] = OP_INSERT

        return opetations

    def _fetch_remote_files_list(self, remote_path: str = None, operations: Optional[Dict[str, Any]] = None) -> Dict[
        str, Any]:

        if remote_path is None:
            remote_path = self._archive_path

        if operations is None:
            operations = {}

        prefix = append_end_path_sep(remote_path)
        objects_count = self.storage.get_objects_count(prefix=prefix)

        if objects_count > 0:
            completed_objects_count = 0

            for remote_object in self.storage.fetch_bucket_objects(prefix=prefix):
                if self.show_progress:
                    completed_objects_count += 1
                    print_progress_bar(iteration=completed_objects_count,
                                       total=objects_count,
                                       prefix='Fetching remote objects',
                                       length=get_terminal_width())

                file_name = convert_value_to_type(remote_object.get('Key', None), to_type=str)

                name = file_name
                if name.startswith(prefix):
                    name = name[len(prefix):]
                    name = append_start_path_sep(name)

                name_hash = _get_name_hash(name)

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

                operation_info = operations.setdefault(name_hash, {})
                operation_info[INFO_OLD] = file_info
                operation_info[INFO_OP] = OP_DELETE

        return operations

    def _get_local_files_list(self, files: Dict[str, Any]) -> List[Dict[str, Any]]:

        result = [operation_info.get(INFO_NEW, None) for _, operation_info in files.items()]
        result = [file_info for file_info in result if file_info is not None]

        return result

    def _get_remote_files_list(self, files: Dict[str, Any]) -> List[Dict[str, Any]]:

        result = [operation_info.get(INFO_OLD, None) for _, operation_info in files.items()]
        result = [file_info for file_info in result if file_info is not None]

        return result

    def _make_local_file(self, file_name: str) -> str:
        return make_string_from_template(file_name, path=remove_end_path_sep(self._local_path))

    def _make_remote_file(self, file_name: str) -> str:
        return make_string_from_template(file_name, path=remove_end_path_sep(self._archive_path))

    def _do_operation(self, files: Dict[str, Any]):
        pass
        # TODO Restore the following lines after testing
        # if len(self._files) > 0:
        #
        #     self._process_upload_files()

    def _operation(self):

        files = {}

        files = self._fetch_remote_files_list(operations=files)
        files = self._fetch_local_files_list(opetations=files)

        if len(files) > 0:
            files = self._check_files(files)

            self._do_operation(files)

    def _process_post(self):
        self._show_statistics()

    def _process_pre(self):
        delete_files = []

        for file_object in self.storage.fetch_bucket_objects(prefix=consts.HOME_FOLDER):
            file_path = file_object.get('Key', None)
            if file_path is not None:
                delete_files.append({
                    'Key': file_path
                })

        if len(delete_files) > 0:
            self.storage.delete_objects(delete_files)

        self.storage.abort_all_multipart(for_path=self._archive_path)
        self._remove_old_objects()

    def _remove_old_objects(self):
        fetched_old_objects = self.storage.get_old_objects()
        if len(fetched_old_objects) > 0:
            fetched_old_objects = [{'Key': fetched_object.get('Key')} for fetched_object in fetched_old_objects]
        if len(fetched_old_objects) > 0:
            self.storage.delete_objects(fetched_old_objects)

    def _show_statistics(self):
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

    def _upload_file(self, operation: str, file_info_old: Dict[str, Any], file_info_new: Dict[str, Any]) -> None:

        if file_info_old is not None:
            file_path = file_info_old[INFO_FIELD_NAME]
        else:
            file_path = file_info_new[INFO_FIELD_NAME]

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

    def process(self, *args, **kwargs) -> None:
        self._process_pre()
        try:
            self._operation()
        finally:
            self._process_post()

    def set_local_path(self, local_path: str):
        self._local_path = local_path
        logger.debug(f'LOCAL PATH: {self._local_path}')

    def set_archive_path(self, remote_path: str):
        dt = datetime.now()

        archive_name = remove_end_path_sep(remote_path)

        archive_names = archive_name.split(os.path.sep)
        if len(archive_names) > 0:
            archive_name = archive_names[-1]

        archive_name = f'{dt.strftime("%H%M%S")} {archive_name}'
        archive_path = append_end_path_sep(os.path.join('Archives', dt.strftime('%Y/%j')))
        archive_path = os.path.join(archive_path, archive_name)

        self._archive_path = remove_end_path_sep(remote_path)
        self._bak_archive_path = remove_end_path_sep(archive_path)

        logger.debug(f'ARCHIVE_PATH: {self._archive_path}')
        logger.debug(f'BACKUP ARCHIVE_PATH: {self._bak_archive_path}')

    @classmethod
    def get_instance(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(S3Base, cls).__new__()
        return cls.instance
