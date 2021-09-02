#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow

import asyncio
import os
import random
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Tuple

import boto3
import pytz
from boto3.s3.transfer import TransferConfig
from boto3.exceptions import Boto3Error
from botocore.config import Config
from botocore.exceptions import ClientError
from dateutil import tz

from config import configure
from s3.s3base.s3object import S3Object
from s3.s3functions import client_exception_handler
from utils import functions
from utils.app_logger import get_logger
from utils.arguments import Arguments
from utils.asyncobject import AsyncObjectHandler
from utils.consts import CPU_COUNT, FILE_SIZE_LIMIT, BUFFER_SIZE, MAX_CONCURRENCY, CLEAR_TO_END_LINE
from utils.convertors import append_end_path_sep, remove_start_separator, size_to_human, make_template_from_string, \
    remove_end_path_sep
from utils.functions import show_message
from utils.metasingleton import MetaSingleton
from utils.progress_bar import ProgressBar

logger = get_logger(__name__)

UTC = pytz.UTC


class S3StorageException(Exception):
    pass


class S3LocalFileNotFoundException(S3StorageException):
    def __init__(self, file_name: str):
        super(S3LocalFileNotFoundException, self).__init__(f'Local file "{file_name}" could not be found.')


class S3LocalIsNotFileException(S3StorageException):
    def __init__(self, file_name: str):
        super(S3LocalIsNotFileException, self).__init__(f'"{file_name}" is not file.')


class S3RemoteFileNotFoundException(S3StorageException):
    def __init__(self, file_name: str):
        super(S3RemoteFileNotFoundException, self).__init__(f'Remote file "{file_name}" could not be found.')


class S3Storage(AsyncObjectHandler, metaclass=MetaSingleton):

    def __init__(self, bucket: Optional[str] = None):
        logger.debug('-' * 4 + f" Constructor object {self.__class__.__name__}" + '-' * 40)
        AsyncObjectHandler.__init__(self)

        self._lock = threading.Lock()

        try:
            logger.debug(f'Open S3 AWS Session ' + '-' * 40)
            self._session = boto3.session.Session()

            aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID', None)
            aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY', None)

            client_config = Config(
                connect_timeout=10.0,
                read_timeout=20.0,
                max_pool_connections=CPU_COUNT,
                retries={
                    'total_max_attempts': 200,
                    'max_attempts': 100,
                    'mode': 'standard'
                }
            )

            logger.debug(f'Initialize S3 AWS client for {configure.S3_URL}...')
            self._client = self._session.client(
                service_name='s3',
                endpoint_url=configure.S3_URL,
                config=client_config,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key
            )

            if bucket is not None:
                response = self._client.get_bucket_lifecycle_configuration(
                    Bucket=bucket
                )

                rules = response.get('Rules')

                self._rules = {}

                if rules:
                    dt = datetime.now(tz=tz.UTC)

                    for rule in rules:
                        expiration = rule['Expiration']
                        rule_id = rule['ID']
                        rule_prefix = rule['Prefix']
                        rule_status = rule['Status']

                        if rule_status.lower() == 'enabled':
                            days = expiration['Days']
                            dt_start = dt - timedelta(days=days)
                            self._rules[rule_id] = {
                                'time': dt_start,
                                'prefix': rule_prefix,
                                'days': days
                            }
        except Exception as ex:
            logger.error(f'Error: {str(ex)}')
            raise ex

        logger.debug(f'{bucket=}')

        self._bucket = bucket if bucket is not None else configure.S3_BUCKET_NAME

        logger.debug(f'Initialize configure...')

        self._upload_onfig = TransferConfig(
            multipart_threshold=FILE_SIZE_LIMIT,
            multipart_chunksize=BUFFER_SIZE,
            max_concurrency=MAX_CONCURRENCY,
            max_io_queue=CPU_COUNT * 4,
            use_threads=True
        )

        self._download_config = TransferConfig(
            multipart_threshold=FILE_SIZE_LIMIT,
            multipart_chunksize=BUFFER_SIZE,
            max_concurrency=MAX_CONCURRENCY,
            num_download_attempts=20,
            max_io_queue=CPU_COUNT * 4,
            use_threads=True
        )

        logger.debug("Upload config: {!r}".format(str(self._upload_onfig)))
        logger.debug("Download config: {!r}".format(str(self._download_config)))

    def __del__(self):
        AsyncObjectHandler.__del__(self)
        logger.debug('-' * 4 + f" Destroyer object {self.__class__.__name__}" + '-' * 40)

    @client_exception_handler()
    def fetch_buckets_list(self) -> Optional[Dict[str, Any]]:
        logger.debug(f'Get buckets list from {configure.S3_URL}...')
        response = self._client.list_buckets()

        if response is not None:
            buckets = response.get('Buckets', None)
            if buckets is not None:
                logger.debug(f'{buckets=}')
                for key in buckets:
                    yield key

        return None

    def check_bucket(self, bucket: str) -> None:
        is_found = False

        for bucket_object in self.fetch_buckets_list():
            bucket_name = bucket_object.get('Name', '')

            if bucket_name.lower() == bucket.lower():
                is_found = True
                break

        if not is_found:
            logger.info(f'Create bucket with name {self._bucket}...')
            self.create_bucket(bucket=self._bucket)

    @client_exception_handler()
    def fetch_bucket_object(self, prefix: Optional[str] = None) -> Optional[Dict[str, Any]]:
        response = self._client.list_objects_v2(Bucket=self._bucket, Prefix=prefix)

        files = response.get('Contents')

        if files and len(files) > 0:
            file = files[0]

            key = file['Key']

            content: Dict[str, Any] = {
                'key': key,
                'size': file['Size'],
                'hash': file['ETag'],
                'mtime': file['LastModified']
            }

            return content

        return None

    def _fetch_all_objects(self, prefix: Optional[str] = None) -> List[Dict[str, Any]]:
        ret: List[Dict[str, Any]] = []

        # if prefix is not None:
        #     response = self._client.list_objects_v2(Bucket=self._bucket, Prefix=prefix, FetchOwner=True)
        # else:
        #     response = self._client.list_objects_v2(Bucket=self._bucket, FetchOwner=True)
        response = self._client.list_objects_v2(Bucket=self._bucket, Prefix=prefix, FetchOwner=True)

        key_count = response.get('KeyCount', 0)

        if key_count > 0:
            while True:
                is_truncated = response.get('IsTruncated', False)
                contents = response.get('Contents', None)
                if contents is not None:
                    ret.extend(contents)

                if is_truncated:
                    continuation_key = response.get('NextContinuationToken', None)
                    if continuation_key is not None:
                        # if prefix is not None:
                        #     response = self._client.list_objects_v2(Bucket=self._bucket,
                        #                                             ContinuationToken=continuation_key,
                        #                                             Prefix=prefix)
                        # else:
                        #     response = self._client.list_objects_v2(Bucket=self._bucket,
                        #                                             ContinuationToken=continuation_key)
                        response = self._client.list_objects_v2(Bucket=self._bucket,
                                                                ContinuationToken=continuation_key,
                                                                Prefix=prefix)
                else:
                    break
        return ret

    @client_exception_handler()
    def fetch_bucket_objects(self, prefix: Optional[str] = None) -> Optional[Dict[str, Any]]:
        objects_list = self._fetch_all_objects(prefix=prefix)

        for obj in objects_list:
            yield obj

        return None

    def _show_error(self, message: str) -> None:
        with self._lock:
            functions.show_error(message=message)

    def _show_message(self, message: str) -> None:
        with self._lock:
            functions.show_message(message=message)

    def get_bucket(self) -> str:
        return self._bucket

    def set_bucket(self, bucket: str) -> None:
        self._bucket = bucket

    @property
    def bucket(self) -> str:
        return self.get_bucket()

    @bucket.setter
    def bucket(self, bucket: str) -> None:
        self.set_bucket(bucket)

    def abort_all_multipart(self, for_path: Optional[str] = None):
        upload_id_check = {}

        attempt = 0

        while True:
            if attempt == 100:
                break

            multipart_list = self.get_multipart_list()

            if len(multipart_list) == 0:
                break

            logger.info('-' * 4 + f" ATTEMPT # {(attempt + 1):04d} " + '-' * 40)

            logger.info(f"{len(multipart_list)} part(s)")

            for multipart in multipart_list:
                upload_id = multipart.get('UploadId')
                key = multipart.get('Key')

                id_list = upload_id_check.setdefault(key, [])
                if upload_id not in id_list:
                    id_list.append(upload_id)

                need_abort = False

                if for_path is None:
                    need_abort = True
                elif key.startswith(for_path):
                    need_abort = True

                if need_abort:
                    self.append_task_to_list(
                        future=self.abort_multipart_async(
                            remote_file_path=key,
                            upload_id=upload_id
                        ),
                    )

            self.run_tasks()

            timeout = random.randint(250, 750)
            timeout = float(timeout) / 1000.0
            logger.info(f"Timeout is {timeout} sec.")
            time.sleep(timeout)

            attempt += 1

        ix = 0

        for key, upload_id_list in upload_id_check.items():
            ix += 1
            logger.error(f"{ix:04d}: File {key} couldn't be deleted. ({len(upload_id_list)} attempts).")

    async def abort_multipart_async(self, remote_file_path: str, upload_id: str):
        response = self._client.abort_multipart_upload(Bucket=self._bucket, Key=remote_file_path, UploadId=upload_id)
        logger.debug(
            f'Abort multipart upload with id {upload_id} for {remote_file_path} (Code = {response.get("ResponseMetadata").get("HTTPStatusCode")})')
        logger.debug(f"{response=}")

    @client_exception_handler()
    def abort_multipart(self, remote_file_path: str, upload_id: str):
        response = self._client.abort_multipart_upload(Bucket=self._bucket, Key=remote_file_path, UploadId=upload_id)
        logger.info(
            f'Abort multipart upload with id {upload_id} for {remote_file_path} (Code = {response.get("ResponseMetadata").get("HTTPStatusCode")})')
        logger.debug(f"{response=}")

    async def copy_object_async(self, src_remote_path: str, dst_remote_path: str, /, src_bucket: Optional[str] = None,
                                version_id: Optional[str] = None) -> None:

        if src_bucket is None:
            src_bucket = self._bucket

        src = {
            'Bucket': src_bucket,
            'Key': src_remote_path
        }

        if version_id is not None:
            src['VersionId'] = version_id

        self._client.copy_object(
            Bucket=self._bucket,
            CopySource=src,
            Key=dst_remote_path,
            MetadataDirective='COPY',
            TaggingDirective='COPY'
        )

    @client_exception_handler()
    def copy_object(self, src_remote_path: str, dst_remote_path: str, /, src_bucket: Optional[str] = None,
                    version_id: Optional[str] = None) -> None:
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(
            self.copy_object_async(src_remote_path, dst_remote_path, src_bucket=src_bucket, version_id=version_id))
        return result

    async def create_bucket_async(self, bucket: str) -> None:
        await self._client.create_bucket(Bucket=bucket)

    @client_exception_handler()
    def create_bucket(self, bucket: str) -> None:
        self._client.create_bucket(Bucket=bucket)

    async def get_object_async(self, remote_file_path: str, last_modified: datetime = None) -> Optional[S3Object]:
        response = self._client.get_object(
            Bucket=self._bucket,
            IfModifiedSince=last_modified,
            Key=remote_file_path
        )

        if response is None:
            return None

        return S3Object(name=remote_file_path, response=response)

    @client_exception_handler(['304'])
    def get_object(self, remote_file_path: str, last_modified: Optional[datetime] = None) -> Optional[S3Object]:
        response = self._client.get_object(
            Bucket=self._bucket,
            IfModifiedSince=last_modified,
            Key=remote_file_path
        )

        if response is None:
            return None

        return S3Object(name=remote_file_path, response=response)

    async def delete_file_async(self, remote_file_path: str):
        try:
            self._client.delete_object(Bucket=self._bucket, Key=remote_file_path)
        except ClientError as error:
            logger.error(
                f"Error uploading file {remote_file_path}: {str(error)}"
            )
        else:
            logger.debug(
                f'Object {remote_file_path} was deleted from {self._bucket}.'
            )

    @client_exception_handler(('404',))
    def delete_file(self, remote_file_path: str):
        self._client.delete_object(Bucket=self._bucket, Key=remote_file_path)
        logger.debug(
            f'Object {remote_file_path} was deleted from {self._bucket}.'
        )

    async def delete_objects_async(self, objects: Union[Tuple[Dict[str, str]], List[Dict[str, str]]], quiet: bool = False):
        objects_for_delete = {
            'Objects': list(objects),
            'Quiet': quiet
        }

        try:
            response = self._client.delete_objects(Bucket=self._bucket, Delete=objects_for_delete)

            if response is not None:
                deleted_items = response.get('Deleted', [])
                errors = response.get('Errors', [])

                for deleted_item in deleted_items:
                    logger.info(f'{deleted_item.get("Key", "Unknown object")} was deleted.')

                for error in errors:
                    logger.error(f'{error.get("Message")} ({error.get("Key")}).')

        except ClientError as error:
            logger.error(f"Error deleting some objects: {str(error)}")

    @client_exception_handler(['404'])
    def delete_objects(self, objects: Union[Tuple[Dict[str, str]], List[Dict[str, str]]], quiet: bool = False):
        delete_objects = {
            'Objects': objects,
            'Quiet': quiet
        }

        response = self._client.delete_objects(Bucket=self._bucket, Delete=delete_objects)

        if response is not None:
            deleted_items = response.get('Deleted', [])
            errors = response.get('Errors', [])

            for deleted_item in deleted_items:
                logger.info(f'{deleted_item.get("Key", "Unknown object")} was deleted.')

            for error in errors:
                logger.error(f'{error.get("Message")} ({error.get("Key")}).')

    async def download_file_async(self, local_file_path: str, remote_file_path: str, /, **kwargs) -> None:
        show_progress: bool = kwargs.pop('show_progress', True)

        file_info = self.get_object_info(remote_file_path=remote_file_path)

        content_length = file_info.get('ContentLength')
        content_length = int(content_length)

        try:
            self._client.download_file(
                Bucket=self._bucket,
                Key=remote_file_path,
                Filename=local_file_path,
                Config=self._download_config,
                Callback=ProgressBar(caption='File download run_process', total=content_length) if show_progress else None)
        except ClientError as error:
            logger.error(f"Error downloading file {remote_file_path}: {str(error)}.")
        else:
            logger.debug(f"File {remote_file_path} was downloaded")

    @client_exception_handler(['404'])
    def download_file(self, local_file_path: str, remote_file_path: str, /, **kwargs) -> None:
        show_progress: bool = kwargs.pop('show_progress', True)

        file_info = self.get_object_info(remote_file_path=remote_file_path)

        content_length = file_info.get('ContentLength')
        content_length = int(content_length)

        self._client.download_file(Bucket=self._bucket,
                                   Key=remote_file_path,
                                   Filename=local_file_path,
                                   Config=self._download_config,
                                   Callback=ProgressBar(caption='File download run_process',
                                                        total=content_length) if show_progress else None)

    @client_exception_handler(['404'])
    def get_bucket_encryption(self):
        response = self._client.get_bucket_encryption(Bucket=self._bucket)

        return response

    def is_directory(self, remote_path: str) -> bool:
        try:
            response = self.fetch_bucket_object(prefix=remote_path)
            if response is not None:
                return True
        except:
            pass

        return False

    @client_exception_handler(['404'])
    def get_object_info(self, remote_file_path: str) -> Dict[str, Any]:
        response = self._client.head_object(
            Bucket=self._bucket,
            Key=remote_file_path
        )

        return response

    def is_exist(self, remote_path: str) -> bool:
        try:
            if not self.is_directory(remote_path=remote_path):
                info = self.get_object_info(remote_file_path=remote_path)
                if info is not None:
                    return True
            else:
                response = self.fetch_bucket_object(prefix=remote_path)
                if response is not None:
                    return True
        except:
            pass

        return False

    @client_exception_handler(['404'])
    def get_multipart_list(self) -> Tuple[Dict[str, Any]]:
        response = self._client.list_multipart_uploads(Bucket=self._bucket)

        if response is None:
            return tuple()

        if response.get('Uploads'):
            return tuple(response.get('Uploads'))

        return tuple()

    @client_exception_handler()
    def get_objects_count(self, prefix: Optional[str] = None) -> int:
        objects_list = self._fetch_all_objects(prefix=prefix)

        if objects_list is None:
            return 0

        return len(objects_list)

    @client_exception_handler()
    def get_old_objects(self) -> List[Dict[str, Any]]:
        old_objects = []

        for _, rule in self._rules.items():
            dt: datetime = rule.get('time', None)
            prefix: str = rule.get('prefix', None)

            if dt is not None and prefix is not None:
                fetched_objects = self._fetch_all_objects(prefix=prefix)

                for fetched_object in fetched_objects:
                    last_modified = fetched_object.get('LastModified')
                    if last_modified is not None:
                        if last_modified < dt:
                            old_objects.append(fetched_object)

        return old_objects

    async def upload_file_async(self, local_file_path: str, remote_file_path: str, /, **kwargs) -> None:
        callback = None

        show_progress: bool = kwargs.pop('show_progress', True)

        if not show_progress:
            if not hasattr(self, '_dummy_dict_'):
                self._dummy_dict_ = {}

            self._dummy_dict_[remote_file_path] = {
                'iteration': 0,
                'total': 0
            }

        if not os.path.exists(local_file_path):
            raise S3LocalFileNotFoundException(file_name=local_file_path)

        if os.path.isfile(local_file_path):
            local_file_size = os.stat(local_file_path).st_size
            logger.debug(f'{local_file_size=}')

            file_stat = os.stat(local_file_path, follow_symlinks=True)

            logger.info(
                f"Upload file {remote_file_path} ({size_to_human(file_stat.st_size)}) "
                f"modified at {datetime.fromtimestamp(file_stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')}"
            )

            if show_progress:
                callback = ProgressBar(caption='File upload process', total=local_file_size)
            else:
                self._dummy_dict_[remote_file_path]['total'] = local_file_size

                if not show_progress:
                    def callback_dummy(*args, **kwargs):
                        arguments = Arguments(*args, **kwargs)
                        chuck_size = arguments.get(argument_type=(int, float,), throw_error=False, default=0)

                        total = self._dummy_dict_[remote_file_path]['total']
                        iteration = self._dummy_dict_[remote_file_path]['iteration']

                        iteration += chuck_size

                        self._dummy_dict_[remote_file_path]['iteration'] = iteration

                        percent = float(iteration) / float(total) if total != 0 else 0

                        logger.debug(
                            f"{args=}, {kwargs=}"
                        )

                        logger.debug(
                            f"{local_file_path} => {remote_file_path}"
                            f" uploaded {size_to_human(iteration)}"
                            f" from {size_to_human(total)} ({percent:0.4%})"
                        )
                    callback = callback_dummy

            try:
                self._client.upload_file(
                    local_file_path,
                    self._bucket,
                    remote_file_path,
                    Config=self._upload_onfig,
                    ExtraArgs={'ACL': 'private', 'ContentType': 'gzip'},
                    Callback=callback
                )
            except ClientError as error:
                logger.error(f"Error uploading file {remote_file_path}: {str(error)}"
                )
            else:
                logger.debug(
                    f'File {remote_file_path} ({size_to_human(local_file_size)}) upload completed.'
                )
            finally:
                if not show_progress:
                    del self._dummy_dict_[remote_file_path]
        else:
            raise S3LocalIsNotFileException(file_name=local_file_path)

    @client_exception_handler()
    def upload_file(self, local_file_path: str, remote_file_path: str, /, **kwargs) -> None:
        show_progress: bool = kwargs.pop('show_progress', True)

        if not os.path.exists(local_file_path):
            raise S3LocalFileNotFoundException(file_name=local_file_path)

        if os.path.isfile(local_file_path):
            local_file_size = os.stat(local_file_path).st_size
            logger.debug(f'{local_file_size=}')

            response = self._client.upload_file(
                local_file_path,
                self._bucket,
                remote_file_path,
                Config=self._upload_onfig,
                ExtraArgs={'ACL': 'private', 'ContentType': 'gzip'},
                Callback=ProgressBar(caption='File upload process', total=local_file_size) if show_progress else None
            )

            logger.debug(
                f'File {remote_file_path} ({size_to_human(local_file_size)}) upload completed {CLEAR_TO_END_LINE}')
        else:
            raise S3LocalIsNotFileException(file_name=local_file_path)

    @client_exception_handler()
    def upload(self, local_path: str, remote_path: str, /, **kwargs) -> None:
        show_progress: bool = kwargs.pop('show_progress', True)

        if os.path.exists(local_path):
            if os.path.isfile(local_path):
                self.upload_file(local_path, remote_path, show_progress=show_progress)
            else:
                for root, dirs, files in os.walk(local_path):
                    for file in files:
                        if not file.startswith('.') and not file.startswith('~'):
                            local_file_path = os.path.join(append_end_path_sep(root), file)

                            file_name_template = make_template_from_string(local_file_path, path=remove_end_path_sep(local_path))
                            remote_file_path = make_template_from_string(file_name_template, path=remove_end_path_sep(remote_path))

                            self.upload_file(local_file_path, remote_file_path, show_progress=show_progress)

            show_message('\r' + CLEAR_TO_END_LINE + '\r')
