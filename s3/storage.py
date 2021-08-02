#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow

import os
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Tuple

import boto3
import pytz
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from dateutil import tz

from common import app_logger
from common import utils
from common.consts import CPU_COUNT, FILE_SIZE_LIMIT, BUFFER_SIZE, MAX_CONCURRENCY, CLEAR_TO_END_LINE
from common.progress_bar import ProgressBar
from config import configure
from s3._base._object import S3Object
from s3.utils import client_exception_handler

logger = app_logger.get_logger(__name__)

UTC = pytz.UTC


class S3Storage(object):

    def __init__(self, bucket: Optional[Union[str, bytes]] = None):
        self._lock = threading.Lock()

        try:
            logger.info(f'Open S3 AWS Session...')
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

            logger.info(f'Initialize S3 AWS client for {configure.S3_URL}...')
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

        self._config = TransferConfig(
            multipart_threshold=FILE_SIZE_LIMIT,
            multipart_chunksize=BUFFER_SIZE,
            max_concurrency=MAX_CONCURRENCY,
            use_threads=True
        )

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
        if prefix is not None:
            response = self._client.list_objects_v2(Bucket=self._bucket, Prefix=prefix)
        else:
            response = self._client.list_objects_v2(Bucket=self._bucket)

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

        if prefix is not None:
            response = self._client.list_objects_v2(Bucket=self._bucket, Prefix=prefix, FetchOwner=True)
        else:
            response = self._client.list_objects_v2(Bucket=self._bucket, FetchOwner=True)

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
                        if prefix is not None:
                            response = self._client.list_objects_v2(Bucket=self._bucket,
                                                                    ContinuationToken=continuation_key,
                                                                    Prefix=prefix)
                        else:
                            response = self._client.list_objects_v2(Bucket=self._bucket,
                                                                    ContinuationToken=continuation_key)
                else:
                    break
        return ret

    @client_exception_handler()
    def fetch_bucket_objects(self, prefix: Optional[str] = None) -> Optional[Dict[str, Any]]:
        objects_list = self._fetch_all_objects(prefix=prefix)

        for obj in objects_list:
            yield obj

        return None

    def _show_error(self, message: Union[str, bytes]) -> None:
        with self._lock:
            utils.show_error(message=message)

    def _show_message(self, message: Union[str, bytes]) -> None:
        with self._lock:
            utils.show_message(message=message)

    def get_bucket(self) -> str:
        return self._bucket

    def set_bucket(self, bucket: Union[str, bytes]) -> None:
        self._bucket = bucket

    @property
    def bucket(self) -> str:
        return self.get_bucket()

    @bucket.setter
    def bucket(self, bucket: Union[str, bytes]) -> None:
        self.set_bucket(bucket)

    def abort_all_multipart(self, for_path: Optional[str] = None):
        multipart_list = self.get_multipart_list()

        for multipart in multipart_list:
            upload_id = multipart.get('UploadId')
            key = multipart.get('Key')
            need_abort = False
            if for_path is None:
                need_abort = True
            elif key.startswith(for_path):
                need_abort = True
            if need_abort:
                self.abort_multipart(remote_file_path=key, upload_id=upload_id)

    @client_exception_handler()
    def abort_multipart(self, remote_file_path: Union[str, bytes], upload_id: Union[str, bytes]):
        logger.info(f'Abort multipart upload with id {upload_id} for {remote_file_path}')
        self._client.abort_multipart_upload(Bucket=self._bucket, Key=remote_file_path, UploadId=upload_id)

    @client_exception_handler()
    def copy_object(self,
                    src_remote_path: str,
                    dst_remote_path: str,
                    bucket: Optional[str] = None,
                    version_id: Optional[str] = None) -> None:

        if bucket is not None:
            src = {
                'Bucket': bucket,
                'Key': src_remote_path
            }
        else:
            src = {
                'Bucket': self._bucket,
                'Key': src_remote_path
            }
            # src = src_remote_path

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
    def create_bucket(self, bucket: Union[str, bytes]) -> None:
        self._client.create_bucket(Bucket=bucket)

    @client_exception_handler(['304'])
    def get_object(self, remote_file_path: str, last_modified: datetime = None) -> Optional[S3Object]:
        if last_modified is not None:
            response = self._client.get_object(Bucket=self._bucket, IfModifiedSince=last_modified, Key=remote_file_path)
        else:
            response = self._client.get_object(Bucket=self._bucket, Key=remote_file_path)

        if response is None:
            return None

        return S3Object(name=remote_file_path, response=response)

    @client_exception_handler(('404',))
    def delete_file(self, remote_file_path: Union[str, bytes]):
        logger.debug(f'Delete object {remote_file_path} from {self._bucket}...')
        self._client.delete_object(Bucket=self._bucket, Key=remote_file_path)

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

    @client_exception_handler(['404'])
    def download_file(self, local_file_path: Union[str, bytes], remote_file_path: Union[str, bytes]) -> None:
        file_info = self.get_object_info(remote_file_path=remote_file_path)

        content_length = file_info.get('ContentLength')
        content_length = int(content_length)

        self._client.download_file(Bucket=self._bucket,
                                   Key=remote_file_path,
                                   Filename=local_file_path,
                                   Config=self._config,
                                   Callback=ProgressBar(caption=f'Download file', max_value=content_length))

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
    def get_object_info(self, remote_file_path: Union[str, bytes]) -> Dict[str, Any]:
        response = self._client.head_object(Bucket=self._bucket, Key=remote_file_path)

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

    @client_exception_handler()
    def upload_file(self,
                    local_file_path: Union[str, bytes],
                    remote_file_path: Union[str, bytes],
                    show_progress: bool = True) -> None:
        if os.path.isfile(local_file_path):
            local_file_size = os.stat(local_file_path).st_size
            logger.debug(f'{local_file_size=}')

            if show_progress:
                self._client.upload_file(
                    local_file_path,
                    self._bucket,
                    remote_file_path,
                    Config=self._config,
                    Callback=ProgressBar(caption=f'Upload file {os.path.basename(remote_file_path)}',
                                         total=local_file_size)
                )
                utils.show_message('\r' + CLEAR_TO_END_LINE + '\r')
            else:
                self._client.upload_file(
                    local_file_path,
                    self._bucket,
                    remote_file_path,
                    Config=self._config
                )
            logger.debug(f'File {remote_file_path} upload completed {CLEAR_TO_END_LINE}')

    @client_exception_handler()
    def upload(self,
               local_path: Union[str, bytes],
               remote_path: Union[str, bytes],
               show_progress: bool = True) -> None:
        if os.path.isfile(local_path):
            self.upload_file(local_path, remote_path, show_progress=show_progress)
        else:
            for root, dirs, files in os.walk(local_path):
                for file in files:
                    if not file.startswith('.') and not file.startswith('~'):
                        local_file_path = os.path.join(utils.append_end_path_sep(root), file)
                        remote_file_path = os.path.join(
                            utils.append_end_path_sep(remote_path),
                            utils.remove_start_separator(local_file_path[len(local_path):])
                        )
                        self.upload_file(local_file_path, remote_file_path, show_progress=show_progress)

        utils.show_message('\r' + consts.CLEAR_TO_END_LINE + '\r')
