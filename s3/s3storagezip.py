#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow
import shutil
from gzip import GzipFile
from io import BytesIO

from s3.s3storage import S3Storage
from utils.app_logger import get_logger
from utils.convertors import size_to_human
from utils.files import get_file_size
from utils.functions import total_len
from utils.progress_bar import ProgressBar

logger = get_logger(__name__)


class S3StorageZip(S3Storage):

    def _upload_gzipped(self, key, fp, compressed_fp=None, show_progress=False):
        if compressed_fp is None:
            compressed_fp = BytesIO()

        file_size = total_len(fp)
        if file_size > 0:
            with GzipFile(fileobj=compressed_fp, mode='wb') as gzipped_fp:
                shutil.copyfileobj(fp, gzipped_fp)

            compressed_fp.seek(0)
            compressed_file_size = total_len(compressed_fp)

            ratio = compressed_file_size / file_size if file_size != 0 else 0

            logger.info(
                f"File {key} was compressed from "
                f"{size_to_human(file_size)} to {size_to_human(compressed_file_size)} ({ratio:.2%})")

            self._client.upload_fileobj(
                Fileobj=compressed_fp,
                Bucket=self._bucket,
                Key=f"{key}.gz",
                Config=self._upload_onfig,
                Callback=ProgressBar(caption='File upload process',
                                     total=compressed_file_size) if show_progress else None
            )

    def upload_file(self, local_file_path: str, remote_file_path: str, /, **kwargs) -> None:
        show_progress: bool = kwargs.pop('show_progress', True)

        file_size = get_file_size(local_file_path)
        if file_size > 0:
            with open(local_file_path, mode='rb') as file:
                self._upload_gzipped(key=remote_file_path, fp=file, show_progress=show_progress)
        else:
            super(S3StorageZip, self).upload_file(local_file_path, remote_file_path, **kwargs)

    def download_file(self, local_file_path: str, remote_file_path: str, /, **kwargs) -> None:
        super(S3StorageZip, self).download_file(local_file_path, remote_file_path, **kwargs)
