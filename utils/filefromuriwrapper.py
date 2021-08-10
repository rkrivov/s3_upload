#  Copyright (c) 2021. by Roman N. Krivov a.k.a. Eochaid Bres Drow


class FileFromURLWrapper(object):
    left_bytes: int

    def __init__(self,
                 file_url: Union[str, bytes] = None,
                 session: Session = None,
                 response: requests.Response = None,
                 chuck_size: int = None,
                 callback: Callable[[Any], Any] = None):

        if not file_url and not response and not session:
            raise AttributeError("You've to pass one of three parameters")

        self.content_length = None
        self.bytes_read = 0

        if not requests:
            self.session = session or requests.Session()
            requested_file = response or self._request_for_file(file_url)

            self.response = requested_file
            self.raw_data = _get_raw_data(requested_file)
        else:
            self.response = response
            self.raw_data = _get_raw_data(response)

        self.total_size = self.len
        self.left_bytes = self.total_size
        self.chunk_size = min(self.total_size, chuck_size if chuck_size else DEFAULT_CHUCK_SIZE)
        self.chunk = self._next_chunk()

        self.set_callback(callback)

    def _next_chunk(self):

        try:
            if isinstance(self.raw_data, S3Object):
                return FileWrapperBytesIO(buffer=self.raw_data.next())
            else:
                return FileWrapperBytesIO(buffer=next_chunk(self.raw_data, chunk_size=self.chunk_size))
        except Exception as ex:
            logging.error(ex, stack_info=False)

        return None

    def close(self):
        if not is_closed(self.chunk):
            close(self.chunk)
            self.chunk = None

        if not is_closed(self.raw_data):
            close(self.chunk)
            self.chunk = None

        if not is_closed(self.response):
            close(self.response)
            self.response = None

        self.raw_data = None

    @property
    def file_url(self):
        if self.response is not None:
            if hasattr(self.response, "url"):
                urn = Urn(self.response.url)
                return urn.quote()
        return ""

    @property
    def len(self):
        if self.content_length is None:
            if hasattr(self.response, 'content_length'):
                content_length = self.response.content_length
            elif hasattr(self.response, '_content_length'):
                content_length = self.response._content_length
            else:
                content_length = self.response.headers.get('content-length', None)

            if content_length is None:
                error_msg = (
                    f"Data from provided URL {self.file_url} is not supported. Lack of "
                    "content-length Header in requested file response."
                )
                raise FileNotSupportedError(error_msg)
            elif not content_length.isdigit():
                error_msg = (
                    f"Data from provided URL {self.file_url} is not supported. content-length"
                    " header value is not a digit."
                )
                raise FileNotSupportedError(error_msg)

            self.content_length = int(content_length)

        return self.content_length

    def _request_for_file(self, file_url):
        """Make call for file under provided URL."""
        response = self.session.get(file_url, stream=True)
        _ = self.len
        return response

    def read(self, chunk_size: int = -1) -> Optional[Union[bytes, memoryview]]:
        """Read file in chunks."""
        if self.chunk is None:
            self.chunk = self._next_chunk()
            if self.chunk is None:
                return None

        if self.chunk.len == 0:
            self.chunk = self._next_chunk()
            read_size = self.chunk.len if self.chunk else 0
            if read_size == 0:
                return None

        if chunk_size == -1:
            chunk_size = 8192

        try:
            # chunk_size = min(chunk_size, self.chunk.len)
            chunk_size = min(max(chunk_size, DEFAULT_CHUCK_SIZE), self.chunk.len)

            chunk = read_data_from(self.chunk, length=chunk_size)

            read_size = total_len(chunk) if chunk else 0
            self.left_bytes -= read_size
            self.bytes_read += read_size

            if self.callback:
                self.callback(self)

            return chunk
        except Exception as ex:
            logging.error(ex, stack_info=False)

        return None

    def set_callback(self, callback: Callable[[Any], Any]):
        self.callback = callback

    @staticmethod
    def create(body: Union[StreamingBody, HTTPResponse],
               chuck_size: int = None,
               callback: Callable[[Any], Any] = None):
        response = None
        if body is not None:
            if hasattr(body, 'raw_stream'):
                response = body.raw_stream
            else:
                response = body

        if response is None:
            raise ValueError(f'The body incorrect.')

        file_wrapper = FileFromURLWrapper(response=response, chuck_size=chuck_size, callback=callback)

        return file_wrapper
