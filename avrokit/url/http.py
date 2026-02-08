# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

from .base import URL
from typing import Any, Sequence, override, IO
import tempfile
import io
import requests


class HttpURL(URL):
    """
    A class to represent an HTTP URL.
    """

    def __init__(
        self,
        url: str,
        mode: str = "rb",
        write_http_method: str = "POST",
        read_http_method: str = "GET",
        spill_request_to_file: bool = False,
        content_type: str = "application/octet-stream",
    ) -> None:
        super().__init__(url, mode)
        self.write_http_method = write_http_method
        self.read_http_method = read_http_method
        self.spill_request_to_file = spill_request_to_file
        self.content_type = content_type
        self._current_response: requests.Response | None = None
        self._current_request_buffer: IO[Any] | None = None

    @override
    def expand(self) -> Sequence[URL]:
        # N.b. no way to "expand" an HTTP URL, i.e. discover sub resources
        return [self]

    @override
    def delete(self) -> None:
        res = requests.delete(self.url)
        res.raise_for_status()

    @override
    def exists(self) -> bool:
        try:
            response = requests.head(self.url, timeout=5)
            return response.ok
        except requests.RequestException:
            return False

    @override
    def size(self) -> int:
        try:
            response = requests.head(self.url, timeout=5)
            if response.ok:
                return int(response.headers.get("Content-Length", 0))
            return 0
        except requests.RequestException:
            return 0

    @override
    def open(self) -> IO[Any]:
        if "r" in self.mode:
            # Read from the URL and return a file-like object of its content body
            res = requests.request(self.read_http_method, self.url, stream=True)
            res.raise_for_status()
            self._current_response = res
            content: IO[Any] = io.BytesIO(res.content)
            # Wrap in TextIOWrapper if not binary mode
            if "b" not in self.mode:
                content = io.TextIOWrapper(content, encoding="utf-8")
            return content
        else:
            # Create a buffer for the upstream request to write to
            buf: IO[Any] | None = None
            if self.spill_request_to_file:
                # Use a temporary file to store the request body
                buf = tempfile.NamedTemporaryFile(delete=False)
            else:
                # Use an in-memory buffer
                buf = io.BytesIO()
            # Wrap in TextIOWrapper if not binary mode
            if "b" not in self.mode:
                buf = io.TextIOWrapper(buf, encoding="utf-8")
            self._current_request_buffer = buf
            return buf

    @override
    def close(self) -> None:
        # Close the response if it exists
        if self._current_response is not None:
            self._current_response.close()
            self._current_response = None
        # Send request buffer if it exists
        if self._current_request_buffer is not None:
            # Reset the buffer position to the beginning
            self._current_request_buffer.flush()
            self._current_request_buffer.seek(0)
            # Send the HTTP request
            res = requests.request(
                self.write_http_method,
                self.url,
                data=self._current_request_buffer,
                headers={"Content-Type": self.content_type},
            )
            res.raise_for_status()
            # Close the buffer (will delete temp file if used)
            self._current_request_buffer.close()

    @override
    def with_mode(self, mode) -> URL:
        return HttpURL(self.url, mode=mode)

    @override
    def with_path(self, path) -> URL:
        return HttpURL(path, mode=self.mode)

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, HttpURL):
            return False
        return self.url == value.url
