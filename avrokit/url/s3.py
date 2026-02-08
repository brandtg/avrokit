# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import boto3
import tempfile
import io
from mypy_boto3_s3 import S3Client
from .base import URL
from typing import Any, Sequence, override, IO, cast
from contextlib import contextmanager


@contextmanager
def s3_client():
    client = boto3.client("s3")
    try:
        yield client
    finally:
        client.close()


class S3URL(URL):
    def __init__(self, url: str, mode: str = "rb") -> None:
        super().__init__(url, mode)
        self.bucket = self.parsed_url.netloc
        self.path = self.parsed_url.path.lstrip("/")
        self._current_client: S3Client | None = None
        self._current_local: tempfile._TemporaryFileWrapper | None = None
        self._current_local_stream: IO[Any] | None = None

    @override
    def expand(self) -> Sequence[URL]:
        with s3_client() as client:
            path = self.path
            if not path.endswith("/"):
                # N.b. we need to add a trailing slash to list all objects in the directory and not
                # just the file. If it is a file, we'll get an empty list, and that will trigger
                # returning self.
                path += "/"
            urls = []
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=path):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        key = obj.get("Key")
                        if key:
                            urls.append(S3URL(self._append_path(key), mode=self.mode))
            if not urls:
                # N.b. we also include URLs that don't exist yet here because there is nothing to expand
                return [self]
            return urls

    def _is_404(self, client: S3Client, e: Exception) -> bool:
        if isinstance(e, client.exceptions.ClientError):
            return e.response.get("Error", {}).get("Code") == "404"
        return False

    @override
    def delete(self) -> None:
        with s3_client() as client:
            try:
                client.delete_object(Bucket=self.bucket, Key=self.path)
            except client.exceptions.ClientError as e:
                if not self._is_404(client, e):
                    raise e

    @override
    def exists(self) -> bool:
        with s3_client() as client:
            try:
                client.head_object(Bucket=self.bucket, Key=self.path)
                return True
            except client.exceptions.ClientError as e:
                if self._is_404(client, e):
                    return False
                raise e

    @override
    def size(self) -> int:
        with s3_client() as client:
            try:
                response = client.head_object(Bucket=self.bucket, Key=self.path)
                return response["ContentLength"]
            except client.exceptions.ClientError as e:
                if self._is_404(client, e):
                    return 0
                raise e

    @override
    def open(self) -> IO[Any]:
        client = boto3.client("s3")
        tmpfile = tempfile.NamedTemporaryFile(delete=False, mode="w+b")
        self._current_client = client
        self._current_local = tmpfile
        self._current_local_stream = self._current_local
        # Download to file if "r" or "rb" mode
        if "r" in self.mode:
            # N.b. always writes in binary mode
            client.download_fileobj(self.bucket, self.path, tmpfile)
            tmpfile.seek(0)
            if "b" not in self.mode:
                # So if the user wants to read text, we need to decode it
                self._current_local_stream = io.TextIOWrapper(tmpfile, encoding="utf-8")
        elif ("w" in self.mode or "a" in self.mode) and "b" not in self.mode:
            # Same thing when we're writing text
            self._current_local_stream = io.TextIOWrapper(tmpfile, encoding="utf-8")
        stream = cast(IO[Any], self._current_local_stream)
        return stream

    @override
    def close(self) -> None:
        if self._current_local and self._current_local_stream and self._current_client:
            try:
                # Flush the local file to ensure all data is written
                if not self._current_local.closed:
                    self._current_local_stream.flush()
                    self._current_local.flush()
                # Upload the local file to S3 if "w" or "a" mode
                if "w" in self.mode or "a" in self.mode:
                    with open(self._current_local.name, "rb") as f:
                        self._current_client.upload_fileobj(f, self.bucket, self.path)
            finally:
                # Cleanup resources
                self._current_local.close()
                self._current_client.close()
                self._current_local = None
                self._current_client = None

    @override
    def with_mode(self, mode: str) -> URL:
        return S3URL(self.url, mode=mode)

    @override
    def with_path(self, path: str) -> URL:
        return S3URL(self._append_path(path), mode=self.mode)

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, S3URL):
            return False
        return self.url == value.url
