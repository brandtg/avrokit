# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import io
import os
import tempfile
from google.api_core.exceptions import NotFound
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage  # type: ignore
from typing import Any, Sequence, cast, override, IO
from contextlib import contextmanager

from .base import URL


def _create_client() -> storage.Client:
    # Configure client options
    client_options = {}
    api_endpoint = os.getenv("GOOGLE_CLOUD_STORAGE_API_ENDPOINT")
    if api_endpoint:
        client_options["api_endpoint"] = api_endpoint
    use_anonymous_credentials = (
        os.getenv("GOOGLE_CLOUD_STORAGE_USE_ANONYMOUS_CREDENTIALS") == "true"
    )
    # Create a Google Cloud Storage client
    client = storage.Client(
        client_options=client_options,
        credentials=AnonymousCredentials() if use_anonymous_credentials else None,
    )
    return client


@contextmanager
def google_cloud_storage_client() -> storage.Client:
    client = _create_client()
    try:
        yield client
    finally:
        client.close()


class GoogleCloudStorageURL(URL):
    def __init__(self, url: str, mode: str = "rb") -> None:
        super().__init__(url, mode)
        self.bucket = self.parsed_url.netloc
        self.path = self.parsed_url.path.lstrip("/")
        self._current_client: storage.Client | None = None
        self._current_remote: storage.Blob | None = None
        self._current_local: tempfile._TemporaryFileWrapper | None = None
        self._current_local_stream: IO[Any] | None = None

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, GoogleCloudStorageURL):
            return False
        return self.url == value.url

    @override
    def expand(self) -> Sequence[URL]:
        with google_cloud_storage_client() as client:
            try:
                path = self.path
                if not path.endswith("/"):
                    # N.b. we need to add a trailing slash to list all blobs in the directory and not
                    # just the file. If it is a file, we'll get an empty list, and that will trigger
                    # returning self.
                    path += "/"
                bucket = client.bucket(self.bucket)
                blobs = bucket.list_blobs(prefix=path)
                urls = [
                    GoogleCloudStorageURL(
                        f"gs://{self.bucket}/{blob.name}", mode=self.mode
                    )
                    for blob in blobs
                ]
                if not urls:
                    return [self]
                return urls
            except NotFound:
                # N.b. if the bucket doesn't exist, we can't expand it
                return [self]

    @override
    def delete(self) -> None:
        with google_cloud_storage_client() as client:
            bucket = client.bucket(self.bucket)
            blob = bucket.blob(self.path)
            if blob.exists():
                blob.delete()
            else:
                raise FileNotFoundError(
                    f"Blob {self.path} does not exist in bucket {self.bucket}."
                )

    @override
    def exists(self) -> bool:
        with google_cloud_storage_client() as client:
            bucket = client.bucket(self.bucket)
            blob = bucket.blob(self.path)
            return blob.exists()

    @override
    def size(self) -> int:
        with google_cloud_storage_client() as client:
            bucket = client.bucket(self.bucket)
            blob = bucket.blob(self.path)
            if not blob.exists():
                return 0
            blob.reload()  # N.b. loads metadata including size
            return blob.size if blob.size is not None else 0

    @override
    def open(self) -> IO[Any]:
        client = _create_client()
        bucket = client.bucket(self.bucket)
        blob = bucket.blob(self.path)
        tmpfile = tempfile.NamedTemporaryFile(delete=False, mode="w+b")
        self._current_client = client
        self._current_remote = blob
        self._current_local = tmpfile
        self._current_local_stream = self._current_local
        # Download to file if r/rb mode, or if append mode (to preserve existing content on failure)
        if "r" in self.mode or ("a" in self.mode and blob.exists()):
            blob.download_to_file(tmpfile)
            tmpfile.seek(0)
        if "a" in self.mode:
            tmpfile.seek(0, 2)
        if "b" not in self.mode:
            self._current_local_stream = io.TextIOWrapper(tmpfile, encoding="utf-8")
        stream = cast(IO[Any], self._current_local_stream)
        return stream

    @override
    def close(self) -> None:
        # Close the local file
        if (
            self._current_local
            and self._current_local_stream
            and self._current_remote
            and self._current_client
        ):
            try:
                # Flush the local file to ensure all data is written
                if not self._current_local.closed:
                    self._current_local_stream.flush()
                    self._current_local.flush()
                # Upload the local file to GCS if "w" or "a" mode
                if "w" in self.mode or "a" in self.mode:
                    self._current_remote.upload_from_filename(self._current_local.name)
            finally:
                # Cleanup resources
                self._current_local.close()  # N.b. deletes on close
                self._current_client.close()
                self._current_local = None
                self._current_local_stream = None
                self._current_remote = None
                self._current_client = None

    @override
    def with_mode(self, mode) -> URL:
        return GoogleCloudStorageURL(self.url, mode=mode)

    @override
    def with_path(self, path) -> URL:
        return GoogleCloudStorageURL(self._append_path(path), mode=self.mode)
