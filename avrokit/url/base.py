# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Sequence, IO, Any
from urllib.parse import urlparse


class URL(ABC):
    def __init__(self, url: str, mode: str = "r") -> None:
        self.url = url
        self.mode = mode
        self.parsed_url = urlparse(url)

    def __repr__(self) -> str:
        return self.url

    def __str__(self) -> str:
        return self.url

    @abstractmethod
    def expand(self) -> Sequence[URL]:
        """
        Expands the URL into all the URLs that represent concrete resources.
        """
        ...

    @abstractmethod
    def delete(self) -> None:
        """
        Deletes the resources at the URL and any sub-URLs.
        """
        ...

    @abstractmethod
    def exists(self) -> bool:
        """
        Returns True if the URL exists, False otherwise.
        """
        ...

    @abstractmethod
    def size(self) -> int:
        """
        Returns the size of the URL in bytes.
        """
        ...

    @abstractmethod
    def open(self) -> IO[Any]:
        """
        Opens the URL for reading/writing.
        """
        ...

    @abstractmethod
    def close(self) -> None:
        """
        Closes the URL.
        """
        ...

    def __enter__(self) -> IO[Any]:
        """
        Enter the context to read/write this URL.
        """
        return self.open()

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the context to read/write this URL and handle exceptions if necessary.
        """
        self.close()

    @abstractmethod
    def with_mode(self, mode) -> URL:
        """
        Returns a copy of the URL with the specified mode.
        """
        ...

    @abstractmethod
    def with_path(self, path) -> URL:
        """
        Returns a copy of the URL with the specified mode.
        """
        ...

    def _append_path(self, path: str) -> str:
        head = self.parsed_url.path
        if head.endswith("/"):
            head = head[:-1]
        tail = path
        if tail.startswith("/"):
            tail = tail[1:]
        return head + "/" + tail
