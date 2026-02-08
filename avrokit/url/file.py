# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

from .base import URL
from typing import Any, Sequence, override, IO
import os
import shutil
import glob


class FileURL(URL):
    def __init__(self, url: str, mode: str = "rb") -> None:
        super().__init__(url, mode)
        self.prefix = "file://" if self.parsed_url.scheme == "file" else ""
        self.path = self.parsed_url.path
        self.fh: IO[Any] | None = None

    @override
    def expand(self) -> Sequence[URL]:
        # Check if the path contains wildcard characters
        if "*" in self.path or "?" in self.path or "[" in self.path:
            # Use glob to expand the pattern
            matches = glob.glob(self.path)
            if not matches:
                # No matches found, return empty list
                return []
            # Return sorted list of FileURL objects for all matches
            return [
                FileURL(self.prefix + match, mode=self.mode)
                for match in sorted(matches)
            ]

        # If it's a regular file or doesn't exist (no expansion needed)
        if os.path.isfile(self.path) or not os.path.exists(self.path):
            return [self]

        # If it's a directory, walk it and return all files
        acc = []
        for root, _, filenames in os.walk(self.path):
            for filename in sorted(filenames):
                acc.append(
                    FileURL(self.prefix + os.path.join(root, filename), mode=self.mode)
                )
        return acc

    @override
    def delete(self) -> None:
        # If it's a wildcard pattern, expand and delete all matching files
        if "*" in self.path or "?" in self.path or "[" in self.path:
            expanded = self.expand()
            for url in expanded:
                url.delete()
        elif os.path.isdir(self.path):
            shutil.rmtree(self.path)
        else:
            os.remove(self.path)

    @override
    def exists(self) -> bool:
        return os.path.exists(self.path)

    @override
    def size(self) -> int:
        if os.path.isdir(self.path):
            return sum(
                os.path.getsize(os.path.join(root, filename))
                for root, _, filenames in os.walk(self.path)
                for filename in filenames
            )
        else:
            return os.path.getsize(self.path)

    @override
    def open(self) -> IO[Any]:
        if os.path.isdir(self.path):
            raise ValueError(f"Cannot open directory {self.path} for reading/writing.")
        if "w" in self.mode or "a" in self.mode:
            dirname = os.path.dirname(self.path)
            os.makedirs(dirname, exist_ok=True)
        self.fh = open(self.path, mode=self.mode)
        return self.fh

    @override
    def close(self) -> None:
        if self.fh and not self.fh.closed:
            self.fh.close()

    @override
    def with_mode(self, mode: str) -> URL:
        return FileURL(self.url, mode=mode)

    @override
    def with_path(self, path: str) -> URL:
        # If the current path contains wildcards, replace the wildcard pattern with the new path
        if "*" in self.path or "?" in self.path or "[" in self.path:
            # Get the directory part (before the wildcard)
            dir_part = os.path.dirname(self.path)
            # Create new path by joining directory with the new filename
            new_path = os.path.join(dir_part, path)
            return FileURL(self.prefix + new_path, mode=self.mode)
        # Otherwise, append as normal
        return FileURL(self.prefix + self._append_path(path), mode=self.mode)

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, FileURL):
            return False
        return self.url == value.url
