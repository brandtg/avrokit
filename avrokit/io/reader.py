# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations
from ..url import URL
from avro.datafile import DataFileReader
from avro.io import DatumReader
from contextlib import contextmanager
from typing import Generator, Iterator, Sequence, IO, Any, Self, Union, cast


@contextmanager
def avro_reader(url: URL) -> Generator[DataFileReader, None, None]:
    """
    Opens an Avro DataFileReader for the given URL.

    :param url: The URL of the Avro file to read.
    :return: A DataFileReader object.
    """
    with url as f, DataFileReader(f, DatumReader()) as reader:
        yield reader


def avro_records(url: URL) -> Generator[dict[str, Any], None, None]:
    with avro_reader(url) as reader:
        for record in reader:
            yield cast(dict[str, Any], record)


class PartitionedAvroReader:
    def __init__(self, urls: Union[URL, Sequence[URL]]):
        self.urls = [urls] if isinstance(urls, URL) else urls
        self.expanded_urls: list[URL] = []
        self.current_index = 0
        self.current_url: URL | None = None
        self.current_url_stream: IO[Any] | None = None
        self.current_reader: DataFileReader | None = None

    def _open_reader(self) -> DataFileReader:
        if self.current_reader:
            self.current_reader.close()
        if self.current_index >= len(self.expanded_urls):
            raise StopIteration
        self.current_url = self.expanded_urls[self.current_index]
        if not self.current_url:
            raise StopIteration
        self.current_url_stream = self.current_url.open()
        self.current_reader = DataFileReader(self.current_url_stream, DatumReader())
        return self.current_reader

    def open(self) -> Self:
        self.expanded_urls = [
            expanded_url for url in self.urls for expanded_url in url.expand()
        ]
        if len(self.expanded_urls) != 0:
            self._open_reader()
        return self

    def close(self) -> None:
        if self.current_reader:
            self.current_reader.close()
            self.current_reader = None
        self.current_url_stream = None
        self.current_index = 0

    def __enter__(self) -> Self:
        return self.open()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __iter__(self) -> Iterator[object]:
        return self

    def __next__(self) -> object:
        while True:
            try:
                if not self.current_reader:
                    raise StopIteration
                return next(self.current_reader)
            except StopIteration:
                self.current_index += 1
                self._open_reader()
