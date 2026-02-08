# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations
from avro.schema import Schema
from avrokit.url.utils import flatten_urls
from ..url import URL
from avro.datafile import NULL_CODEC, DataFileWriter
from avro.io import DatumWriter
from contextlib import contextmanager
from typing import (
    Generator,
    IO,
    Any,
    Protocol,
    Self,
    Sequence,
    Literal,
    Mapping,
)
from datetime import datetime
from itertools import groupby
from datetime_truncate import truncate as truncate_datetime  # type: ignore
import re
import os


class Appendable(Protocol):
    def append(self, datum: object) -> None:
        """
        Appends a record to the writer.

        :param record: The record to append.
        """
        ...


@contextmanager
def avro_writer(
    url: URL, schema: Schema | None = None, codec: str = NULL_CODEC
) -> Generator[DataFileWriter, None, None]:
    """
    Opens an Avro DataFileWriter for the given URL and schema.

    :param url: The URL of the Avro file to write.
    :param schema: The Avro schema to use for writing.
    :return: A DataFileWriter object.
    """
    if "b" not in url.mode:
        raise ValueError("URL must be opened in binary mode.")
    elif url.exists() and url.size() > 0 and "a" in url.mode:
        # N.b. setting writers_schema to None reuses the schema from the existing file
        with url as f, DataFileWriter(
            f, DatumWriter(), writers_schema=None, codec=codec
        ) as writer:
            yield writer
    elif schema is None:
        raise ValueError("Schema must be provided for new files.")
    else:
        # Pass the schema to the writer in the general case
        with url as f, DataFileWriter(f, DatumWriter(), schema, codec=codec) as writer:
            yield writer


class PartitionedAvroWriter:
    def __init__(self, url: URL, schema: Schema, codec: str = NULL_CODEC) -> None:
        if not url.mode == "wb":
            raise ValueError("URL must be opened in binary write mode.")
        self.url = url
        self.schema = schema
        self.codec = codec
        self.current_url: URL | None = None
        self.current_url_stream: IO[Any] | None = None
        self.current_writer: DataFileWriter | None = None

    def find_last_filename(self) -> str | None:
        """
        Finds the last filename in the partitioned Avro files.

        Note: Files must be lexicographically sortable.

        :return: The last filename or None if no files exist.
        """
        # Always try to expand - this handles wildcards correctly
        expanded_urls = self.url.expand()
        if not expanded_urls or len(expanded_urls) == 0:
            return None
        filenames = sorted([os.path.basename(url.url) for url in expanded_urls])
        return filenames[-1]

    @staticmethod
    def next_filename(previous: str | None) -> str:
        """
        Generates the next filename for the Avro file.

        :return: The next filename.
        """
        i = 0
        if previous:
            match = re.search(r"part-(\d+)", previous)
            if not match:
                raise ValueError("Invalid filename format: " + previous)
            i = int(match.group(1)) + 1
        return f"part-{i:05d}.avro"

    def _open_writer(self) -> DataFileWriter:
        if self.current_writer:
            self.current_writer.close()
        last_filename = self.find_last_filename()
        next_filename = self.next_filename(last_filename)
        self.current_url = self.url.with_path(next_filename)
        self.current_url_stream = self.current_url.open()
        self.current_writer = DataFileWriter(
            self.current_url_stream,
            DatumWriter(),
            self.schema,
            codec=self.codec,
        )
        return self.current_writer

    def open(self) -> Self:
        if not self.current_writer:
            self._open_writer()
        return self

    def close(self) -> None:
        if self.current_writer:
            self.current_writer.close()
            self.current_writer = None
        self.current_url_stream = None
        self.current_url = None

    def __enter__(self) -> Self:
        return self.open()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def append(self, datum: object, flush: bool = False) -> Self:
        if not self.current_writer:
            raise ValueError("Writer is not initialized.")
        self.current_writer.append(datum)
        if flush:
            self.current_writer.flush()
        return self

    def flush(self) -> Self:
        if not self.current_writer:
            raise ValueError("Writer is not initialized.")
        self.current_writer.flush()
        return self

    def roll(self) -> Self:
        if not self.current_writer:
            raise ValueError("Writer is not initialized.")
        self._open_writer()
        return self


class TimePartitionedAvroWriter(PartitionedAvroWriter):
    format = "%Y-%m-%d_%H-%M-%S"

    def __init__(self, url: URL, schema: Schema) -> None:
        super().__init__(url, schema)

    @classmethod
    def next_filename(cls, _: str | None) -> str:
        """
        Generates the next filename for the Avro file based on the current time.

        :return: The next filename.
        """
        return datetime.now().strftime(cls.format)

    @classmethod
    def group_time_partitions(
        cls,
        src: URL | Sequence[URL],
        time_resolution: Literal["hour", "day"],
        expand_src: bool = True,
    ) -> Mapping[datetime, Sequence[URL]]:
        src_urls_with_time = sorted(
            [
                (
                    truncate_datetime(
                        datetime.strptime(os.path.basename(url.url), cls.format),
                        time_resolution,
                    ),
                    url,
                )
                for url in flatten_urls(src, expand=expand_src)
            ],
            key=lambda x: x[0],
        )
        return {
            ts: sorted([url for _, url in group], key=lambda x: x.url)
            for ts, group in groupby(src_urls_with_time, key=lambda x: x[0])
        }
