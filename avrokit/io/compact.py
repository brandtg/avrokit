# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

from avrokit.io.reader import avro_reader
from avrokit.url.utils import flatten_urls
from ..url import URL
from .writer import avro_writer
from .schema import read_avro_schema_from_first_nonempty_file
from typing import Union, Sequence


def compact_avro_data(
    src: Union[URL, Sequence[URL]],
    dst: URL,
    expand_src: bool = True,
) -> None:
    """
    Compact Avro data from source URLs into a single destination URL.

    :param src: Source URL(s) to read Avro data from.
    :param dst: Destination URL to write the compacted Avro data to.
    :param expand_src: Whether to expand source URLs if they are partitioned.
    """
    src_urls = flatten_urls(src, expand=expand_src)
    if not src_urls:
        raise ValueError("No source URLs found to compact.")
    schema = read_avro_schema_from_first_nonempty_file(src_urls)
    if schema is None:
        raise ValueError("No Avro schema found in source URLs.")
    # TODO Codec for avro_writer compression
    with avro_writer(dst.with_mode("wb"), schema) as writer:
        for url in src_urls:
            with avro_reader(url.with_mode("rb")) as reader:
                for record in reader:
                    writer.append(record)
