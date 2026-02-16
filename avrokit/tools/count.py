# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import argparse
import logging
import avro.errors
import avro.datafile
from typing import Sequence
from ..io import avro_reader
from ..url import parse_url, flatten_urls, URL


class CountTool:
    """
    A tool to count the number of records in an Avro file.
    """

    logger = logging.getLogger(__name__)

    def name(self) -> str:
        return "count"

    def configure(self, subparsers: argparse._SubParsersAction) -> None:
        parser = subparsers.add_parser(
            self.name(),
            help="Counts the number of records in an Avro file.",
        )
        parser.add_argument(
            "url",
            nargs="+",
            help="URL of the input Avro data file(s).",
        )

    def fast_count_records(self, reader: avro.datafile.DataFileReader) -> int:
        reader._read_header()
        sync_marker = reader.sync_marker
        total = 0
        while True:
            try:
                # Read block count and size
                block_count = reader.raw_decoder.read_long()
                block_size = reader.raw_decoder.read_long()
                total += block_count
                # Skip over the block data
                reader.reader.seek(block_size, 1)
                # Check sync marker
                marker = reader.reader.read(16)
                if marker != sync_marker:
                    raise avro.errors.AvroException(
                        f"Sync marker does not match (after {total} records)"
                    )
            except EOFError:
                break  # End of file reached
            except avro.errors.AvroException as e:
                if "Sync marker does not match" in str(e):
                    self.logger.warning(
                        "File may still be open for writing (sync mismatch after %d records). "
                        "Counting records up to last valid sync marker.",
                        total,
                    )
                    break
                if "Read 0 bytes, expected 1 bytes" in str(e):
                    break  # Normal end of file for Avro
                self.logger.error(f"Avro error while reading block: {e}")
                raise
            except Exception as e:
                self.logger.exception(f"Unexpected error: {e}")
                raise
        return total

    def count(self, input_urls: Sequence[URL]) -> int:
        total_count = 0
        for url in input_urls:
            self.logger.debug("Reading %s", url)
            with avro_reader(url.with_mode("rb")) as reader:
                total_count += self.fast_count_records(reader)
        return total_count

    def run(self, args: argparse.Namespace) -> None:
        urls = flatten_urls([parse_url(url) for url in args.url])
        total_count = self.count(urls)
        print(total_count)
