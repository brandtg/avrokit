# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import argparse
import logging
import struct
from typing import Sequence, Tuple
from avro.datafile import DataFileReader
from avro.io import DatumReader, BinaryDecoder, BinaryEncoder
from avrokit.io.schema import read_avro_schema_from_first_nonempty_file
from avrokit.io.writer import avro_writer
from ..io import avro_reader
from ..url import parse_url, flatten_urls, URL


class ConcatTool:
    """
    Concatenates Avro files with a specified codec.
    """

    logger = logging.getLogger(__name__)

    def name(self) -> str:
        return "concat"

    def configure(self, subparsers: argparse._SubParsersAction) -> None:
        parser = subparsers.add_parser(
            self.name(),
            help="Concatenates Avro files with a specified codec.",
        )
        parser.add_argument(
            "url",
            nargs="+",
            help="URL of the input Avro data file(s).",
        )
        parser.add_argument(
            "output_url",
            help="URL of the output Avro data file.",
        )
        parser.add_argument(
            "--record",
            action="store_true",
            help="Concatenate by records (default is by blocks if schemas and codecs match).",
        )
        parser.add_argument(
            "--codec",
            choices=["null", "deflate"],
            default="null",
            help="Codec to use for compression.",
        )

    def concat(self, input_urls: Sequence[URL], output_url: URL, codec: str) -> None:
        self.logger.debug("Concatenating files to %s with codec %s", output_url, codec)
        schema = read_avro_schema_from_first_nonempty_file(input_urls)
        if schema is None:
            raise ValueError("No non-empty Avro files found.")
        self.logger.debug("Using schema: %s", schema)
        count = 0
        with avro_writer(output_url.with_mode("wb"), schema, codec=codec) as writer:
            for url in input_urls:
                self.logger.debug("Reading %s", url)
                with avro_reader(url.with_mode("rb")) as reader:
                    for record in reader:
                        writer.append(record)
                        count += 1
                        if count % 100000 == 0:
                            self.logger.debug("Processed %d records", count)
        self.logger.debug("Processed %d records (done)", count)

    def block_concat(
        self, input_urls: Sequence[URL], output_url: URL, codec: str
    ) -> None:
        self.logger.debug(
            "Block concatenating files to %s with codec %s", output_url, codec
        )

        output_sync_marker = None

        with output_url.with_mode("wb").open() as out_f:
            writer = BinaryEncoder(out_f)

            for i, url in enumerate(input_urls):
                with url.with_mode("rb").open() as in_f:
                    # Initialize DataFileReader to parse the header/metadata and
                    # position the file pointer at the start of the first block.
                    reader = DataFileReader(in_f, DatumReader())
                    input_sync_marker = reader.sync_marker

                    if i == 0:
                        # For the first file, we copy the entire header (bytes 0 to current pos)
                        # to the output file. This establishes the schema and codec.
                        header_size = in_f.tell()
                        in_f.seek(0)
                        header_data = in_f.read(header_size)
                        out_f.write(header_data)

                        # The output file must use the same sync marker as the header we just wrote
                        output_sync_marker = input_sync_marker
                    else:
                        # For subsequent files, DataFileReader has already advanced us past the header.
                        # We just need to ensure we don't re-write the header.
                        pass

                    # Create a decoder for the raw input stream
                    decoder = BinaryDecoder(in_f)

                    while True:
                        # Check for EOF by comparing position to file size
                        # (Robust way to detect end of blocks without triggering read errors)
                        current_pos = in_f.tell()
                        in_f.seek(0, 2)
                        file_size = in_f.tell()
                        in_f.seek(current_pos)

                        if current_pos >= file_size:
                            break

                        try:
                            # Avro Block Structure:
                            # 1. Block Count (long)
                            # 2. Block Size (long)
                            # 3. Compressed Data (bytes)
                            # 4. Sync Marker (16 bytes)

                            block_count = decoder.read_long()
                            block_size = decoder.read_long()

                            # Read raw compressed data
                            block_data = decoder.read(block_size)

                            # Read and discard the input file's sync marker
                            _ = decoder.read(16)

                            # Write to output
                            writer.write_long(block_count)
                            writer.write_long(block_size)
                            out_f.write(block_data)

                            # Critical: Write the OUTPUT file's sync marker, not the input's
                            out_f.write(output_sync_marker)

                        except (StopIteration, EOFError, struct.error):
                            break

    def get_schema_and_codec(self, url: URL) -> Tuple[bytes | None, bytes | None]:
        with avro_reader(url) as reader:
            schema = reader.meta.get("avro.schema")
            codec = reader.meta.get("avro.codec", b"null")
            return schema, codec

    def check_schema_and_codec(self, urls: Sequence[URL], desired_codec: str) -> bool:
        if not urls:
            return True
        base_schema, base_codec = self.get_schema_and_codec(urls[0])
        if base_codec != desired_codec.encode("utf-8"):
            self.logger.debug("Codec mismatch: %s != %s", base_codec, desired_codec)
            return False
        for url in urls[1:]:
            schema, codec = self.get_schema_and_codec(url)
            if schema != base_schema or codec != base_codec:
                self.logger.debug("Schema or codec mismatch in %s", url)
                return False
        self.logger.debug("All schemas and codecs match")
        return True

    def run(self, args: argparse.Namespace) -> None:
        urls = flatten_urls([parse_url(url) for url in args.url])
        output_url = parse_url(args.output_url)
        if not args.record and self.check_schema_and_codec(urls, args.codec):
            self.block_concat(urls, output_url, args.codec)
        else:
            self.concat(urls, output_url, args.codec)
        self.logger.debug("Wrote %s", output_url)
