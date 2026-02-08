# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import tempfile
from avro.datafile import DataFileReader
from avro.io import BinaryDecoder, DatumReader
from avro.schema import Schema
from avrokit import avro_writer
from avrokit.url import create_url_mapping
from dataclasses import dataclass
from typing import IO, Literal, Tuple, Sequence
import argparse
import io
import json
import logging
import sys
from ..url import parse_url, URL

SYNC_SIZE = 16

logger = logging.getLogger(__name__)


@dataclass
class RepairToolReport:
    input_url: str
    output_url: str
    count_blocks: int
    count_corrupt_blocks: int


class RepairTool:
    def name(self) -> str:
        return "repair"

    def configure(self, subparsers: argparse._SubParsersAction) -> None:
        parser = subparsers.add_parser(
            self.name(),
            help="Prints out metadata of an Avro data file.",
        )
        parser.add_argument("input_url", help="URL of the input Avro data file.")
        parser.add_argument("output_url", help="URL of the output Avro data file.")
        parser.add_argument(
            "--report_format",
            choices=["text", "json", "json_pretty"],
            default="text",
            help="Format of the report.",
        )
        parser.add_argument(
            "--dry_run",
            action="store_true",
            help="If set, only scan the file without writing to the output.",
        )

    def read_varint(self, fh: IO[bytes]) -> int:
        """
        Read a variable-length integer using zig-zag encoding from the file handle.
        """
        shift = 0
        result = 0
        while True:
            b = fh.read(1)
            if not b:
                raise EOFError("Unexpected end of file while reading varint.")
            byte = b[0]
            result |= (byte & 0x7F) << shift
            if not (byte & 0x80):
                break
            shift += 7
        return (result >> 1) ^ -(result & 1)  # Decode the zig-zag encoding

    def read_header(self, fh: IO[bytes]) -> Tuple[Schema, str, bytes]:
        """
        Read the Avro file header to extract the schema, codec, and sync marker.
        """
        header_reader = DataFileReader(fh, DatumReader())
        schema = header_reader.datum_reader.writers_schema
        if not schema:
            raise ValueError("Schema is not defined in the Avro file.")
        codec = header_reader.codec
        if codec != "null":
            # TODO Support other codecs
            raise ValueError("Unsupported codec: {}".format(codec))
        sync_marker = header_reader.sync_marker
        return schema, codec, sync_marker

    def scan_to_next_sync_marker(
        self, fh: IO[bytes], sync_marker: bytes, chunk_size: int = 8192
    ) -> bool:
        """
        Scan the file to find the next sync marker.
        """
        sync_marker_len = len(sync_marker)
        buffer = b""
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                return False  # EOF
            buffer += chunk
            idx = buffer.find(sync_marker)
            if idx != -1:
                fh.seek(fh.tell() - len(buffer) + idx + sync_marker_len)
                return True
            buffer = buffer[-(sync_marker_len - 1) :]

    def url_mapping(self, input_url: URL, output_url: URL) -> Sequence[tuple[URL, URL]]:
        if len(input_url.expand()) > 1:
            return create_url_mapping(input_url, output_url)
        else:
            return [(input_url, output_url)]

    def repair(
        self,
        base_input_url: URL,
        base_output_url: URL,
        dry_run: bool = False,
    ) -> Sequence[RepairToolReport]:
        """
        Repair the Avro file by reading the header and blocks, and writing to a new file.

        This function handles block-level corruption by skipping to the next sync marker.

        :param input_url: URL of the input Avro data file, containing potentially corrupt data.
        :param output_url: URL of the output Avro data file, where repaired data will be written.
        :param dry_run: If True, only scan the file without writing to the output.
        :return: A report containing the number of records, blocks, and corrupt records.
        """
        # TODO Handle multiple urls
        # input_urls = input_url.expand()
        # if len(input_url.expand()) > 1:
        #     # Treat output_url as the base URL and get a mapping
        #     create_url_mapping(input_urls, output_url)
        acc: list[RepairToolReport] = []
        for input_url, output_url in self.url_mapping(base_input_url, base_output_url):
            with input_url as f:
                # Read the schema / codec from the input file
                schema, codec, sync_marker = self.read_header(f)
                logger.debug("Schema: %s", schema)
                logger.debug("Codec: %s", codec)
                logger.debug("Sync marker: %s", sync_marker.hex())
                # Use a temporary file if dry_run is set
                if dry_run:
                    tmp = tempfile.NamedTemporaryFile(delete=False)
                    output_url = parse_url(tmp.name, mode="wb")
                    logger.debug("Using temporary file %s for dry run", output_url)
                # Initialize the output file with the same schema and codec
                with avro_writer(output_url, schema, codec=codec) as writer:
                    count_blocks = 0
                    count_corrupt_blocks = 0
                    while True:
                        try:
                            # Read block header
                            block_start = f.tell()
                            block_count = self.read_varint(f)
                            block_size = self.read_varint(f)
                            block_data = f.read(block_size)
                            count_blocks += 1
                            # Check sync marker
                            sync_check = f.read(SYNC_SIZE)
                            if sync_check != sync_marker:
                                count_corrupt_blocks += 1
                                logger.debug(
                                    "Sync marker mismatch at offset %d. Expected %s, found %s",
                                    block_start + block_size,
                                    sync_marker.hex(),
                                    sync_check.hex(),
                                )
                                if not self.scan_to_next_sync_marker(f, sync_marker):
                                    logger.debug("No more sync markers found.")
                                    break
                                continue
                            # Decode and write records in block
                            decoder = BinaryDecoder(io.BytesIO(block_data))
                            datum_reader = DatumReader(schema)
                            for _ in range(block_count):
                                try:
                                    record = datum_reader.read(decoder)
                                    writer.append(record)
                                except Exception as e:
                                    # Record-level corruption
                                    count_corrupt_blocks += 1
                                    logger.debug(
                                        "Error decoding record at block %d, offset %d: %s",
                                        block_start,
                                        f.tell(),
                                        e,
                                    )
                                    break
                        except EOFError:
                            break
                        except Exception as e:
                            # Block-level corruption
                            count_corrupt_blocks += 1
                            logger.debug(
                                "Error reading block at offset %d: %s", f.tell(), e
                            )
                            if not self.scan_to_next_sync_marker(f, sync_marker):
                                logger.debug("No more sync markers found.")
                                break
            if dry_run:
                logger.debug("Deleting temporary file %s", output_url)
                output_url.delete()
            acc.append(
                RepairToolReport(
                    input_url.url,
                    "(dry run)" if dry_run else output_url.url,
                    count_blocks,
                    count_corrupt_blocks,
                )
            )
        return acc

    def format_report(
        self,
        report: RepairToolReport,
        report_format: Literal["text", "json", "json_pretty"],
    ) -> str:
        if report_format == "json":
            return json.dumps(report.__dict__)
        elif report_format == "json_pretty":
            return json.dumps(report.__dict__, indent=2)
        else:
            return "\n".join(
                [
                    f"{report.input_url} -> {report.output_url}",
                    f"\tBlocks: {report.count_blocks}",
                    f"\tCorrupt blocks: {report.count_corrupt_blocks}",
                ]
            )

    def run(self, args: argparse.Namespace) -> None:
        input_url = parse_url(args.input_url, mode="rb")
        output_url = parse_url(args.output_url, mode="wb")
        reports = self.repair(input_url, output_url, dry_run=args.dry_run)
        for report in reports:
            sys.stdout.write(self.format_report(report, args.report_format) + "\n")
