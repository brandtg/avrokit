# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import argparse
import logging
from ..url import URL, parse_url
from ..io import (
    avro_reader,
    read_avro_schema_from_first_nonempty_file,
    PartitionedAvroWriter,
)

logger = logging.getLogger(__name__)


class PartitionTool:
    def name(self) -> str:
        return "partition"

    def configure(self, subparsers: argparse._SubParsersAction) -> None:
        parser = subparsers.add_parser(
            self.name(),
            help="Partitions an Avro data file into multiple files.",
        )
        parser.add_argument("input_url", help="Input data file.")
        parser.add_argument("output_url", help="Partitioned output data file.")
        parser.add_argument(
            # TODO Support partitioning by target partition size
            # In that case, either --count or --size must be specified.
            # But specifying required=True for now to avoid confusion.
            "-c",
            "--count",
            type=int,
            help="Number of partitions.",
            required=True,
        )
        parser.add_argument(
            "-f",
            "--force",
            action="store_true",
            help="Force overwrite of existing output files.",
        )

    def compute_partition_size(self, base_url: URL, count_partitions: int) -> int:
        size = 0
        for url in base_url.expand():
            size += url.size()
        logger.info("Total size of %s: %d bytes", base_url, size)
        partition_size = size // count_partitions
        logger.info("Partition size: %d bytes", partition_size)
        return partition_size

    def partition_avro(
        self,
        base_input_url: URL,
        base_output_url: URL,
        count_partitions: int,
        force: bool = False,
    ) -> None:
        # Check if output files already exist (handle wildcards)
        existing_files = base_output_url.expand()
        if existing_files:
            if force:
                base_output_url.delete()
            else:
                raise ValueError(
                    f"Output URL {base_output_url} already exists. Please delete it first."
                )
        # Read the schema from the first non-empty file
        input_urls = base_input_url.expand()
        schema = read_avro_schema_from_first_nonempty_file(input_urls)
        if schema is None:
            raise ValueError("No valid Avro schema found in input files.")
        # Compute the target partition size
        size_target = self.compute_partition_size(base_input_url, count_partitions)
        size_cur = 0
        # Iterate input files and write to output files
        with PartitionedAvroWriter(base_output_url.with_mode("wb"), schema) as writer:
            logger.info("Writing to %s", writer.current_url)
            for input_url in input_urls:
                # Track position in the current file
                pos_cur = 0
                pos_last = 0
                # Open the input file for reading
                with avro_reader(input_url.with_mode("rb")) as reader:
                    for record in reader:
                        # Write the record to the output URL
                        writer.append(record)
                        # Track the current partition size
                        pos_cur = reader._reader.tell()
                        record_size = pos_cur - pos_last
                        size_cur += record_size
                        pos_last = pos_cur
                        # Roll over to the next partition if the size exceeds the target
                        if size_cur >= size_target:
                            writer.roll()
                            size_cur = 0
                            logger.info("Writing to %s", writer.current_url)

    def run(self, args: argparse.Namespace) -> None:
        self.partition_avro(
            base_input_url=parse_url(args.input_url),
            base_output_url=parse_url(args.output_url),
            count_partitions=args.count,
            force=args.force,
        )
