# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import argparse
import tempfile
import os
import heapq
from typing import Callable, Any
from avro.datafile import DataFileReader
from avro.io import DatumReader
from avro.schema import Schema
from avrokit.io.reader import avro_reader
from avrokit.io.writer import avro_writer
from ..url import parse_url, URL

DEFAULT_BATCH_SIZE = 1000


class FileSortTool:
    """
    Sorts an Avro file based on a specified field.
    """

    def filesort(
        self,
        input_url: URL,
        output_url: URL,
        sort_fields: list[str],
        reverse: bool = False,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        """
        Sorts the Avro file at input_url and writes the sorted data to output_url.
        """
        with avro_reader(input_url) as reader, tempfile.TemporaryDirectory() as tmp:
            batch = []
            batch_id = 0
            sort_key = self.sort_key(sort_fields)
            schema = reader._datum_reader.writers_schema
            if not schema:
                raise ValueError("Schema not found in the input file.")
            # Write the sorted data to temporary files
            for record in reader:
                batch.append(record)
                if len(batch) >= batch_size:
                    self.sort_and_write_batch(
                        schema, tmp, batch_id, batch, sort_key, reverse=reverse
                    )
                    batch_id += 1
                    batch = []
            if batch:
                self.sort_and_write_batch(
                    schema, tmp, batch_id, batch, sort_key, reverse=reverse
                )
            # Merge sorted temporary files into output file
            with avro_writer(output_url.with_mode("wb"), schema) as writer:
                sorted_iterators = [
                    DataFileReader(
                        open(os.path.join(tmp, batch_file), "rb"), DatumReader()
                    )
                    for batch_file in os.listdir(tmp)
                ]
                for record in heapq.merge(
                    *sorted_iterators,
                    key=sort_key,
                ):
                    writer.append(record)
                for iterator in sorted_iterators:
                    iterator.close()

    def sort_key(self, sort_fields: list[str]) -> Callable[[Any], Any]:
        # TODO Nested fields
        def _sort_key(record: Any) -> Any:
            if not isinstance(record, dict):
                raise ValueError(f"Record is not a dictionary: {record}")
            acc = []
            for sort_field in sort_fields:
                acc.append(record.get(sort_field))
            return tuple(acc)

        return _sort_key

    def sort_and_write_batch(
        self,
        schema: Schema,
        root: str,
        batch_id: int,
        records: list[object],
        sort_key: Callable[[Any], Any],
        reverse: bool = False,
    ) -> None:
        with avro_writer(
            parse_url(os.path.join(root, f"batch_{batch_id}.avro"), "wb"), schema
        ) as writer:
            for record in sorted(records, key=sort_key, reverse=reverse):
                writer.append(record)

    def name(self) -> str:
        return "sort"

    def configure(self, subparsers: argparse._SubParsersAction) -> None:
        parser = subparsers.add_parser(
            self.name(),
            help="Sorts an Avro file based on a specified field.",
        )
        parser.add_argument(
            "input_url",
            help="URL of the input Avro data file.",
        )
        parser.add_argument(
            "output_url",
            help="URL of the output Avro data file.",
        )
        parser.add_argument(
            "sort_field",
            action="append",
            help="Field to sort the Avro data by.",
        )
        parser.add_argument(
            "-r",
            "--reverse",
            action="store_true",
            help="Sort in ascending order.",
        )
        parser.add_argument(
            "-b",
            "--batch_size",
            type=int,
            default=DEFAULT_BATCH_SIZE,
            help="Number of records to sort in memory at a time.",
        )

    def run(self, args: argparse.Namespace) -> None:
        self.filesort(
            parse_url(args.input_url),
            parse_url(args.output_url),
            sort_fields=args.sort_field,
            reverse=args.reverse,
            batch_size=args.batch_size,
        )
