# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import argparse
import logging
import random
from typing import Sequence
from avro.schema import Schema
from avrokit.io.schema import read_avro_schema_from_first_nonempty_file
from avrokit.io.writer import avro_writer
from ..io import avro_reader
from ..url import parse_url, flatten_urls, URL


class CatTool:
    """
    Extracts samples from Avro files
    """

    logger = logging.getLogger(__name__)

    def name(self) -> str:
        return "cat"

    def configure(self, subparsers: argparse._SubParsersAction) -> None:
        parser = subparsers.add_parser(
            self.name(),
            help="Extracts samples from Avro files",
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
            "--offset",
            type=int,
            default=0,
            help="Offset to start sampling from.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            help="Number of records to sample.",
        )
        parser.add_argument(
            "--samplerate",
            type=float,
            help="Rate at which records will be collected (0.0 to 1.0).",
        )
        parser.add_argument(
            "--codec",
            choices=["null", "deflate"],
            default="null",
            help="Codec to use for compression.",
        )

    def should_sample(self, samplerate: float | None) -> bool:
        return samplerate is None or random.random() <= samplerate

    def sample(
        self,
        urls: Sequence[URL],
        output_url: URL,
        schema: Schema,
        codec: str,
        offset: int = 0,
        limit: int | None = None,
        samplerate: float | None = None,
    ) -> None:
        with avro_writer(output_url.with_mode("wb"), schema, codec=codec) as writer:
            index = 0
            for url in urls:
                self.logger.debug("Reading %s", url)
                with avro_reader(url.with_mode("rb")) as reader:
                    for record in reader:
                        if limit is not None and index >= limit:
                            return
                        if index >= offset and self.should_sample(samplerate):
                            writer.append(record)
                        index += 1

    def run(self, args: argparse.Namespace) -> None:
        urls = flatten_urls([parse_url(url) for url in args.url])
        output_url = parse_url(args.output_url)
        schema = read_avro_schema_from_first_nonempty_file(urls)
        if schema is None:
            raise ValueError("No non-empty Avro files found.")
        self.sample(
            urls,
            output_url,
            schema,
            codec=args.codec,
            offset=args.offset,
            limit=args.limit,
            samplerate=args.samplerate,
        )
        self.logger.debug("Wrote %s", output_url)
