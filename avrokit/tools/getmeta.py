# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import argparse
from avrokit.io.reader import avro_reader
from ..url import parse_url


class GetMetaTool:
    """
    Prints the metadata of an Avro data file to stdout.
    """

    def name(self) -> str:
        return "getmeta"

    def configure(self, subparsers: argparse._SubParsersAction) -> None:
        parser = subparsers.add_parser(
            self.name(),
            help="Prints out metadata of an Avro data file.",
        )
        parser.add_argument("url", help="URL of the Avro data file.")

    def run(self, args: argparse.Namespace) -> None:
        url = parse_url(args.url)
        with avro_reader(url.with_mode("rb")) as reader:
            for key, value in reader.meta.items():
                display: str = (
                    value.decode("utf-8") if isinstance(value, bytes) else value
                )
                print(f"{key}: {display}")
