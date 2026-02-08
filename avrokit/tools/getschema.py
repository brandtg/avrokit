# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import argparse
import json
import sys
from ..io import read_avro_schema_from_first_nonempty_file
from ..url import parse_url


class GetSchemaTool:
    """
    Prints the schema of an Avro data file to stdout.
    """

    def name(self) -> str:
        return "getschema"

    def configure(self, subparsers: argparse._SubParsersAction) -> None:
        parser = subparsers.add_parser(
            self.name(),
            help="Prints out schema of an Avro data file.",
        )
        parser.add_argument("url", help="URL of the Avro data file.")

    def run(self, args: argparse.Namespace) -> None:
        urls = parse_url(args.url).expand()
        schema = read_avro_schema_from_first_nonempty_file(urls)
        if schema is None:
            raise ValueError(f"Could not read schema from {args.url}")
        json.dump(schema.to_json(), sys.stdout, indent=2)
