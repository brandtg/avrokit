# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import argparse
import json
import sys
from datetime import date, datetime, time
from decimal import Decimal

from ..io import avro_reader
from ..url import flatten_urls, parse_url


class AvroJsonEncoder(json.JSONEncoder):
    """Encodes Avro logical types (datetime, date, time, decimal) as JSON."""

    def default(self, o: object) -> object:
        if isinstance(o, datetime | date | time):
            return o.isoformat()
        if isinstance(o, Decimal):
            return str(o)
        return super().default(o)


class ToJsonTool:
    """
    Dumps Avro data file(s) as JSON, record per line.
    """

    def name(self) -> str:
        return "tojson"

    def configure(self, subparsers: argparse._SubParsersAction) -> None:
        parser = subparsers.add_parser(
            self.name(),
            help="Dumps Avro data file(s) as JSON, record per line.",
        )
        parser.add_argument("url", nargs="+", help="URL of the Avro data file.")

    def run(self, args: argparse.Namespace) -> None:
        urls = flatten_urls([parse_url(url) for url in args.url])
        for url in urls:
            with avro_reader(url) as reader:
                for record in reader:
                    json.dump(record, sys.stdout, cls=AvroJsonEncoder)
                    sys.stdout.write("\n")
