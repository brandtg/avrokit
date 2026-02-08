# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

from ..io import avro_reader
from ..url import parse_url, flatten_urls
from dataclasses import dataclass, field, asdict
from typing import Any, cast
import argparse
import json
import logging
import sys

logger = logging.getLogger(__name__)


@dataclass
class Stats:
    count: int = 0
    count_by_file: dict[str, int] = field(default_factory=dict)
    count_null_by_field: dict[str, int] = field(default_factory=dict)
    size_bytes: int = 0
    size_bytes_by_file: dict[str, int] = field(default_factory=dict)


class StatsTool:
    def name(self) -> str:
        return "stats"

    def configure(self, subparsers: argparse._SubParsersAction) -> None:
        parser = subparsers.add_parser(
            self.name(),
            help="Computes statistics for Avro files.",
        )
        parser.add_argument("url", nargs="+", help="URL of the Avro data file.")

    def run(self, args: argparse.Namespace) -> None:
        stats = Stats()
        urls = flatten_urls([parse_url(url) for url in args.url])
        for url in urls:
            logger.debug("Reading %s", url)
            # Get size of file
            url_size = url.size()
            stats.size_bytes += url_size
            stats.size_bytes_by_file[url.url] = url_size
            # Read Avro file
            with avro_reader(url) as reader:
                for record in reader:
                    record = cast(dict[str, Any], record)
                    # Count total records
                    stats.count += 1
                    # Count records by file
                    if url.url not in stats.count_by_file:
                        stats.count_by_file[url.url] = 0
                    stats.count_by_file[url.url] += 1
                    # Count field-level stats
                    for f, v in record.items():
                        # Count null values
                        if f not in stats.count_null_by_field:
                            stats.count_null_by_field[f] = 0
                        if v is None:
                            stats.count_null_by_field[f] = (
                                stats.count_null_by_field[f] + 1
                            )
        # Print stats as JSON to stdout
        json.dump(asdict(stats), sys.stdout, indent=2)
