#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import logging
import argparse

from .base import Tool
from .stats import StatsTool
from .getschema import GetSchemaTool
from .tojson import ToJsonTool
from .concat import ConcatTool
from .cat import CatTool
from .getmeta import GetMetaTool
from .repair import RepairTool
from .httpserver import HttpServerTool
from .toparquet import ToParquetTool
from .fromparquet import FromParquetTool
from .partition import PartitionTool
from .filesort import FileSortTool
from .count import CountTool

# TODO fromjson  Reads JSON records and writes an Avro data file. (requires --schema or --schema-file)
# TODO Infer schema tool

TOOLS: list[Tool] = [
    CatTool(),
    ConcatTool(),
    FromParquetTool(),
    GetMetaTool(),
    GetSchemaTool(),
    HttpServerTool(),
    PartitionTool(),
    RepairTool(),
    StatsTool(),
    ToJsonTool(),
    ToParquetTool(),
    FileSortTool(),
    CountTool(),
]


def select_tool(tool_name: str):
    for tool in TOOLS:
        if tool.name() == tool_name:
            return tool
    raise ValueError(f"Tool {tool_name} not found.")


def configure_tools(subparsers: argparse._SubParsersAction) -> None:
    for tool in TOOLS:
        tool.configure(subparsers)


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        prog="avrokit.tools",
        description="Tools for working with Avro data.",
    )
    parser.add_argument("--debug", action="store_true")
    subparsers = parser.add_subparsers(dest="tool", required=True)
    configure_tools(subparsers)
    args = parser.parse_args()
    # Configure logging
    logging.basicConfig(
        format="%(levelname)s:%(message)s",
        level=logging.DEBUG if args.debug else logging.INFO,
    )
    try:
        # Run the selected tool
        tool = select_tool(args.tool)
        tool.run(args)
    except argparse.ArgumentError as e:
        parser.error(e.message)


if __name__ == "__main__":
    main()
