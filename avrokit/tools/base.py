# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

from typing import Protocol
import argparse


class Tool(Protocol):
    """
    A protocol for defining tools that can be run from the command line.
    """

    def name(self) -> str:
        """
        Returns the name of the tool.
        """
        ...

    def configure(self, subparsers: argparse._SubParsersAction) -> None:
        """
        Configures the command line arguments for the tool.
        """
        ...

    def run(self, args: argparse.Namespace) -> None:
        """
        Runs the tool with the given command line arguments.
        """
        ...
