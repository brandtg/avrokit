# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

from .base import URL
from .factory import parse_url
from .file import FileURL
from .utils import create_url_mapping, flatten_urls

__all__ = ["URL", "FileURL", "parse_url", "create_url_mapping", "flatten_urls"]
