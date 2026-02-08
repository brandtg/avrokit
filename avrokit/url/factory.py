# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

from .base import URL
from urllib.parse import urlparse


def parse_url(url: str, mode: str = "rb") -> URL:
    """
    Parse a URL string and return an instance of URL.

    :param url: The URL string to parse.
    :return: An instance of URL.
    """
    scheme = urlparse(url).scheme
    if not scheme or scheme == "file":
        from .file import FileURL

        return FileURL(url, mode=mode)
    elif scheme == "gs":
        from .google import GoogleCloudStorageURL

        return GoogleCloudStorageURL(url, mode=mode)
    elif scheme in ("http", "https"):
        from .http import HttpURL

        return HttpURL(url, mode=mode)
    elif scheme == "s3":
        from .s3 import S3URL

        return S3URL(url, mode=mode)
    else:
        raise ValueError(f"Unsupported URL scheme: {scheme}")
