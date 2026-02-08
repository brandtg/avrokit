# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

from typing import Sequence, Union
from avrokit.url.base import URL
import os


def create_url_mapping(src: URL, dst: URL) -> Sequence[tuple[URL, URL]]:
    """
    Create a mapping of source URLs to destination URLs.

    :param src: The source URL to expand.
    :param dst: The destination URL to map to.
    :return: A list of tuples containing the source and destination URLs.
    """
    # Expand the source URL
    src_expanded = src.expand()
    # If there is just one source URL, map directly to the destination
    if len(src_expanded) == 1:
        return [(src, dst)]
    # If there are multiple source URLs, treat dst as a root directory
    acc = []
    for src_url in src_expanded:
        filename = os.path.basename(src_url.url)
        dst_url = dst.with_path(filename)
        acc.append((src_url, dst_url))
    return acc


def flatten_urls(
    urls: Union[None, URL, Sequence[URL | None]], expand: bool = True
) -> Sequence[URL]:
    """
    Flatten a list of URLs, removing None values and expanding if needed.

    :param urls: The URLs to flatten.
    :param expand: Whether to expand the URLs if they are partitioned.
    """
    acc: list[URL] = []
    # Flatten the provided URLs
    if urls:
        if isinstance(urls, URL):
            acc.append(urls)
        elif isinstance(urls, Sequence):
            for url in urls:
                if isinstance(url, URL):
                    acc.append(url)
    # Expand the URLs if needed
    if expand:
        acc = [expanded_url for url in acc for expanded_url in url.expand()]
    # Deduplicate the URLs
    seen: set[str] = set()
    acc_deduped = []
    for url in acc:
        if url.url not in seen:
            acc_deduped.append(url)
            seen.add(url.url)
    return acc_deduped
