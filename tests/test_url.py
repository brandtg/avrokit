# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import pytest
import tempfile
import os
from avrokit.url import parse_url, FileURL, create_url_mapping
from avrokit.url.utils import flatten_urls


@pytest.fixture(scope="class")
def tmpdir():
    with tempfile.TemporaryDirectory() as tmpdir:
        for i in range(10):
            with open(f"{tmpdir}/file_{i}.txt", "w") as f:
                f.write(f"Content of file {i}")
        yield tmpdir


class TestParseUrl:
    def test_file(self):
        url = parse_url("file:///path/to/file.txt")
        assert isinstance(url, FileURL)

    def test_file_no_scheme(self):
        url = parse_url("/path/to/file.txt")
        assert isinstance(url, FileURL)

    def test_file_no_scheme_relative(self):
        url = parse_url("./file.txt")
        assert isinstance(url, FileURL)


class TestCreateUrlMapping:
    def test_exists(self, tmpdir):
        mapping = create_url_mapping(
            FileURL(f"file://{tmpdir}"),
            FileURL(f"file://{tmpdir}_out"),
        )
        assert len(mapping) == 10
        for src_url, dst_url in mapping:
            assert os.path.basename(src_url.url) == os.path.basename(dst_url.url)
            assert os.path.dirname(dst_url.parsed_url.path) == f"{tmpdir}_out"


class TestFlattenUrls:
    def test_scalar_file(self, tmpdir):
        urls = flatten_urls(FileURL(f"file://{tmpdir}/file_0.txt"))
        assert urls == [FileURL(f"file://{tmpdir}/file_0.txt")]

    def test_scalar_dir(self, tmpdir):
        urls = flatten_urls(FileURL(f"file://{tmpdir}"))
        assert [u.url for u in urls] == [
            f"file://{tmpdir}/file_{i}.txt" for i in range(10)
        ]

    def test_list(self, tmpdir):
        urls = flatten_urls(
            [
                FileURL(f"file://{tmpdir}/file_0.txt"),
                FileURL(f"file://{tmpdir}/file_1.txt"),
            ]
        )
        assert urls == [
            FileURL(f"file://{tmpdir}/file_0.txt"),
            FileURL(f"file://{tmpdir}/file_1.txt"),
        ]

    def test_none(self):
        urls = flatten_urls(None)
        assert urls == []

    def test_empty_list(self):
        urls = flatten_urls([])
        assert urls == []

    def test_mixed_file(self, tmpdir):
        urls = flatten_urls(
            [
                FileURL(f"file://{tmpdir}/file_0.txt"),
                None,
                FileURL(f"file://{tmpdir}/file_1.txt"),
            ]
        )
        assert urls == [
            FileURL(f"file://{tmpdir}/file_0.txt"),
            FileURL(f"file://{tmpdir}/file_1.txt"),
        ]

    def test_mixed_dir(self, tmpdir):
        urls = flatten_urls(
            [
                FileURL(f"file://{tmpdir}"),
                None,
            ]
        )
        assert [u.url for u in urls] == [
            f"file://{tmpdir}/file_{i}.txt" for i in range(10)
        ]

    def test_duplicates_file(self, tmpdir):
        urls = flatten_urls(
            [
                FileURL(f"file://{tmpdir}/file_0.txt"),
                FileURL(f"file://{tmpdir}/file_0.txt"),
            ]
        )
        assert urls == [
            FileURL(f"file://{tmpdir}/file_0.txt"),
        ]

    def test_duplicates_dir(self, tmpdir):
        urls = flatten_urls(
            [
                FileURL(f"file://{tmpdir}"),
                FileURL(f"file://{tmpdir}"),
            ]
        )
        assert [u.url for u in urls] == [
            f"file://{tmpdir}/file_{i}.txt" for i in range(10)
        ]
