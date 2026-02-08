# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import pytest
import tempfile
import os
from avrokit.url.file import FileURL


@pytest.fixture(scope="class")
def tmpdir():
    with tempfile.TemporaryDirectory() as tmpdir:
        for i in range(10):
            with open(f"{tmpdir}/file_{i}.txt", "w") as f:
                f.write(f"Content of file {i}")
        yield tmpdir


class TestFileURL:
    def test_expand_directory(self, tmpdir):
        url = FileURL(f"file://{tmpdir}")
        expanded_urls = url.expand()
        assert len(expanded_urls) == 10
        assert all(isinstance(u, FileURL) for u in expanded_urls)
        assert [u.url for u in expanded_urls] == [
            f"file://{tmpdir}/file_{i}.txt" for i in range(10)
        ]

    def test_expand_file(self, tmpdir):
        url = FileURL(f"file://{tmpdir}/file_0.txt")
        expanded_urls = url.expand()
        assert len(expanded_urls) == 1
        assert isinstance(expanded_urls[0], FileURL)
        assert expanded_urls[0].url == f"file://{tmpdir}/file_0.txt"

    def test_exists_directory(self, tmpdir):
        url = FileURL(f"file://{tmpdir}")
        assert url.exists()

    def test_exists_file(self, tmpdir):
        url = FileURL(f"file://{tmpdir}/file_0.txt")
        assert url.exists()
        url = FileURL(f"file://{tmpdir}/non_existent_file.txt")
        assert not url.exists()

    def test_delete_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            os.mkdir(f"{tmp}/dir_to_delete")
            url = FileURL(f"file://{tmp}/dir_to_delete")
            assert url.exists()
            url.delete()
            assert not url.exists()

    def test_delete_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            with open(f"{tmp}/file_to_delete.txt", "w") as f:
                f.write("This file will be deleted.")
            url = FileURL(f"file://{tmp}/file_to_delete.txt")
            assert url.exists()
            url.delete()
            assert not url.exists()

    def test_size_directory(self, tmpdir):
        url = FileURL(f"file://{tmpdir}")
        size = url.size()
        assert size > 0
        assert size == sum(
            os.path.getsize(os.path.join(tmpdir, f)) for f in os.listdir(tmpdir)
        )

    def test_size_file(self, tmpdir):
        url = FileURL(f"file://{tmpdir}/file_0.txt")
        size = url.size()
        assert size > 0
        assert size == os.path.getsize(f"{tmpdir}/file_0.txt")

    def test_read_file(self, tmpdir):
        with FileURL(f"file://{tmpdir}/file_0.txt", mode="r") as f:
            data = f.read()
            assert data == "Content of file 0"

    def test_write_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            with FileURL(f"file://{tmp}/dummy.txt", mode="w") as f:
                f.write("Content of file 0")
            with FileURL(f"file://{tmp}/dummy.txt", mode="r") as f:
                data = f.read()
                assert data == "Content of file 0"

    def test_with_mode(self, tmpdir):
        url = FileURL(f"file://{tmpdir}/file_0.txt", mode="r")
        new_url = url.with_mode("w")
        assert new_url.mode == "w"

    def test_with_path(self, tmpdir):
        url = FileURL(f"file://{tmpdir}")
        new_url = url.with_path("file_0.txt")
        assert new_url.url == f"file://{tmpdir}/file_0.txt"

    def test_with_path_slashes(self, tmpdir):
        url = FileURL(f"file://{tmpdir}/")
        new_url = url.with_path("/file_0.txt")
        assert new_url.url == f"file://{tmpdir}/file_0.txt"
