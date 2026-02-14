# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

"""
Additional tests for URL handlers and other edge cases.
"""

import pytest
import tempfile
import os
from avrokit.io import avro_schema
from avrokit.url.factory import parse_url


class TestURLHandlerEdgeCases:
    """Test edge cases in URL handlers."""

    def test_file_url_nonexistent_read(self):
        """Test reading from non-existent file."""
        url = parse_url("/tmp/nonexistent_file_12345.avro")
        with pytest.raises(FileNotFoundError):
            with url.with_mode("rb") as _:
                pass

    def test_file_url_exists_check(self):
        """Test exists() method."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                url = parse_url(tmp.name)
                assert url.exists()

                os.unlink(tmp.name)
                assert not url.exists()
            finally:
                if os.path.exists(tmp.name):
                    os.unlink(tmp.name)

    def test_file_url_size(self):
        """Test size() method."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                # Write some data
                tmp.write(b"Hello, World!")
                tmp.flush()

                url = parse_url(tmp.name)
                assert url.size() == 13
            finally:
                os.unlink(tmp.name)

    def test_file_url_delete(self):
        """Test delete() method."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            pass

        url = parse_url(tmp.name)
        assert url.exists()
        url.delete()
        assert not url.exists()

    def test_file_url_with_mode(self):
        """Test with_mode() method."""
        url = parse_url("/tmp/test.avro", mode="rb")
        assert url.mode == "rb"

        new_url = url.with_mode("wb")
        assert new_url.mode == "wb"
        assert url.mode == "rb"  # Original unchanged

    def test_parse_url_with_explicit_scheme(self):
        """Test parsing URLs with explicit file:// scheme."""
        url = parse_url("file:///tmp/test.avro")
        assert url.url == "file:///tmp/test.avro"

    def test_wildcard_expansion_single_file(self):
        """Test wildcard expansion with single matching file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a single file
            test_file = os.path.join(tmpdir, "test.avro")
            with open(test_file, "w") as f:
                f.write("test")

            # Test wildcard expansion
            pattern = os.path.join(tmpdir, "*.avro")
            url = parse_url(pattern)
            expanded = url.expand()

            assert len(expanded) == 1
            assert expanded[0].url.endswith("test.avro")

    def test_wildcard_expansion_multiple_files(self):
        """Test wildcard expansion with multiple matching files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create multiple files
            for i in range(5):
                test_file = os.path.join(tmpdir, f"test_{i}.avro")
                with open(test_file, "w") as f:
                    f.write("test")

            # Test wildcard expansion
            pattern = os.path.join(tmpdir, "*.avro")
            url = parse_url(pattern)
            expanded = url.expand()

            assert len(expanded) == 5

    def test_wildcard_expansion_no_matches(self):
        """Test wildcard expansion with no matching files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Don't create any files
            pattern = os.path.join(tmpdir, "*.avro")
            url = parse_url(pattern)
            expanded = url.expand()

            assert len(expanded) == 0

    def test_directory_operations(self):
        """Test directory-related operations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Test with directory path
            url = parse_url(tmpdir)
            assert url.exists()

            # Create a file in the directory
            test_file = os.path.join(tmpdir, "test.txt")
            with open(test_file, "w") as f:
                f.write("test")

            # Pattern for files in directory
            pattern = os.path.join(tmpdir, "*.txt")
            url = parse_url(pattern)
            expanded = url.expand()
            assert len(expanded) == 1


class TestCompactOperation:
    """Test the compact operation for Avro files."""

    def test_compact_concatenates_files(self):
        """Test that compact concatenates multiple files."""
        from avrokit.io.compact import compact_avro_data

        schema = avro_schema(
            {
                "type": "record",
                "name": "Test",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "value", "type": "string"},
                ],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create multiple input files
            input_files = []
            for i in range(3):
                input_file = os.path.join(tmpdir, f"input_{i}.avro")
                input_files.append(input_file)
                input_url = parse_url(input_file)

                from avrokit.io import avro_writer

                with avro_writer(input_url.with_mode("wb"), schema) as writer:
                    for j in range(10):
                        writer.append({"id": i * 10 + j, "value": f"value_{i}_{j}"})

            output_file = os.path.join(tmpdir, "output.avro")
            output_url = parse_url(output_file)

            # Compact - should concatenate all files
            input_urls = [parse_url(f) for f in input_files]
            compact_avro_data(input_urls, output_url)

            # Verify
            from avrokit.io import avro_reader

            with avro_reader(output_url.with_mode("rb")) as reader:
                records = list(reader)
                # Should have all 30 records
                assert len(records) == 30

    def test_compact_preserves_order(self):
        """Test that compact preserves record order within files."""
        from avrokit.io.compact import compact_avro_data

        schema = avro_schema(
            {
                "type": "record",
                "name": "Test",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "seq", "type": "int"},
                ],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = os.path.join(tmpdir, "input.avro")
            output_file = os.path.join(tmpdir, "output.avro")
            input_url = parse_url(input_file)
            output_url = parse_url(output_file)

            # Write data
            from avrokit.io import avro_writer, avro_reader

            with avro_writer(input_url.with_mode("wb"), schema) as writer:
                for i in range(10):
                    writer.append({"id": i, "seq": i})

            # Compact (single file)
            compact_avro_data(input_url, output_url)

            # Verify order preserved
            with avro_reader(output_url.with_mode("rb")) as reader:
                records = list(reader)
                assert len(records) == 10
                for i, record in enumerate(records):
                    assert record["seq"] == i


class TestErrorPropagation:
    """Test that errors are properly propagated."""

    def test_write_to_readonly_file(self):
        """Test writing to a read-only file."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Test",
                "fields": [{"name": "id", "type": "int"}],
            }
        )

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                # Make file read-only
                os.chmod(tmp.name, 0o444)

                url = parse_url(tmp.name)
                with pytest.raises((PermissionError, OSError)):
                    from avrokit.io import avro_writer

                    with avro_writer(url.with_mode("wb"), schema) as writer:
                        writer.append({"id": 1})
            finally:
                os.chmod(tmp.name, 0o644)
                os.unlink(tmp.name)

    def test_read_corrupted_file(self):
        """Test reading a corrupted Avro file."""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                # Write garbage data
                tmp.write(b"This is not a valid Avro file")
                tmp.flush()

                url = parse_url(tmp.name)
                with pytest.raises(Exception):  # Could be various exceptions
                    from avrokit.io import avro_reader

                    with avro_reader(url.with_mode("rb")) as reader:
                        list(reader)
            finally:
                os.unlink(tmp.name)
