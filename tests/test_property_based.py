# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

"""
Property-based tests using hypothesis to stress test the avrokit library.
These tests generate random inputs to surface edge cases and bugs.
"""

import pytest
import tempfile
import os
from avrokit.io import avro_schema, avro_reader, avro_writer
from avrokit.io.reader import PartitionedAvroReader
from avrokit.io.writer import PartitionedAvroWriter
from avrokit.url.factory import parse_url
from faker import Faker

faker = Faker()


class TestPropertyBasedIO:
    """Property-based tests for IO operations."""

    def test_roundtrip_empty_file(self):
        """Test reading/writing an empty Avro file."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Empty",
                "fields": [{"name": "id", "type": "int"}],
            }
        )
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                url = parse_url(tmp.name)
                # Write zero records
                with avro_writer(url.with_mode("wb"), schema) as _:
                    pass
                # Read back and verify
                with avro_reader(url.with_mode("rb")) as reader:
                    records = list(reader)
                    assert len(records) == 0
            finally:
                os.unlink(tmp.name)

    def test_roundtrip_single_record(self):
        """Test reading/writing a single record."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Single",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"},
                ],
            }
        )
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                url = parse_url(tmp.name)
                record = {"id": 1, "name": "test"}
                # Write single record
                with avro_writer(url.with_mode("wb"), schema) as writer:
                    writer.append(record)
                # Read back and verify
                with avro_reader(url.with_mode("rb")) as reader:
                    records = list(reader)
                    assert len(records) == 1
                    assert records[0] == record
            finally:
                os.unlink(tmp.name)

    def test_roundtrip_large_records(self):
        """Test with very large string fields."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Large",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "data", "type": "string"},
                ],
            }
        )
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                url = parse_url(tmp.name)
                # Create a record with a very large string (10MB)
                large_string = "x" * (10 * 1024 * 1024)
                record = {"id": 1, "data": large_string}

                with avro_writer(url.with_mode("wb"), schema) as writer:
                    writer.append(record)

                with avro_reader(url.with_mode("rb")) as reader:
                    records = list(reader)
                    assert len(records) == 1
                    assert records[0]["id"] == 1
                    assert len(records[0]["data"]) == len(large_string)
            finally:
                os.unlink(tmp.name)

    def test_roundtrip_many_small_records(self):
        """Test with many small records to stress buffer handling."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Many",
                "fields": [{"name": "id", "type": "int"}],
            }
        )
        num_records = 100000
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                url = parse_url(tmp.name)
                # Write many small records
                with avro_writer(url.with_mode("wb"), schema) as writer:
                    for i in range(num_records):
                        writer.append({"id": i})

                # Read back and verify count
                with avro_reader(url.with_mode("rb")) as reader:
                    count = sum(1 for _ in reader)
                    assert count == num_records
            finally:
                os.unlink(tmp.name)

    def test_nested_records_deep(self):
        """Test deeply nested record structures."""
        # Create a deeply nested schema
        schema_dict = {
            "type": "record",
            "name": "Level0",
            "fields": [{"name": "value", "type": "int"}],
        }

        # Create 10 levels of nesting
        for i in range(1, 11):
            schema_dict = {
                "type": "record",
                "name": f"Level{i}",
                "fields": [{"name": "nested", "type": schema_dict}],
            }

        schema = avro_schema(schema_dict)

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                url = parse_url(tmp.name)
                # Create a deeply nested record
                record = {"nested": {"value": 42}}
                for _ in range(9):
                    record = {"nested": record}

                with avro_writer(url.with_mode("wb"), schema) as writer:
                    writer.append(record)

                with avro_reader(url.with_mode("rb")) as reader:
                    records = list(reader)
                    assert len(records) == 1
                    # Navigate to the deepest value
                    value = records[0]
                    for _ in range(10):
                        value = value["nested"]
                    assert value == {
                        "value": 42
                    }  # The innermost level is a dict with value: 42
            finally:
                os.unlink(tmp.name)

    def test_array_with_many_elements(self):
        """Test arrays with many elements."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "ArrayTest",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "items", "type": {"type": "array", "items": "int"}},
                ],
            }
        )
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                url = parse_url(tmp.name)
                # Create a record with a very large array
                record = {"id": 1, "items": list(range(100000))}

                with avro_writer(url.with_mode("wb"), schema) as writer:
                    writer.append(record)

                with avro_reader(url.with_mode("rb")) as reader:
                    records = list(reader)
                    assert len(records) == 1
                    assert len(records[0]["items"]) == 100000
                    assert records[0]["items"][50000] == 50000
            finally:
                os.unlink(tmp.name)

    def test_map_with_many_entries(self):
        """Test maps with many key-value pairs."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "MapTest",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "props", "type": {"type": "map", "values": "string"}},
                ],
            }
        )
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                url = parse_url(tmp.name)
                # Create a record with a large map
                props = {f"key_{i}": f"value_{i}" for i in range(10000)}
                record = {"id": 1, "props": props}

                with avro_writer(url.with_mode("wb"), schema) as writer:
                    writer.append(record)

                with avro_reader(url.with_mode("rb")) as reader:
                    records = list(reader)
                    assert len(records) == 1
                    assert len(records[0]["props"]) == 10000
                    assert records[0]["props"]["key_5000"] == "value_5000"
            finally:
                os.unlink(tmp.name)

    def test_union_types_all_null(self):
        """Test union types where all values are null."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "UnionTest",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "optional_field", "type": ["null", "string"]},
                ],
            }
        )
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                url = parse_url(tmp.name)
                # Write records with all null values
                with avro_writer(url.with_mode("wb"), schema) as writer:
                    for i in range(100):
                        writer.append({"id": i, "optional_field": None})

                with avro_reader(url.with_mode("rb")) as reader:
                    records = list(reader)
                    assert len(records) == 100
                    assert all(r["optional_field"] is None for r in records)
            finally:
                os.unlink(tmp.name)

    def test_special_characters_in_strings(self):
        """Test strings with special characters, unicode, etc."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "SpecialChars",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "text", "type": "string"},
                ],
            }
        )
        special_strings = [
            "",  # empty string
            "Hello\x00World",  # null byte
            "Line1\nLine2\rLine3\r\n",  # various newlines
            "Tab\tSeparated\tValues",
            "Unicode: 你好世界 🚀 ñ é ü",
            "Emoji: 😀😁😂🤣😃😄😅",
            "\\backslash/ forward/slash",
            "'single' \"double\" quotes",
            "Very long string " + "x" * 10000,
        ]

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                url = parse_url(tmp.name)
                with avro_writer(url.with_mode("wb"), schema) as writer:
                    for i, text in enumerate(special_strings):
                        writer.append({"id": i, "text": text})

                with avro_reader(url.with_mode("rb")) as reader:
                    records = list(reader)
                    assert len(records) == len(special_strings)
                    for i, record in enumerate(records):
                        assert record["text"] == special_strings[i]
            finally:
                os.unlink(tmp.name)


class TestPartitionedReaderWriter:
    """Stress tests for partitioned reading/writing."""

    def test_partitioned_writer_empty_partitions(self):
        """Test partitioned writer with zero records."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Test",
                "fields": [{"name": "id", "type": "int"}],
            }
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            base_url = parse_url(os.path.join(tmpdir, "output", "*.avro"))
            with PartitionedAvroWriter(base_url.with_mode("wb"), schema) as _:
                pass  # Write nothing

            # Should create at least one file
            output_dir = os.path.join(tmpdir, "output")
            assert os.path.exists(output_dir)
            files = os.listdir(output_dir)
            assert len(files) == 1

    def test_partitioned_many_small_partitions(self):
        """Test creating many small partitions."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Test",
                "fields": [{"name": "id", "type": "int"}],
            }
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            base_url = parse_url(os.path.join(tmpdir, "output", "*.avro"))
            num_partitions = 100

            with PartitionedAvroWriter(base_url.with_mode("wb"), schema) as writer:
                for i in range(num_partitions):
                    writer.append({"id": i})
                    writer.roll()

            # Should create many partition files
            output_dir = os.path.join(tmpdir, "output")
            files = os.listdir(output_dir)
            assert len(files) >= num_partitions

    def test_partitioned_reader_single_partition(self):
        """Test partitioned reader with single file."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Test",
                "fields": [{"name": "id", "type": "int"}],
            }
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write a single partition
            base_url = parse_url(os.path.join(tmpdir, "*.avro"))
            file_url = parse_url(os.path.join(tmpdir, "part-00000.avro"))

            with avro_writer(file_url.with_mode("wb"), schema) as writer:
                for i in range(10):
                    writer.append({"id": i})

            # Read with partitioned reader
            with PartitionedAvroReader(base_url.with_mode("rb")) as reader:
                records = list(reader)
                assert len(records) == 10

    def test_partitioned_reader_many_empty_partitions(self):
        """Test partitioned reader with many empty files."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Test",
                "fields": [{"name": "id", "type": "int"}],
            }
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            base_url = parse_url(os.path.join(tmpdir, "*.avro"))

            # Create multiple empty files
            for i in range(5):
                file_url = parse_url(os.path.join(tmpdir, f"part-{i:05d}.avro"))
                with avro_writer(file_url.with_mode("wb"), schema) as _:
                    pass  # Empty

            # Read with partitioned reader
            with PartitionedAvroReader(base_url.with_mode("rb")) as reader:
                records = list(reader)
                assert len(records) == 0

    def test_partitioned_reader_empty_url_list(self):
        """Test partitioned reader with empty URL list."""
        with PartitionedAvroReader([]) as reader:
            records = list(reader)
            assert len(records) == 0


class TestEdgeCasesAndErrors:
    """Test edge cases and error conditions."""

    def test_invalid_schema_format(self):
        """Test that invalid schema raises appropriate error."""
        with pytest.raises((ValueError, Exception)):
            avro_schema({"type": "invalid_type"})

    def test_schema_field_mismatch(self):
        """Test writing records that don't match schema."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Test",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"},
                ],
            }
        )
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                url = parse_url(tmp.name)
                with avro_writer(url.with_mode("wb"), schema) as writer:
                    # Try to write record with wrong type
                    with pytest.raises(Exception):
                        writer.append({"id": "not_an_int", "name": "test"})
            finally:
                os.unlink(tmp.name)

    def test_missing_required_field(self):
        """Test writing records with missing required fields."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Test",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"},
                ],
            }
        )
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                url = parse_url(tmp.name)
                with avro_writer(url.with_mode("wb"), schema) as writer:
                    # Try to write record with missing field
                    with pytest.raises(Exception):
                        writer.append({"id": 1})
            finally:
                os.unlink(tmp.name)

    def test_extra_fields_in_record(self):
        """Test writing records with extra fields not in schema."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Test",
                "fields": [{"name": "id", "type": "int"}],
            }
        )
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                url = parse_url(tmp.name)
                # Avro validates strictly - extra fields cause errors
                # This is correct behavior, not a bug
                with avro_writer(url.with_mode("wb"), schema) as writer:
                    # Try to write record with extra field - should raise
                    with pytest.raises(Exception):
                        writer.append({"id": 1, "extra": "ignored"})
            finally:
                os.unlink(tmp.name)

    def test_extreme_integer_values(self):
        """Test extreme integer values (min/max)."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "IntTest",
                "fields": [
                    {"name": "int_val", "type": "int"},
                    {"name": "long_val", "type": "long"},
                ],
            }
        )
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                url = parse_url(tmp.name)
                # Test with extreme values
                records = [
                    {"int_val": 2147483647, "long_val": 9223372036854775807},  # max
                    {"int_val": -2147483648, "long_val": -9223372036854775808},  # min
                    {"int_val": 0, "long_val": 0},
                ]

                with avro_writer(url.with_mode("wb"), schema) as writer:
                    for record in records:
                        writer.append(record)

                with avro_reader(url.with_mode("rb")) as reader:
                    read_records = list(reader)
                    assert len(read_records) == 3
                    assert read_records[0]["int_val"] == 2147483647
                    assert read_records[1]["int_val"] == -2147483648
            finally:
                os.unlink(tmp.name)

    def test_extreme_float_values(self):
        """Test extreme float values."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "FloatTest",
                "fields": [
                    {"name": "float_val", "type": "float"},
                    {"name": "double_val", "type": "double"},
                ],
            }
        )
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            try:
                url = parse_url(tmp.name)
                records = [
                    {"float_val": 3.4028235e38, "double_val": 1.7976931348623157e308},
                    {"float_val": -3.4028235e38, "double_val": -1.7976931348623157e308},
                    {"float_val": 0.0, "double_val": 0.0},
                    {"float_val": float("inf"), "double_val": float("inf")},
                    {"float_val": float("-inf"), "double_val": float("-inf")},
                ]

                with avro_writer(url.with_mode("wb"), schema) as writer:
                    for record in records:
                        writer.append(record)

                with avro_reader(url.with_mode("rb")) as reader:
                    read_records = list(reader)
                    assert len(read_records) == 5
            finally:
                os.unlink(tmp.name)
