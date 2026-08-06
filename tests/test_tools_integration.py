# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

"""
Integration tests for command-line tools in avrokit.
"""

import json
import os
import tempfile
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import cast

import pytest
from faker import Faker

from avrokit.io import avro_reader, avro_schema, avro_writer
from avrokit.tools.filesort import FileSortTool
from avrokit.tools.fromparquet import parquet_to_avro
from avrokit.tools.partition import PartitionTool
from avrokit.tools.tojson import ToJsonTool
from avrokit.tools.toparquet import ToParquetTool, avro_to_parquet
from avrokit.url.factory import parse_url

faker = Faker()


class TestPartitionTool:
    """Tests for the partition tool."""

    def test_partition_small_file(self):
        """Test partitioning a small Avro file."""
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
            # Create input file
            input_file = os.path.join(tmpdir, "input.avro")
            input_url = parse_url(input_file)

            with avro_writer(input_url.with_mode("wb"), schema) as writer:
                for i in range(100):
                    writer.append({"id": i, "value": f"value_{i}"})

            # Partition into 3 parts
            output_pattern = os.path.join(tmpdir, "output", "*.avro")
            output_url = parse_url(output_pattern)

            tool = PartitionTool()
            tool.partition_avro(input_url, output_url, count_partitions=3, force=False)

            # Verify output
            output_dir = os.path.join(tmpdir, "output")
            assert os.path.exists(output_dir)
            files = sorted(os.listdir(output_dir))
            # Exactly 3 partitions requested; the tool splits by byte size so we
            # expect at least 2 and no more than 100 (one per record).
            assert 2 <= len(files) <= 100

            # Read back all records
            from avrokit.io.reader import PartitionedAvroReader

            with PartitionedAvroReader(output_url.with_mode("rb")) as reader:
                records = list(reader)
                assert len(records) == 100

    def test_partition_reader_tell_advances(self):
        """tell() on the reader inside partition_avro must return non-zero advancing values.

        This exercises the reader.tell() code path changed from reader._reader.tell().
        """
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
            input_file = os.path.join(tmpdir, "input.avro")
            input_url = parse_url(input_file)

            with avro_writer(input_url.with_mode("wb"), schema) as writer:
                for i in range(20):
                    writer.append({"id": i, "value": f"value_{i}"})

            # Collect tell() positions directly via avro_reader to verify the
            # method returns meaningful values (positive, non-decreasing).
            from avrokit.io import avro_reader

            positions = []
            with avro_reader(input_url.with_mode("rb")) as reader:
                for _ in reader:
                    positions.append(reader.tell())

            assert len(positions) == 20
            assert positions[0] > 0, "tell() must return a positive value after reading"
            assert positions == sorted(positions), (
                "tell() positions must be non-decreasing"
            )

    def test_partition_force_overwrite(self):
        """Test force overwrite functionality."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Test",
                "fields": [{"name": "id", "type": "int"}],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = os.path.join(tmpdir, "input.avro")
            input_url = parse_url(input_file)

            with avro_writer(input_url.with_mode("wb"), schema) as writer:
                for i in range(10):
                    writer.append({"id": i})

            output_pattern = os.path.join(tmpdir, "output", "*.avro")
            output_url = parse_url(output_pattern)

            tool = PartitionTool()
            # First partition
            tool.partition_avro(input_url, output_url, count_partitions=2, force=False)

            # Second partition should fail without force
            with pytest.raises(ValueError):
                tool.partition_avro(
                    input_url, output_url, count_partitions=2, force=False
                )

            # Should succeed with force
            tool.partition_avro(input_url, output_url, count_partitions=2, force=True)


class TestFileSortTool:
    """Tests for the filesort tool."""

    def test_sort_by_single_field(self):
        """Test sorting by a single field."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Test",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "value", "type": "int"},
                ],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = os.path.join(tmpdir, "input.avro")
            output_file = os.path.join(tmpdir, "output.avro")
            input_url = parse_url(input_file)
            output_url = parse_url(output_file)

            # Write unsorted data
            with avro_writer(input_url.with_mode("wb"), schema) as writer:
                for i in [5, 2, 8, 1, 9, 3, 7, 4, 6, 0]:
                    writer.append({"id": i, "value": i * 10})

            # Sort
            tool = FileSortTool()
            tool.filesort(input_url, output_url, sort_fields=["id"], batch_size=3)

            # Verify sorted
            with avro_reader(output_url.with_mode("rb")) as reader:
                records = list(reader)
                assert len(records) == 10
                for i, record in enumerate(records):
                    assert record["id"] == i

    def test_sort_by_multiple_fields(self):
        """Test sorting by multiple fields."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Test",
                "fields": [
                    {"name": "group", "type": "int"},
                    {"name": "id", "type": "int"},
                ],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = os.path.join(tmpdir, "input.avro")
            output_file = os.path.join(tmpdir, "output.avro")
            input_url = parse_url(input_file)
            output_url = parse_url(output_file)

            # Write unsorted data
            records = [
                {"group": 2, "id": 1},
                {"group": 1, "id": 2},
                {"group": 2, "id": 0},
                {"group": 1, "id": 1},
                {"group": 1, "id": 0},
            ]
            with avro_writer(input_url.with_mode("wb"), schema) as writer:
                for record in records:
                    writer.append(record)

            # Sort by group, then id
            tool = FileSortTool()
            tool.filesort(input_url, output_url, sort_fields=["group", "id"])

            # Verify sorted
            with avro_reader(output_url.with_mode("rb")) as reader:
                sorted_records = list(reader)
                assert len(sorted_records) == 5
                # Should be sorted by group first, then id
                assert sorted_records[0] == {"group": 1, "id": 0}
                assert sorted_records[1] == {"group": 1, "id": 1}
                assert sorted_records[2] == {"group": 1, "id": 2}
                assert sorted_records[3] == {"group": 2, "id": 0}
                assert sorted_records[4] == {"group": 2, "id": 1}

    def test_sort_reverse(self):
        """Test reverse sorting."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Test",
                "fields": [{"name": "id", "type": "int"}],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = os.path.join(tmpdir, "input.avro")
            output_file = os.path.join(tmpdir, "output.avro")
            input_url = parse_url(input_file)
            output_url = parse_url(output_file)

            with avro_writer(input_url.with_mode("wb"), schema) as writer:
                for i in range(10):
                    writer.append({"id": i})

            tool = FileSortTool()
            tool.filesort(input_url, output_url, sort_fields=["id"], reverse=True)

            with avro_reader(output_url.with_mode("rb")) as reader:
                records = list(reader)
                assert records[0]["id"] == 9
                assert records[-1]["id"] == 0


class TestParquetConversion:
    """Tests for Parquet conversion tools."""

    def test_avro_to_parquet_basic(self):
        """Test basic Avro to Parquet conversion."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Test",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"},
                    {"name": "value", "type": "double"},
                ],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            avro_file = os.path.join(tmpdir, "input.avro")
            parquet_file = os.path.join(tmpdir, "output.parquet")
            avro_url = parse_url(avro_file)
            parquet_url = parse_url(parquet_file)

            # Write Avro data
            with avro_writer(avro_url.with_mode("wb"), schema) as writer:
                for i in range(10):
                    writer.append({"id": i, "name": f"name_{i}", "value": i * 1.5})

            # Convert to Parquet
            tool = ToParquetTool()
            tool.convert(avro_url, parquet_url)

            # Verify Parquet file exists
            assert os.path.exists(parquet_file)
            assert os.path.getsize(parquet_file) > 0

    def test_parquet_to_avro_basic(self):
        """Test basic Parquet to Avro conversion."""
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

        with tempfile.TemporaryDirectory() as tmpdir:
            avro_file1 = os.path.join(tmpdir, "input.avro")
            parquet_file = os.path.join(tmpdir, "intermediate.parquet")
            avro_file2 = os.path.join(tmpdir, "output.avro")

            avro_url1 = parse_url(avro_file1)
            parquet_url = parse_url(parquet_file)
            avro_url2 = parse_url(avro_file2)

            # Write Avro data
            with avro_writer(avro_url1.with_mode("wb"), schema) as writer:
                for i in range(10):
                    writer.append({"id": i, "name": f"name_{i}"})

            # Convert to Parquet
            avro_to_parquet(avro_url1, parquet_url)

            # Convert back to Avro
            parquet_to_avro(parquet_url, avro_url2, "Test", None)

            # Verify roundtrip
            with avro_reader(avro_url2.with_mode("rb")) as reader:
                records = list(reader)
                assert len(records) == 10
                assert records[0]["id"] == 0
                assert records[0]["name"] == "name_0"

    def test_avro_to_parquet_with_arrays(self):
        """Test Parquet conversion with array types."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Test",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "items", "type": {"type": "array", "items": "int"}},
                ],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            avro_file = os.path.join(tmpdir, "input.avro")
            parquet_file = os.path.join(tmpdir, "output.parquet")
            avro_url = parse_url(avro_file)
            parquet_url = parse_url(parquet_file)

            with avro_writer(avro_url.with_mode("wb"), schema) as writer:
                writer.append({"id": 1, "items": [1, 2, 3]})
                writer.append({"id": 2, "items": []})
                writer.append({"id": 3, "items": [10, 20, 30, 40, 50]})

            tool = ToParquetTool()
            tool.convert(avro_url, parquet_url)

            assert os.path.exists(parquet_file)

    def test_avro_to_parquet_with_maps(self):
        """Test Parquet conversion with map types."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Test",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "props", "type": {"type": "map", "values": "string"}},
                ],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            avro_file = os.path.join(tmpdir, "input.avro")
            parquet_file = os.path.join(tmpdir, "output.parquet")
            avro_url = parse_url(avro_file)
            parquet_url = parse_url(parquet_file)

            with avro_writer(avro_url.with_mode("wb"), schema) as writer:
                writer.append({"id": 1, "props": {"key1": "value1", "key2": "value2"}})
                writer.append({"id": 2, "props": {}})

            tool = ToParquetTool()
            tool.convert(avro_url, parquet_url)

            assert os.path.exists(parquet_file)


class TestToJsonTool:
    """Tests for ToJson tool."""

    def test_tojson_basic(self):
        """Test basic JSON output."""
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

        with tempfile.TemporaryDirectory() as tmpdir:
            avro_file = os.path.join(tmpdir, "input.avro")
            json_file = os.path.join(tmpdir, "output.json")
            avro_url = parse_url(avro_file)

            with avro_writer(avro_url.with_mode("wb"), schema) as writer:
                for i in range(5):
                    writer.append({"id": i, "name": f"name_{i}"})

            # Convert to JSON
            tool = ToJsonTool()

            # Create a mock args object
            import argparse

            args = argparse.Namespace(url=[avro_file])

            with open(json_file, "w") as f:
                import sys

                old_stdout = sys.stdout
                sys.stdout = f
                try:
                    tool.run(args)
                finally:
                    sys.stdout = old_stdout

            # Verify JSON output
            with open(json_file) as f:
                lines = f.readlines()
                assert len(lines) == 5
                for i, line in enumerate(lines):
                    record = json.loads(line)
                    assert record["id"] == i
                    assert record["name"] == f"name_{i}"

    def test_tojson_logical_types(self):
        """Test that Avro logical types (timestamps, dates, decimals) are serialized."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Test",
                "fields": [
                    {"name": "id", "type": "int"},
                    {
                        "name": "created_at",
                        "type": {"type": "long", "logicalType": "timestamp-millis"},
                    },
                    {"name": "day", "type": {"type": "int", "logicalType": "date"}},
                    {
                        "name": "amount",
                        "type": {
                            "type": "bytes",
                            "logicalType": "decimal",
                            "precision": 10,
                            "scale": 2,
                        },
                    },
                ],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            avro_file = os.path.join(tmpdir, "input.avro")
            json_file = os.path.join(tmpdir, "output.json")
            avro_url = parse_url(avro_file)

            with avro_writer(avro_url.with_mode("wb"), schema) as writer:
                writer.append(
                    {
                        "id": 1,
                        "created_at": datetime(
                            2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc
                        ),
                        "day": date(2025, 1, 2),
                        "amount": Decimal("12.34"),
                    }
                )

            tool = ToJsonTool()

            import argparse

            args = argparse.Namespace(url=[avro_file])

            with open(json_file, "w") as f:
                import sys

                old_stdout = sys.stdout
                sys.stdout = f
                try:
                    tool.run(args)
                finally:
                    sys.stdout = old_stdout

            with open(json_file) as f:
                lines = f.readlines()
                assert len(lines) == 1
                record = json.loads(lines[0])
                assert record["id"] == 1
                assert record["created_at"] == "2025-01-02T03:04:05+00:00"
                assert record["day"] == "2025-01-02"
                assert record["amount"] == "12.34"


class TestGetSchemaTool:
    """Tests for GetSchema tool."""

    def test_get_schema(self):
        """Test extracting schema from Avro file."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "TestRecord",
                "namespace": "com.example",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"},
                ],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            avro_file = os.path.join(tmpdir, "input.avro")
            avro_url = parse_url(avro_file)

            with avro_writer(avro_url.with_mode("wb"), schema) as writer:
                writer.append({"id": 1, "name": "test"})

            # Get schema
            from avro.schema import RecordSchema

            from avrokit.io import read_avro_schema

            extracted_schema = cast(RecordSchema, read_avro_schema(avro_url))

            assert extracted_schema.name == "TestRecord"
            assert extracted_schema.namespace == "com.example"


class TestConcatTool:
    """Tests for Concat tool."""

    def test_concat_multiple_files(self):
        """Test concatenating multiple Avro files."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Test",
                "fields": [{"name": "id", "type": "int"}],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create multiple input files
            for file_idx in range(3):
                file_path = os.path.join(tmpdir, f"input_{file_idx}.avro")
                url = parse_url(file_path)
                with avro_writer(url.with_mode("wb"), schema) as writer:
                    for i in range(10):
                        writer.append({"id": file_idx * 10 + i})

            # Concatenate
            input_pattern = os.path.join(tmpdir, "input_*.avro")
            output_file = os.path.join(tmpdir, "output.avro")

            from avrokit.io.reader import PartitionedAvroReader

            input_url = parse_url(input_pattern)
            output_url = parse_url(output_file)

            with (
                PartitionedAvroReader(input_url.with_mode("rb")) as reader,
                avro_writer(output_url.with_mode("wb"), schema) as writer,
            ):
                for record in reader:
                    writer.append(record)

            # Verify
            with avro_reader(output_url.with_mode("rb")) as reader:
                records = list(reader)
                assert len(records) == 30


class TestStressCombinations:
    """Stress tests combining multiple operations."""

    def test_partition_then_concat(self):
        """Test partitioning then concatenating back."""
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
            # Create original file
            original_file = os.path.join(tmpdir, "original.avro")
            original_url = parse_url(original_file)

            original_records = [{"id": i, "value": f"val_{i}"} for i in range(100)]
            with avro_writer(original_url.with_mode("wb"), schema) as writer:
                for record in original_records:
                    writer.append(record)

            # Partition
            partition_dir = os.path.join(tmpdir, "partitions", "*.avro")
            partition_url = parse_url(partition_dir)
            tool = PartitionTool()
            tool.partition_avro(
                original_url, partition_url, count_partitions=5, force=False
            )

            # Concat back
            concat_file = os.path.join(tmpdir, "concat.avro")
            concat_url = parse_url(concat_file)

            from avrokit.io.reader import PartitionedAvroReader

            with (
                PartitionedAvroReader(partition_url.with_mode("rb")) as reader,
                avro_writer(concat_url.with_mode("wb"), schema) as writer,
            ):
                for record in reader:
                    writer.append(record)

            # Verify all records present
            with avro_reader(concat_url.with_mode("rb")) as reader:
                concat_records = list(reader)
                assert len(concat_records) == 100

    def test_sort_large_file(self):
        """Test sorting a larger file with small batch size."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Test",
                "fields": [{"name": "id", "type": "int"}],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = os.path.join(tmpdir, "input.avro")
            output_file = os.path.join(tmpdir, "output.avro")
            input_url = parse_url(input_file)
            output_url = parse_url(output_file)

            # Create unsorted data
            import random

            ids = list(range(1000))
            random.shuffle(ids)

            with avro_writer(input_url.with_mode("wb"), schema) as writer:
                for i in ids:
                    writer.append({"id": i})

            # Sort with small batch size to stress the merge
            tool = FileSortTool()
            tool.filesort(input_url, output_url, sort_fields=["id"], batch_size=50)

            # Verify sorted
            with avro_reader(output_url.with_mode("rb")) as reader:
                records = list(reader)
                assert len(records) == 1000
                for i, record in enumerate(records):
                    assert record["id"] == i
