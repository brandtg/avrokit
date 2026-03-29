# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import os
import tempfile
from datetime import datetime

import pytest
from faker import Faker
from freezegun import freeze_time

from avrokit.io import avro_reader, avro_schema, avro_writer
from avrokit.io.compact import compact_avro_data
from avrokit.io.reader import PartitionedAvroReader, TypedDataFileReader
from avrokit.io.writer import PartitionedAvroWriter, TimePartitionedAvroWriter
from avrokit.url.factory import parse_url

faker = Faker()

SCHEMA = avro_schema(
    {
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"},
            {
                "name": "emails",
                "type": {"type": "array", "items": "string"},
            },
        ],
    }
)


class TestSingleFileIO:
    def test_read_write(self):
        with tempfile.NamedTemporaryFile() as tmp:
            # Write some data to the temporary file
            url = parse_url(tmp.name)
            with avro_writer(url.with_mode("wb"), SCHEMA) as writer:
                for _ in range(100):
                    writer.append(
                        {
                            "name": faker.name(),
                            "age": faker.random_int(min=18, max=80),
                            "emails": [faker.email() for _ in range(3)],
                        }
                    )
            # Read the data back from the temporary file
            with avro_reader(url.with_mode("rb")) as reader:
                count = 0
                for record in reader:
                    assert isinstance(record, dict)
                    assert "name" in record
                    assert "age" in record
                    assert "emails" in record
                    assert isinstance(record["emails"], list)
                    assert len(record["emails"]) == 3
                    count += 1
                assert count == 100

    def test_write_text_mode(self):
        with tempfile.NamedTemporaryFile() as tmp, pytest.raises(ValueError):
            # Attempt to write to a file opened in text mode
            url = parse_url(tmp.name, mode="w")
            with avro_writer(url, SCHEMA):
                pass

    def test_append_to_nonexistent_file_without_schema(self):
        with tempfile.NamedTemporaryFile() as tmp, pytest.raises(ValueError):
            # Attempt to append to a non-existent file without providing a schema
            url = parse_url(tmp.name, mode="ab")
            with avro_writer(url):
                pass

    def test_append_after_write(self):
        with tempfile.NamedTemporaryFile() as tmp:
            url = parse_url(tmp.name, mode="a+b")
            # Try to write to the temporary file in append mode without a schema
            with pytest.raises(ValueError):
                with avro_writer(url):
                    pass
            # Write some data to the temporary file in append mode
            with avro_writer(url, SCHEMA) as writer:
                for _ in range(100):
                    writer.append(
                        {
                            "name": faker.name(),
                            "age": faker.random_int(min=18, max=80),
                            "emails": [faker.email() for _ in range(3)],
                        }
                    )
            # Append to the file without providing a schema
            with avro_writer(url, schema=None) as writer:
                for _ in range(100):
                    writer.append(
                        {
                            "name": faker.name(),
                            "age": faker.random_int(min=18, max=80),
                            "emails": [faker.email() for _ in range(3)],
                        }
                    )
            # Read the data back from the temporary file
            with avro_reader(url) as reader:
                count = 0
                for record in reader:
                    assert isinstance(record, dict)
                    assert "name" in record
                    assert "age" in record
                    assert "emails" in record
                    assert isinstance(record["emails"], list)
                    assert len(record["emails"]) == 3
                    count += 1
                assert count == 200


class TestTypedDataFileReader:
    """Tests for the TypedDataFileReader wrapper introduced to provide typed iteration."""

    SCHEMA = avro_schema(
        {
            "type": "record",
            "name": "Item",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "label", "type": "string"},
            ],
        }
    )

    def _write_records(self, path: str, n: int = 5) -> None:
        url = parse_url(path).with_mode("wb")
        with avro_writer(url, self.SCHEMA) as writer:
            for i in range(n):
                writer.append({"id": i, "label": f"item-{i}"})

    def test_avro_reader_yields_typed_reader(self):
        """avro_reader context manager must yield a TypedDataFileReader instance."""
        with tempfile.NamedTemporaryFile(suffix=".avro") as tmp:
            self._write_records(tmp.name)
            url = parse_url(tmp.name).with_mode("rb")
            with avro_reader(url) as reader:
                assert isinstance(reader, TypedDataFileReader)

    def test_iteration_yields_dicts(self):
        """Iterating a TypedDataFileReader must yield dict records."""
        with tempfile.NamedTemporaryFile(suffix=".avro") as tmp:
            self._write_records(tmp.name, n=3)
            url = parse_url(tmp.name).with_mode("rb")
            with avro_reader(url) as reader:
                records = list(reader)
            assert len(records) == 3
            for record in records:
                assert isinstance(record, dict)
                assert "id" in record
                assert "label" in record

    def test_tell_returns_positive_int_after_reads(self):
        """tell() must return a positive integer that advances as records are read."""
        with tempfile.NamedTemporaryFile(suffix=".avro") as tmp:
            self._write_records(tmp.name, n=5)
            url = parse_url(tmp.name).with_mode("rb")
            with avro_reader(url) as reader:
                positions = []
                for _ in reader:
                    positions.append(reader.tell())
            assert len(positions) == 5
            assert all(isinstance(p, int) for p in positions)
            # Position must be positive (we've read past the Avro header)
            assert positions[0] > 0
            # Position must be non-decreasing
            assert positions == sorted(positions)

    def test_getattr_delegates_schema(self):
        """__getattr__ must delegate attribute access to the underlying DataFileReader."""
        with tempfile.NamedTemporaryFile(suffix=".avro") as tmp:
            self._write_records(tmp.name)
            url = parse_url(tmp.name).with_mode("rb")
            with avro_reader(url) as reader:
                # .schema is a property on DataFileReader; accessed via __getattr__
                schema_str = reader.schema
                assert "Item" in schema_str

    def test_getattr_raises_on_unknown_attribute(self):
        """Accessing a nonexistent attribute must raise AttributeError."""
        with tempfile.NamedTemporaryFile(suffix=".avro") as tmp:
            self._write_records(tmp.name)
            url = parse_url(tmp.name).with_mode("rb")
            with avro_reader(url) as reader:
                with pytest.raises(AttributeError):
                    _ = reader.this_attribute_does_not_exist


class TestPartitionedAvroReader:
    def test_read(self):
        with tempfile.TemporaryDirectory() as tmp:
            # Write a set of partitioned Avro files
            for i in range(10):
                url = parse_url(f"{tmp}/file_{i}.avro")
                with avro_writer(url.with_mode("wb"), SCHEMA) as writer:
                    for _ in range(10):
                        writer.append(
                            {
                                "name": faker.name(),
                                "age": faker.random_int(min=18, max=80),
                                "emails": [faker.email() for _ in range(3)],
                            }
                        )
            # Read the data back from the partitioned Avro files
            with PartitionedAvroReader([parse_url(tmp)]) as reader:
                count = 0
                for record in reader:
                    assert isinstance(record, dict)
                    assert "name" in record
                    assert "age" in record
                    assert "emails" in record
                    assert isinstance(record["emails"], list)
                    assert len(record["emails"]) == 3
                    count += 1
                assert count == 100


class TestPartitionedAvroWriter:
    def test_next_filename_none(self):
        assert PartitionedAvroWriter.next_filename(None) == "part-00000.avro"

    def test_next_filename_previous(self):
        assert (
            PartitionedAvroWriter.next_filename("part-00000.avro") == "part-00001.avro"
        )

    def test_find_last_filename_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            url = parse_url(tmp, mode="wb")
            writer = PartitionedAvroWriter(url, SCHEMA)
            assert writer.find_last_filename() is None

    def test_find_last_filename_previous(self):
        with tempfile.TemporaryDirectory() as tmp:
            # Write a file partition
            url = parse_url(f"{tmp}/part-00000.avro")
            with avro_writer(url.with_mode("wb"), SCHEMA) as writer:
                for _ in range(10):
                    writer.append(
                        {
                            "name": faker.name(),
                            "age": faker.random_int(min=18, max=80),
                            "emails": [faker.email() for _ in range(3)],
                        }
                    )
            # Writer at root should return the last filename
            url = parse_url(tmp, mode="wb")
            writer = PartitionedAvroWriter(url, SCHEMA)
            assert writer.find_last_filename() == "part-00000.avro"

    def test_append(self):
        with tempfile.TemporaryDirectory() as tmp:
            # Write several partitions via rolling the writer
            url = parse_url(tmp, mode="wb")
            with PartitionedAvroWriter(url, SCHEMA) as writer:
                for i in range(10):
                    if i > 0:
                        writer.roll()
                    for _ in range(10):
                        writer.append(
                            {
                                "name": faker.name(),
                                "age": faker.random_int(min=18, max=80),
                                "emails": [faker.email() for _ in range(3)],
                            }
                        )
            # Read the data back from the partitioned Avro files
            expanded_urls = url.expand()
            assert len(expanded_urls) == 10
            assert [os.path.basename(u.url) for u in expanded_urls] == [
                f"part-{i:05d}.avro" for i in range(10)
            ]
            with PartitionedAvroReader(url.with_mode("rb")) as reader:
                count = 0
                for record in reader:
                    assert isinstance(record, dict)
                    assert "name" in record
                    assert "age" in record
                    assert "emails" in record
                    assert isinstance(record["emails"], list)
                    assert len(record["emails"]) == 3
                    count += 1
                assert count == 100


class TestTimePartitionedAvroWriter:
    @freeze_time(datetime(2025, 1, 1, 12, 0, 0))
    def test_next_filename_none(self):
        assert TimePartitionedAvroWriter.next_filename(None) == "2025-01-01_12-00-00"

    @freeze_time(datetime(2025, 1, 1, 12, 0, 0))
    def test_next_filename_previous(self):
        # N.b. doesn't matter what the previous timestamp is, it will pick the current
        assert (
            TimePartitionedAvroWriter.next_filename("2025-01-01_11-00-00")
            == "2025-01-01_12-00-00"
        )

    @freeze_time(datetime(2025, 1, 1, 12, 0, 0))
    def test_next_filename_via_instance(self):
        """next_filename must work when called on an instance (staticmethod behaviour)."""
        with tempfile.TemporaryDirectory() as tmp:
            url = parse_url(f"{tmp}/*.avro").with_mode("wb")
            writer = TimePartitionedAvroWriter(url, SCHEMA)
            # Calling via an instance should behave identically to calling via the class
            assert writer.next_filename(None) == "2025-01-01_12-00-00"
            assert writer.next_filename("2025-01-01_11-00-00") == "2025-01-01_12-00-00"

    def test_group_time_partitions(self):
        urls = [
            parse_url(url)
            for url in [
                "file:///tmp/2025-01-01_12-00-00",
                "file:///tmp/2025-01-01_14-00-00",
                "file:///tmp/2025-01-02_12-00-00",
                "file:///tmp/2025-01-03_12-00-00",
                "file:///tmp/2025-01-03_22-00-00",
            ]
        ]
        grouped = TimePartitionedAvroWriter.group_time_partitions(urls, "day")
        assert len(grouped) == 3
        assert grouped[datetime(2025, 1, 1)] == [
            parse_url("file:///tmp/2025-01-01_12-00-00"),
            parse_url("file:///tmp/2025-01-01_14-00-00"),
        ]
        assert grouped[datetime(2025, 1, 2)] == [
            parse_url("file:///tmp/2025-01-02_12-00-00"),
        ]
        assert grouped[datetime(2025, 1, 3)] == [
            parse_url("file:///tmp/2025-01-03_12-00-00"),
            parse_url("file:///tmp/2025-01-03_22-00-00"),
        ]


class TestCompactAvroData:
    def test_compact_avro_data(self):
        with tempfile.TemporaryDirectory() as src, tempfile.TemporaryDirectory() as dst:
            # Write a set of partitioned Avro files
            for i in range(10):
                url = parse_url(f"{src}/file_{i}.avro")
                with avro_writer(url.with_mode("wb"), SCHEMA) as writer:
                    for _ in range(10):
                        writer.append(
                            {
                                "name": faker.name(),
                                "age": faker.random_int(min=18, max=80),
                                "emails": [faker.email() for _ in range(3)],
                            }
                        )
            # Compact the Avro data into a single file
            src_url = parse_url(src)
            compacted_url = parse_url(f"{dst}/compacted.avro")
            compact_avro_data(src_url, compacted_url)
            # Read the data back from the compacted Avro file
            with avro_reader(compacted_url.with_mode("rb")) as reader:
                count = 0
                for record in reader:
                    assert isinstance(record, dict)
                    assert "name" in record
                    assert "age" in record
                    assert "emails" in record
                    assert isinstance(record["emails"], list)
                    assert len(record["emails"]) == 3
                    count += 1
                assert count == 100
