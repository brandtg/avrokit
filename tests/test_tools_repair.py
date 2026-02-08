# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import pytest
import tempfile
import os
from avrokit.io import avro_writer, avro_schema
from avrokit.io.reader import avro_reader
from avrokit.url import parse_url
from avrokit.tools import RepairTool
from faker import Faker

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


@pytest.fixture
def tmpdir():
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a set of avro files with valid data
        for i in range(3):
            url = parse_url(f"{tmpdir}/file_{i}.avro")
            with avro_writer(url.with_mode("wb"), SCHEMA) as writer:
                for _ in range(5000):
                    writer.append(
                        {
                            "name": faker.name(),
                            "age": faker.random_int(min=18, max=80),
                            "emails": [faker.email() for _ in range(3)],
                        }
                    )
        # Corrupt one of the files (record-level)
        with open(f"{tmpdir}/file_1.avro", "r+b") as f:
            f.seek(8192)
            f.write(b"X" * 32)
        # Corrupt another file more broadly (block-level)
        size = os.path.getsize(f"{tmpdir}/file_2.avro")
        with open(f"{tmpdir}/file_2.avro", "r+b") as f:
            f.seek(size // 4)
            f.write(b"X" * (size // 8))
        # Yield the root directory to the caller
        yield tmpdir


class TestRepairTool:
    def test_ok(self, tmpdir):
        with tempfile.TemporaryDirectory() as outputdir:
            reports = RepairTool().repair(
                parse_url(f"{tmpdir}/file_0.avro", mode="rb"),
                parse_url(f"{outputdir}/file_0.avro", mode="wb"),
            )
            assert len(reports) == 1
            report = reports[0]
            assert report.count_corrupt_blocks == 0
            with avro_reader(
                parse_url(f"{outputdir}/file_0.avro", mode="rb")
            ) as reader:
                count = 0
                for record in reader:
                    assert isinstance(record, dict)
                    assert "name" in record
                    assert "age" in record
                    assert "emails" in record
                    assert isinstance(record["emails"], list)
                    assert len(record["emails"]) == 3
                    count += 1
                assert count == 5000

    def test_corrupted_record(self, tmpdir):
        with tempfile.TemporaryDirectory() as outputdir:
            input_url = parse_url(f"{tmpdir}/file_1.avro", mode="rb")
            output_url = parse_url(f"{outputdir}/file_1.avro", mode="wb")
            reports = RepairTool().repair(input_url, output_url)
            assert len(reports) == 1
            report = reports[0]
            assert report.count_corrupt_blocks > 0
            with avro_reader(output_url.with_mode("rb")) as reader:
                count = 0
                for record in reader:
                    assert isinstance(record, dict)
                    assert "name" in record
                    assert "age" in record
                    assert "emails" in record
                    assert isinstance(record["emails"], list)
                    assert len(record["emails"]) == 3
                    count += 1
                assert count < 5000

    def test_corrupted_blocks(self, tmpdir):
        with tempfile.TemporaryDirectory() as outputdir:
            input_url = parse_url(f"{tmpdir}/file_2.avro", mode="rb")
            output_url = parse_url(f"{outputdir}/file_2.avro", mode="wb")
            reports = RepairTool().repair(input_url, output_url)
            assert len(reports) == 1
            report = reports[0]
            assert report.count_corrupt_blocks > 0
            with avro_reader(output_url.with_mode("rb")) as reader:
                count = 0
                for record in reader:
                    assert isinstance(record, dict)
                    assert "name" in record
                    assert "age" in record
                    assert "emails" in record
                    assert isinstance(record["emails"], list)
                    assert len(record["emails"]) == 3
                    count += 1
                assert count < 5000

    def test_multiple(self, tmpdir):
        with tempfile.TemporaryDirectory() as outputdir:
            input_url = parse_url(tmpdir, mode="rb")
            output_url = parse_url(outputdir, mode="wb")
            reports = RepairTool().repair(input_url, output_url)
            # Check that corrupt blocks are detected
            assert len(reports) == 3
            reports_by_file = {
                os.path.basename(report.input_url): report for report in reports
            }
            assert reports_by_file["file_0.avro"].count_corrupt_blocks == 0
            assert reports_by_file["file_1.avro"].count_corrupt_blocks > 0
            assert reports_by_file["file_2.avro"].count_corrupt_blocks > 0
            # Check that the repaired files are valid
            for report in reports:
                with avro_reader(
                    parse_url(report.output_url).with_mode("rb")
                ) as reader:
                    count = 0
                    for record in reader:
                        assert isinstance(record, dict)
                        assert "name" in record
                        assert "age" in record
                        assert "emails" in record
                        assert isinstance(record["emails"], list)
                        assert len(record["emails"]) == 3
                        count += 1
                    if report.count_corrupt_blocks > 0:
                        assert count < 5000
                    else:
                        assert count == 5000
