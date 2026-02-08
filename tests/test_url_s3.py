# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import boto3
import pytest
from avrokit.url import parse_url
from avrokit.io import avro_schema, avro_writer, avro_reader
from moto import mock_aws
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
def s3_bucket():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket_name = "test-bucket"
        s3.create_bucket(Bucket=bucket_name)
        yield "s3://test-bucket"


class TestS3URL:
    def test_crud(self, s3_bucket):
        url = parse_url(f"{s3_bucket}/test/text/file.txt")
        # Clean up any existing file
        if url.exists():
            url.delete()
        assert url.exists() is False
        # Expand the URL to see the file (should just be the one)
        assert url.expand() == [url]
        # Create a new file
        with url.with_mode("w") as f:
            f.write("This is a test file.")
        assert url.exists() is True
        # Read the file
        with url.with_mode("r") as f:
            data = f.read()
            assert data == "This is a test file."
        # Delete the file
        url.delete()
        assert url.exists() is False

    def test_avro(self, s3_bucket):
        url = parse_url(f"{s3_bucket}/test/avro/file.avro")
        # Clean up any existing file
        if url.exists():
            url.delete()
        assert url.exists() is False
        # Write some avro records
        with avro_writer(url.with_mode("wb"), SCHEMA) as writer:
            for _ in range(1000):
                writer.append(
                    {
                        "name": faker.name(),
                        "age": faker.random_int(min=18, max=80),
                        "emails": [faker.email() for _ in range(3)],
                    }
                )
        assert url.exists() is True
        # Read the avro records
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
            assert count == 1000
        # Delete the file
        url.delete()
        assert url.exists() is False
