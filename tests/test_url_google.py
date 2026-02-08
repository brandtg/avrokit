# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import pytest
import docker
import docker.errors
import tempfile
import os
from avrokit.url.factory import parse_url
from avrokit.url.google import google_cloud_storage_client
from avrokit.io import avro_schema, avro_writer, avro_reader
from faker import Faker
from filelock import FileLock

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

# N.b. needed because the fake-gcs-server container management is not thread-safe from parallel test
# processes. Each will start/stop it while holding this lock.
GCS_TEST_LOCK = FileLock(
    os.path.join(tempfile.gettempdir(), "test_url_google.lock"), timeout=60
)


def start_fake_gcs_server(client: docker.DockerClient):
    try:
        container = client.containers.get("fake-gcs-server")
        if container.status != "running":
            container.start()
    except docker.errors.NotFound:
        try:
            container = client.containers.run(
                "fsouza/fake-gcs-server",
                "-scheme http",
                detach=True,
                name="fake-gcs-server",
                ports={"4443/tcp": 4443},
            )
        except docker.errors.APIError as e:
            if e.status_code == 409:
                return  # Container already exists
            raise e


def stop_fake_gcs_server(client: docker.DockerClient):
    container = client.containers.get("fake-gcs-server")
    try:
        if container and container.status == "running":
            container.stop()
            container.remove()
    except docker.errors.NotFound:
        pass


@pytest.fixture(scope="module")
def gcs_bucket():
    with GCS_TEST_LOCK:
        # Configure dummy environment variables for testing
        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "test-project")
        monkeypatch.setenv("GOOGLE_CLOUD_STORAGE_USE_ANONYMOUS_CREDENTIALS", "true")
        monkeypatch.setenv("GOOGLE_CLOUD_STORAGE_API_ENDPOINT", "http://localhost:4443")
        # Start the fake-gcs-server via docker if not already running
        docker_client = docker.from_env()
        start_fake_gcs_server(docker_client)
        with google_cloud_storage_client() as client:
            client.create_bucket("test-bucket")
        yield "gs://test-bucket"
        stop_fake_gcs_server(docker_client)
        monkeypatch.undo()


class TestGoogleCloudStorageURL:
    def test_crud(self, gcs_bucket):
        url = parse_url(f"{gcs_bucket}/test/text/file.txt")
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
        # Expand the parent URL to see this file
        assert parse_url(f"{gcs_bucket}/test/text").expand() == [url]
        # Check the size
        assert url.size() == len("This is a test file.")
        # Read the file
        with url.with_mode("r") as f:
            data = f.read()
            assert data == "This is a test file."
        # Delete the file
        url.delete()
        assert url.exists() is False

    def test_avro(self, gcs_bucket):
        url = parse_url(f"{gcs_bucket}/test/avro/file.avro")
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
