# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

from avrokit.io import avro_writer, avro_schema
from avrokit.io.reader import avro_reader
from avrokit.tools import HttpServerTool
from avrokit.url import parse_url
from faker import Faker
import json
import pytest
import requests
import tempfile
import threading

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
def tmpserver():
    with tempfile.TemporaryDirectory() as tmpdir:
        root_url = parse_url(tmpdir)
        # Create a temporary Avro file
        with avro_writer(
            root_url.with_path("test.avro").with_mode("wb"), SCHEMA
        ) as writer:
            for _ in range(10):
                writer.append(
                    {
                        "name": faker.name(),
                        "age": faker.random_int(min=18, max=99),
                        "emails": [faker.email() for _ in range(3)],
                    }
                )
        # Create the HTTP server tool in a daemon thread
        tool = HttpServerTool()
        server_thread = threading.Thread(
            target=tool.start, args=(root_url, 0), daemon=True
        )
        server_thread.start()
        tool.wait_until_started(timeout=5)
        http_url = f"http://localhost:{tool.port}"
        yield http_url, root_url
        tool.stop()
        server_thread.join()


class TestHttpServerTool:
    def test_get_schema(self, tmpserver):
        http_url, _ = tmpserver
        res = requests.get(f"{http_url}/test.avro", params={"schema": "true"})
        res.raise_for_status()
        assert res.json() == SCHEMA.to_json()

    def test_get_filenames(self, tmpserver):
        http_url, _ = tmpserver
        res = requests.get(http_url)
        res.raise_for_status()
        filenames = res.json()
        assert isinstance(filenames, list)
        assert "test.avro" in filenames

    def test_get_json(self, tmpserver):
        http_url, _ = tmpserver
        res = requests.get(
            # N.b. anything with JSON maps to application/jsonl
            f"{http_url}/test.avro",
            headers={"Accept": "application/json"},
        )
        res.raise_for_status()
        assert res.headers["Content-Type"] == "application/jsonl"
        data = [json.loads(line) for line in res.content.decode("utf-8").splitlines()]
        assert len(data) == 10
        for item in data:
            assert "name" in item
            assert "age" in item
            assert "emails" in item
            assert isinstance(item["emails"], list)
            assert len(item["emails"]) == 3
            for email in item["emails"]:
                assert isinstance(email, str)
                assert "@" in email

    def test_get_avro(self, tmpserver):
        http_url, _ = tmpserver
        res = requests.get(
            f"{http_url}/test.avro", headers={"Accept": "application/avro"}
        )
        res.raise_for_status()
        assert res.headers["Content-Type"] == "application/avro"
        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(res.content)
            tmp.flush()
            with avro_reader(parse_url(tmp.name)) as reader:
                count = 0
                for _ in reader:
                    count += 1
                assert count == 10

    def test_crud(self, tmpserver):
        http_url, root_url = tmpserver
        # Create a new Avro file
        res = requests.put(
            f"{http_url}/newfile.avro",
            json=SCHEMA.to_json(),
        )
        res.raise_for_status()
        assert res.status_code == 201
        assert root_url.with_path("newfile.avro").exists()
        # Add some records
        for _ in range(5):
            res = requests.post(
                f"{http_url}/newfile.avro",
                json={
                    "name": faker.name(),
                    "age": faker.random_int(min=18, max=99),
                    "emails": [faker.email() for _ in range(3)],
                },
            )
            res.raise_for_status()
            assert res.status_code == 200
        assert root_url.with_path("newfile.avro").size() > 0
        # Read the records
        res = requests.get(
            f"{http_url}/newfile.avro",
            headers={"Accept": "application/json"},
        )
        res.raise_for_status()
        assert res.headers["Content-Type"] == "application/jsonl"
        data = [json.loads(line) for line in res.content.decode("utf-8").splitlines()]
        assert len(data) == 5
        # Compare it against disk
        with avro_reader(root_url.with_path("newfile.avro").with_mode("rb")) as reader:
            data_from_disk = [record for record in reader]
            assert data == data_from_disk
        # Delete the file
        res = requests.delete(f"{http_url}/newfile.avro")
        res.raise_for_status()
        assert res.status_code == 204
        assert not root_url.with_path("newfile.avro").exists()
        # Check that the file is gone
        res = requests.get(f"{http_url}/newfile.avro")
        assert res.status_code == 404
