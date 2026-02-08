# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

from queue import Empty
import tempfile
import time
from avrokit.io import avro_schema, avro_reader, avro_writer
from avrokit.asyncio import DeferredAvroWriter, BlockingQueueAvroReader
from faker import Faker

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


class TestDeferredAvroWriter:
    def test_deferred_avro_writer(self):
        with tempfile.NamedTemporaryFile(mode="wb") as tmp:
            # Create a simple Avro writer to a temporary file
            url = parse_url(tmp.name, mode="wb")
            with avro_writer(url, SCHEMA) as writer:
                # Create a deferred writer
                deferred_writer = DeferredAvroWriter(writer)
                try:
                    deferred_writer.start()
                    # Append some records
                    for _ in range(10):
                        deferred_writer.append(
                            {
                                "name": faker.name(),
                                "age": faker.random_int(min=18, max=80),
                                "emails": [faker.email() for _ in range(3)],
                            }
                        )
                    # Wait for a bit to simulate async behavior
                    time.sleep(0.1)
                    writer.flush()
                    # Read the file back
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
                        assert count == 10
                finally:
                    deferred_writer.stop()


class TestBlockingQueueAvroReader:
    def test_blocking_queue_avro_reader(self):
        with tempfile.NamedTemporaryFile(mode="wb") as tmp:
            # Write some data to a temporary file
            url = parse_url(tmp.name, mode="wb")
            with avro_writer(url, SCHEMA) as writer:
                for _ in range(100):
                    writer.append(
                        {
                            "name": faker.name(),
                            "age": faker.random_int(min=18, max=80),
                            "emails": [faker.email() for _ in range(3)],
                        }
                    )
            # Read the data back using BlockingQueueReader
            with avro_reader(url.with_mode("rb")) as reader:
                count = 0
                blocking_reader = BlockingQueueAvroReader(reader)
                try:
                    blocking_reader.start()
                    while not blocking_reader.empty():
                        record = blocking_reader.queue.get(timeout=1)
                        assert isinstance(record, dict)
                        assert "name" in record
                        assert "age" in record
                        assert "emails" in record
                        assert isinstance(record["emails"], list)
                        assert len(record["emails"]) == 3
                        count += 1
                except Empty:
                    # This exception is expected if the queue is empty
                    pass
                finally:
                    blocking_reader.stop()
                    assert count == 100
