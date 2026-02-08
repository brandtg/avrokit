# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

"""
Python utilities for working with Avro data files.

Basic Usage Examples
--------------------

Reading Avro Files:
    >>> from avrokit import avro_reader, avro_records, parse_url
    >>>
    >>> # Read all records from an Avro file
    >>> url = parse_url('file:///path/to/data.avro', mode='rb')
    >>> for record in avro_records(url):
    ...     print(record)
    >>>
    >>> # Use the reader context manager directly
    >>> with avro_reader(url) as reader:
    ...     schema = reader.datum_reader.writers_schema
    ...     for record in reader:
    ...         print(record)

Writing Avro Files:
    >>> from avrokit import avro_writer, avro_schema, parse_url
    >>>
    >>> # Define your schema
    >>> schema = avro_schema({
    ...     'type': 'record',
    ...     'name': 'User',
    ...     'fields': [
    ...         {'name': 'name', 'type': 'string'},
    ...         {'name': 'age', 'type': 'int'}
    ...     ]
    ... })
    >>>
    >>> # Write records to a new file
    >>> url = parse_url('file:///path/to/output.avro', mode='wb')
    >>> with avro_writer(url, schema) as writer:
    ...     writer.append({'name': 'Alice', 'age': 30})
    ...     writer.append({'name': 'Bob', 'age': 25})
    >>>
    >>> # Append to an existing file
    >>> url = parse_url('file:///path/to/output.avro', mode='ab')
    >>> with avro_writer(url) as writer:
    ...     writer.append({'name': 'Charlie', 'age': 35})

Working with Remote Files (GCS, S3):
    >>> # Google Cloud Storage
    >>> url = parse_url('gs://bucket/path/to/data.avro', mode='rb')
    >>> for record in avro_records(url):
    ...     print(record)
    >>>
    >>> # Amazon S3
    >>> url = parse_url('s3://bucket/path/to/data.avro', mode='rb')
    >>> for record in avro_records(url):
    ...     print(record)

Reading Multiple Files:
    >>> from avrokit import PartitionedAvroReader
    >>>
    >>> # Read from multiple files or glob patterns
    >>> url = parse_url('file:///path/to/data-*.avro', mode='rb')
    >>> with PartitionedAvroReader(url) as reader:
    ...     for record in reader:
    ...         print(record)

Writing Partitioned Files:
    >>> from avrokit import PartitionedAvroWriter
    >>>
    >>> url = parse_url('file:///path/to/output/', mode='wb')
    >>> with PartitionedAvroWriter(url, schema) as writer:
    ...     for i in range(100):
    ...         writer.append({'name': f'User{i}', 'age': i})
    ...         if i % 10 == 0:
    ...             writer.roll()  # Create a new partition file
"""

from .url import URL, parse_url, create_url_mapping
from .io import (
    PartitionedAvroReader,
    PartitionedAvroWriter,
    TimePartitionedAvroWriter,
    add_avro_schema_fields,
    avro_reader,
    avro_schema,
    avro_writer,
    avro_records,
    compact_avro_data,
    validate_avro_schema_evolution,
)
from .asyncio import DeferredAvroWriter, BlockingQueueAvroReader

__all__ = [
    "BlockingQueueAvroReader",
    "DeferredAvroWriter",
    "PartitionedAvroReader",
    "PartitionedAvroWriter",
    "TimePartitionedAvroWriter",
    "URL",
    "add_avro_schema_fields",
    "avro_reader",
    "avro_schema",
    "avro_writer",
    "avro_records",
    "compact_avro_data",
    "create_url_mapping",
    "parse_url",
    "validate_avro_schema_evolution",
]
