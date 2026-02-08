<!--
SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>

SPDX-License-Identifier: Apache-2.0
-->

# avrokit

Python utilities for working with Avro data files with support for local files, GCS, S3, and HTTP/HTTPS URLs.

## Features

- **Unified URL Interface**: Read and write Avro files transparently across local filesystem, GCS, S3, and HTTP/HTTPS
- **CLI Tools**: Comprehensive command-line tools for common Avro operations
- **Python API**: Rich programmatic interface for reading, writing, and transforming Avro data
- **Partitioned I/O**: Support for reading and writing partitioned/multi-file datasets
- **Async Support**: Non-blocking I/O primitives for high-throughput applications
- **Parquet Conversion**: Bidirectional conversion between Avro and Parquet formats
- **Schema Operations**: Schema extraction, validation, and evolution checking

## Installation

### Basic Installation

```bash
pip install avrokit
```

### With Cloud Storage Support

```bash
# AWS S3 support
pip install avrokit[aws]

# Google Cloud Storage support
pip install avrokit[gcp]

# All extras (S3 + GCS)
pip install avrokit[all]
```

### Development Installation

```bash
git clone https://github.com/brandtg/avrokit.git
cd avrokit
poetry env use python3.12
make install
```

## Quick Start

### CLI Usage

```bash
# View statistics for an Avro file
avrokit stats gs://bucket/data.avro

# Convert Avro to JSON
avrokit tojson s3://bucket/data.avro > output.json

# Extract sample records
avrokit cat file:///path/to/data.avro --limit 10

# Get schema
avrokit getschema data.avro

# Convert to Parquet
avrokit toparquet data.avro output.parquet

# Concatenate multiple files
avrokit concat file1.avro file2.avro s3://bucket/file3.avro output.avro
```

### Python API

```python
from avrokit import avro_reader, avro_records, avro_writer, avro_schema, parse_url

# Read records from any URL
url = parse_url('gs://bucket/data.avro', mode='rb')
for record in avro_records(url):
    print(record)

# Write Avro data
schema = avro_schema({
    'type': 'record',
    'name': 'User',
    'fields': [
        {'name': 'name', 'type': 'string'},
        {'name': 'age', 'type': 'int'}
    ]
})

url = parse_url('output.avro', mode='wb')
with avro_writer(url, schema) as writer:
    writer.append({'name': 'Alice', 'age': 30})
    writer.append({'name': 'Bob', 'age': 25})
```

## Architecture

avrokit is organized into four main layers:

### 1. URL Layer (`avrokit.url`)

Provides a unified abstraction over different storage backends. All URL types implement the `URL` protocol with common operations:

- **`URL`** (base protocol): Abstract interface for all URL types
- **`FileURL`**: Local filesystem access with glob pattern support
- **`GCSURL`**: Google Cloud Storage via `google-cloud-storage`
- **`S3URL`**: Amazon S3 via `boto3`
- **`HTTPURL`**: HTTP/HTTPS with range request support for streaming

The `parse_url()` factory function automatically instantiates the correct URL class based on the scheme:

```python
from avrokit.url import parse_url

# All of these return the appropriate URL subclass
url1 = parse_url('file:///path/to/data.avro')      # FileURL
url2 = parse_url('gs://bucket/data.avro')           # GCSURL
url3 = parse_url('s3://bucket/data.avro')           # S3URL
url4 = parse_url('https://example.com/data.avro')   # HTTPURL
```

### 2. I/O Layer (`avrokit.io`)

High-level Avro reading and writing primitives built on the URL layer:

**Reading:**

- `avro_reader(url)`: Context manager for `DataFileReader`
- `avro_records(url)`: Generator yielding records as dictionaries
- `PartitionedAvroReader`: Reads from multiple files or glob patterns as a single stream

**Writing:**

- `avro_writer(url, schema)`: Context manager for `DataFileWriter`
- `PartitionedAvroWriter`: Writes to multiple files with configurable size limits
- `TimePartitionedAvroWriter`: Time-based partitioning (e.g., hourly/daily files)

**Schema Operations:**

- `avro_schema(dict)`: Create schema from dictionary
- `read_avro_schema(url)`: Extract schema from file
- `validate_avro_schema_evolution()`: Check backward/forward compatibility
- `add_avro_schema_fields()`: Schema augmentation utilities

**Utilities:**

- `compact_avro_data()`: Remove deleted/updated records by key

### 3. Async I/O Layer (`avrokit.asyncio`)

Non-blocking primitives for async applications:

- `DeferredAvroWriter`: Async writer that batches records and flushes in background
- `BlockingQueueAvroReader`: Queue-based async reader for producer/consumer patterns

### 4. Tools Layer (`avrokit.tools`)

CLI commands implemented as classes following the `Tool` protocol:

| Tool              | Command       | Description                                          |
| ----------------- | ------------- | ---------------------------------------------------- |
| `CatTool`         | `cat`         | Extract sample records with optional random sampling |
| `ConcatTool`      | `concat`      | Concatenate multiple Avro files into one             |
| `CountTool`       | `count`       | Count records in files                               |
| `FileSortTool`    | `filesort`    | Sort records by key across multiple files            |
| `FromParquetTool` | `fromparquet` | Convert Parquet to Avro                              |
| `GetMetaTool`     | `getmeta`     | Extract file metadata (schema, compression, etc.)    |
| `GetSchemaTool`   | `getschema`   | Extract and print schema                             |
| `HttpServerTool`  | `httpserver`  | Serve Avro files over HTTP with filtering/sampling   |
| `PartitionTool`   | `partition`   | Split files into multiple partitions                 |
| `RepairTool`      | `repair`      | Fix corrupted Avro files                             |
| `StatsTool`       | `stats`       | Compute statistics (count, nulls, sizes)             |
| `ToJsonTool`      | `tojson`      | Convert to newline-delimited JSON                    |
| `ToParquetTool`   | `toparquet`   | Convert to Parquet format                            |

## CLI Reference

### Global Options

```
avrokit --debug <command>    # Enable debug logging
```

### Available Commands

#### cat - Extract Sample Records

```bash
avrokit cat FILE [OPTIONS]
  --limit N          Maximum records to output (default: 10)
  --sample-rate F    Random sampling rate (0.0-1.0)
```

#### concat - Concatenate Files

```bash
avrokit concat INPUT1 [INPUT2 ...] OUTPUT
  INPUT can be local files, GCS, S3, or HTTP URLs
  OUTPUT schema must be compatible with all inputs
```

#### count - Count Records

```bash
avrokit count FILE [FILE ...]
  Returns total record count across all files
```

#### filesort - Sort Records

```bash
avrokit filesort INPUT OUTPUT --keys KEY1 [KEY2 ...]
  Sorts records by specified keys using external merge sort
```

#### fromparquet - Parquet to Avro

```bash
avrokit fromparquet INPUT.parquet OUTPUT.avro
```

#### getmeta - File Metadata

```bash
avrokit getmeta FILE
  Outputs: schema, codec, sync marker, block count, etc.
```

#### getschema - Extract Schema

```bash
avrokit getschema FILE
  Outputs Avro schema as JSON
```

#### httpserver - HTTP Server

```bash
avrokit httpserver --port 8080 --files "*.avro"
  Serves Avro files with filtering and sampling support
```

#### partition - Partition Files

```bash
avrokit partition INPUT OUTPUT --count N
  Splits INPUT into N approximately equal partitions
```

#### repair - Fix Corrupted Files

```bash
avrokit repair INPUT OUTPUT
  Attempts to recover readable records from damaged files
```

#### stats - Compute Statistics

```bash
avrokit stats FILE [FILE ...]
  Outputs: record count, file sizes, null counts per field
```

#### tojson - Convert to JSON

```bash
avrokit tojson FILE [FILE ...]
  One JSON record per line (newline-delimited JSON)
```

#### toparquet - Convert to Parquet

```bash
avrokit toparquet INPUT.avro OUTPUT.parquet
  Type mapping: Avro types → Parquet types
```

## Development

### Prerequisites

- Python 3.12+
- Poetry
- Docker (for GCS/S3 integration tests)

### Setup

```bash
# Set up Python environment
poetry env use python3.12

# Install dependencies
make install

# Pull fake-gcs-server for testing
docker pull fsouza/fake-gcs-server
```

### Running Tests

```bash
# Run all tests
make test

# Run with coverage
make test-coverage

# Run tests in parallel (default)
poetry run pytest -n auto
```

### Code Quality

```bash
# Format code
make format

# Lint code
make lint

# Type checking
make typecheck

# Add license headers
make license
```

### Building

```bash
make build    # Build wheel and source distribution
```

## Python API Examples

### Working with Partitioned Files

```python
from avrokit import PartitionedAvroReader, PartitionedAvroWriter, parse_url

# Read from multiple files as single stream
url = parse_url('data-*.avro', mode='rb')
with PartitionedAvroReader(url) as reader:
    for record in reader:
        process(record)

# Write to multiple files (roll every 10000 records)
url = parse_url('output/', mode='wb')
with PartitionedAvroWriter(url, schema, max_records=10000) as writer:
    for record in records:
        writer.append(record)
        if should_roll():
            writer.roll()  # Create new partition file
```

### Time-Based Partitioning

```python
from avrokit import TimePartitionedAvroWriter, parse_url
from datetime import datetime

# Creates hourly files: output/2024/01/15/10.avro
url = parse_url('output/', mode='wb')
with TimePartitionedAvroWriter(
    url,
    schema,
    time_granularity='hour',
    time_field='timestamp'
) as writer:
    for record in records:
        writer.append(record, timestamp=record['timestamp'])
```

### Async Operations

```python
import asyncio
from avrokit.asyncio import DeferredAvroWriter
from avrokit import parse_url, avro_schema

async def write_async():
    url = parse_url('output.avro', mode='wb')
    schema = avro_schema({...})

    async with DeferredAvroWriter(url, schema) as writer:
        for record in records:
            await writer.append(record)
        # Flushes happen automatically in background

asyncio.run(write_async())
```

### Schema Evolution

```python
from avrokit import validate_avro_schema_evolution, read_avro_schema
from avrokit.url import parse_url

# Check if new schema is backward compatible
reader_schema = read_avro_schema(parse_url('old.avro', 'rb'))
writer_schema = read_avro_schema(parse_url('new.avro', 'rb'))

is_valid = validate_avro_schema_evolution(
    reader_schema,
    writer_schema,
    strategy='backward'  # or 'forward', 'full'
)
```

## License

Apache-2.0 - See [LICENSE](LICENSE) for details.

## Contributing

Contributions welcome! Please ensure:

1. Code follows the existing style (enforced by `make format` and `make lint`)
2. All tests pass (`make test`)
3. Type checks pass (`make typecheck`)
4. License headers are present (`make license`)
