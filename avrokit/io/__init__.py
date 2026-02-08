# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

from .schema import (
    add_avro_schema_fields,
    avro_schema,
    read_avro_schema,
    read_avro_schema_from_first_nonempty_file,
    validate_avro_schema_evolution,
)
from .reader import avro_reader, PartitionedAvroReader, avro_records
from .writer import (
    avro_writer,
    Appendable,
    PartitionedAvroWriter,
    TimePartitionedAvroWriter,
)
from .compact import compact_avro_data

__all__ = [
    "Appendable",
    "PartitionedAvroReader",
    "PartitionedAvroWriter",
    "TimePartitionedAvroWriter",
    "add_avro_schema_fields",
    "avro_reader",
    "avro_schema",
    "avro_writer",
    "avro_records",
    "compact_avro_data",
    "read_avro_schema",
    "read_avro_schema_from_first_nonempty_file",
    "validate_avro_schema_evolution",
]
