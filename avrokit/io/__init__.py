# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

from .compact import compact_avro_data
from .reader import PartitionedAvroReader, avro_reader, avro_records
from .schema import (
    add_avro_schema_fields,
    avro_schema,
    read_avro_schema,
    read_avro_schema_from_first_nonempty_file,
    validate_avro_schema_evolution,
)
from .writer import (
    Appendable,
    PartitionedAvroWriter,
    TimePartitionedAvroWriter,
    avro_writer,
)

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
