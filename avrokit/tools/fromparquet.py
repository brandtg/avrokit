# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import argparse
import avro.schema
import pyarrow as pa  # type: ignore
import pyarrow.parquet as pq  # type: ignore
from typing import Any, Generator
from pyarrow.types import (  # type: ignore
    is_int32,
    is_int64,
    is_float32,
    is_float64,
    is_boolean,
    is_string,
    is_large_string,
    is_timestamp,
    is_date,
    is_null,
    is_struct,
    is_map,
    is_list,
)

from ..io import avro_schema, avro_writer
from ..url import parse_url, URL


def read_parquet_schema(url: URL) -> pa.Schema:
    """
    Reads the Parquet schema from a file at the given URL.
    """
    with url as f:
        return pq.read_schema(f)


def wrap_nullable(avro_type: Any, nullable: bool) -> Any:
    """
    Wraps a field in a nullable schema.

    :param field: The field to wrap.
    :param nullable: Whether the field is nullable.
    :return: The wrapped field.
    """
    return ["null", avro_type] if nullable else avro_type


# N.b. this is still operating with dict schemas, and will later convert to Field types
def parquet_type_to_avro_type(parquet_type: pa.Field) -> Any:
    avro_type: Any = None
    if is_null(parquet_type.type):
        avro_type = "null"
    elif is_int32(parquet_type.type):
        avro_type = "int"
    elif is_int64(parquet_type.type):
        avro_type = "long"
    elif is_float32(parquet_type.type):
        avro_type = "float"
    elif is_float64(parquet_type.type):
        avro_type = "double"
    elif is_string(parquet_type.type) or is_large_string(parquet_type.type):
        avro_type = "string"
    elif is_boolean(parquet_type.type):
        avro_type = "boolean"
    elif is_timestamp(parquet_type.type) or is_date(parquet_type.type):
        avro_type = "long"
    elif is_struct(parquet_type.type):
        avro_type = {
            "name": parquet_type.name,
            "type": {
                "type": "record",
                "name": parquet_type.name,
                "fields": [
                    {
                        "name": child.name,
                        "type": parquet_type_to_avro_type(child),
                    }
                    for child in parquet_type.type
                ],
            },
        }
    elif is_map(parquet_type.type):
        key_field = parquet_type.type.key_field
        if not is_string(key_field.type):
            raise ValueError(
                f"Map key field must be a string, but got {key_field.type}"
            )
        value_field = parquet_type.type.item_field
        avro_type = {
            "name": parquet_type.name,
            "type": {
                "type": "map",
                "values": parquet_type_to_avro_type(value_field),
            },
        }
    elif is_list(parquet_type.type):
        child = parquet_type.type.value_field  # ListType has a single child
        avro_type = {
            "type": "array",
            "items": parquet_type_to_avro_type(child),
        }
    else:
        raise ValueError(f"Unsupported Arrow type: {parquet_type.type}")
    return wrap_nullable(avro_type, parquet_type.nullable)


def parquet_schema_to_avro_schema(
    parquet_schema: pa.Schema, name: str, namespace: str | None = None
) -> avro.schema.Schema:
    return avro_schema(
        {
            "type": "record",
            "name": name,
            "namespace": namespace,
            "fields": [
                {
                    "name": field.name,
                    "type": parquet_type_to_avro_type(field),
                }
                for field in parquet_schema
            ],
        }
    )


def convert_parquet_table_to_avro_records(
    table: pa.Table,
) -> Generator[object, None, None]:
    vectors = [table.column(i) for i in range(table.num_columns)]
    fields = table.schema
    row_count = table.num_rows
    for i in range(row_count):
        record = {}
        for j, vector in enumerate(vectors):
            field = fields[j]
            value = vector[i].as_py()
            record[field.name] = value
        yield record


def parquet_to_avro(
    input_url: URL, output_url: URL, name: str, namespace: str | None
) -> None:
    # Convert the Parquet schema to Avro schema
    parquet_schema = read_parquet_schema(input_url)
    schema = parquet_schema_to_avro_schema(parquet_schema, name, namespace)
    # Map the Parquet tables to Avro records
    with input_url.with_mode("rb") as parquet_stream, pq.ParquetFile(
        parquet_stream
    ) as parquet_file, avro_writer(output_url.with_mode("wb"), schema) as writer:
        for row_group_index in range(parquet_file.num_row_groups):
            # Load the current row group as a Table
            table = parquet_file.read_row_group(row_group_index)
            # Convert the batch to Avro records
            for record in convert_parquet_table_to_avro_records(table):
                writer.append(record)


class FromParquetTool:
    """
    Converts Avro data file(s) to Parquet format.
    """

    def name(self) -> str:
        return "fromparquet"

    def configure(self, subparsers: argparse._SubParsersAction) -> None:
        parser = subparsers.add_parser(
            self.name(),
            help="Dumps Avro data file(s) as JSON, record per line.",
        )
        parser.add_argument(
            "input_url",
            help="URL of the input Avro data file.",
        )
        parser.add_argument(
            "output_url",
            help="URL of the output Parquet data file.",
        )
        parser.add_argument(
            "--name",
            default="Record",
            help="Name of the Avro schema.",
        )
        parser.add_argument(
            "--namespace",
            help="Namespace of the Avro schema.",
        )

    def convert(
        self, input_url: URL, output_url: URL, name: str, namespace: str | None
    ) -> None:
        parquet_to_avro(
            input_url=input_url,
            output_url=output_url,
            name=name,
            namespace=namespace,
        )

    def run(self, args: argparse.Namespace) -> None:
        self.convert(
            input_url=parse_url(args.input_url),
            output_url=parse_url(args.output_url),
            name=args.name,
            namespace=args.namespace,
        )
