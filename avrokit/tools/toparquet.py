# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

from ..io import read_avro_schema, avro_reader
from ..url import parse_url, URL
from typing import cast
import argparse
import avro.schema  # type: ignore
import pyarrow as pa  # type: ignore
import pyarrow.parquet as pq  # type: ignore

DEFAULT_BATCH_SIZE = 1000

AVRO_TO_PARQUET_TYPE_MAP: dict[str, pa.DataType] = {
    "null": pa.null(),
    "boolean": pa.bool_(),
    "int": pa.int32(),
    "long": pa.int64(),
    "float": pa.float32(),
    "double": pa.float64(),
    "bytes": pa.binary(),
    "string": pa.string(),
}


def avro_type_to_parquet_type(avro_type: avro.schema.Schema) -> pa.DataType:
    if isinstance(avro_type, avro.schema.PrimitiveSchema):
        # Primitive types
        if avro_type.type not in AVRO_TO_PARQUET_TYPE_MAP:
            raise ValueError(f"Unsupported Avro type: {avro_type.type}")
        return AVRO_TO_PARQUET_TYPE_MAP[avro_type.type]
    elif isinstance(avro_type, avro.schema.ArraySchema):
        # Array types
        if avro_type.items is None:
            raise ValueError("Array schema must have items type.")
        return pa.list_(
            avro_type_to_parquet_type(cast(avro.schema.Schema, avro_type.items))
        )
    elif isinstance(avro_type, avro.schema.MapSchema):
        # Map types
        if avro_type.values is None:
            raise ValueError("Map schema must have values type.")
        return pa.map_(
            pa.string(),
            avro_type_to_parquet_type(cast(avro.schema.Schema, avro_type.values)),
        )
    elif isinstance(avro_type, avro.schema.RecordSchema):
        # Record types
        fields = [
            pa.field(field.name, avro_type_to_parquet_type(field.type))
            for field in cast(list[avro.schema.Field], avro_type.fields)
        ]
        return pa.struct(fields)
    elif isinstance(avro_type, avro.schema.UnionSchema):
        # Union types
        # TODO Support more than just union with null?
        non_null_types = [t for t in avro_type.schemas if not t.type == "null"]
        if len(non_null_types) == 1:
            return avro_type_to_parquet_type(non_null_types[0])
        else:
            raise ValueError(f"Unsupported union type: {avro_type}")
    else:
        raise ValueError(f"Unsupported Avro type: {avro_type}")


def avro_schema_to_parquet_schema(avro_schema: avro.schema.Schema) -> pa.Schema:
    if not isinstance(avro_schema, avro.schema.RecordSchema):
        raise ValueError("Avro schema must be a record schema.")
    return pa.schema(
        [
            pa.field(field.name, avro_type_to_parquet_type(field.type))
            for field in cast(list[avro.schema.Field], avro_schema.fields)
        ]
    )


def avro_to_parquet(
    input_url: URL, output_url: URL, batch_size: int = DEFAULT_BATCH_SIZE
) -> None:
    # Convert Avro schema to Parquet schema
    avro_schema = read_avro_schema(input_url)
    if not isinstance(avro_schema, avro.schema.RecordSchema):
        raise ValueError("Avro schema must be a record schema.")
    parquet_schema = avro_schema_to_parquet_schema(avro_schema)
    # Map the Avro records to Parquet records in batches
    with avro_reader(input_url.with_mode("rb")) as reader, output_url.with_mode(
        "wb"
    ) as parquet_stream, pq.ParquetWriter(parquet_stream, parquet_schema) as writer:
        batch = []
        for record in reader:
            # Convert Avro record to Parquet record
            if not isinstance(record, dict):
                raise ValueError("Avro record must be a dictionary.")
            parquet_record = {
                field.name: record[field.name]
                for field in cast(list[avro.schema.Field], avro_schema.fields)
            }
            batch.append(parquet_record)
            # Flush the batch to Parquet
            if len(batch) == batch_size:
                table = pa.Table.from_pylist(batch, schema=parquet_schema)
                writer.write_table(table)
                batch = []
        # Write any remaining records
        if batch:
            table = pa.Table.from_pylist(batch, schema=parquet_schema)
            writer.write_table(table)


class ToParquetTool:
    """
    Converts Avro data file(s) to Parquet format.
    """

    def name(self) -> str:
        return "toparquet"

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
            "--batch_size",
            type=int,
            default=DEFAULT_BATCH_SIZE,
            help="Number of records to process in each batch.",
        )

    def convert(
        self, input_url: URL, output_url: URL, batch_size: int = DEFAULT_BATCH_SIZE
    ) -> None:
        avro_to_parquet(
            input_url=input_url,
            output_url=output_url,
            batch_size=batch_size,
        )

    def run(self, args: argparse.Namespace) -> None:
        self.convert(
            input_url=parse_url(args.input_url),
            output_url=parse_url(args.output_url),
            batch_size=args.batch_size,
        )
