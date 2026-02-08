# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import json
from typing import Sequence, Union, Any
from avro.schema import Field, RecordSchema, Schema, UnionSchema, parse, EnumSchema
from avro.io import DatumReader
from avro.datafile import DataFileReader
from ..url import URL


def read_avro_schema(url: URL) -> Schema:
    """
    Reads the Avro schema from a file at the given URL.
    """
    with url as f, DataFileReader(f, DatumReader()) as reader:
        return parse(reader.schema)


def read_avro_schema_from_first_nonempty_file(urls: Sequence[URL]) -> Schema | None:
    """
    Reads the Avro schema from the first non-empty file in a sequence of URLs.
    """
    for url in urls:
        if url.exists() and url.size() > 0:
            return read_avro_schema(url)
    return None


def avro_schema(schema: Union[str, dict]) -> Schema:
    """
    Converts a dictionary schema to an Avro Schema object.

    :param schema: The dictionary schema to convert.
    :return: An Avro Schema object.
    """
    if isinstance(schema, str):
        return parse(schema)
    else:
        return parse(json.dumps(schema))


def add_avro_schema_fields(schema: Schema, fields: Sequence[dict[str, Any]]) -> Schema:
    """
    Adds fields to an Avro schema.
    """
    schema_dict = schema.to_json()
    if not isinstance(schema_dict, dict):
        raise ValueError("Schema is not a valid Avro record schema.")
    schema_dict["fields"].extend(fields)
    return avro_schema(schema_dict)


def flatten_avro_schema_fields(
    schema: Schema, path: list[str] | None = None
) -> dict[str, Field]:
    """
    Flattens the Avro schema fields into a dictionary with dot-notation keys.

    :param schema: The Avro schema to flatten.
    :param path: The current path in the schema (used for recursion).
    :return: A dictionary with dot-notation keys and Field objects as values.
    """
    acc = {}
    if isinstance(schema, RecordSchema) and isinstance(schema.fields, list):
        for field in schema.fields:
            # Resolve the field name
            name = []
            if path is not None:
                name.extend(path)
            name.append(field.name)
            if isinstance(field.type, RecordSchema):
                # Recursively flatten the schema if it's a record
                acc.update(flatten_avro_schema_fields(field.type, name))
            elif isinstance(field.type, UnionSchema):
                # Add the union schema to the accumulator itself
                acc[".".join(name)] = field
                # Look for any record schemas in the union
                for i, union_schema in enumerate(field.type.schemas):
                    if isinstance(union_schema, RecordSchema):
                        # If the union schema is a record, consider its evolution
                        union_name = [*name, "__union__", str(i)]
                        acc.update(flatten_avro_schema_fields(union_schema, union_name))
            else:
                # Otherwise, just add the field to the accumulator
                acc[".".join(name)] = field
    return acc


def validate_avro_schema_evolution(schema_a: Schema, schema_b: Schema):
    """
    Validates the evolution of two Avro schemas.

    Allowed operations:
    - Adding a new field with a default value.
    - Removing a field with a default value.
    - Adding or changing a default value on an existing field.
    - Making a field optional (i.e. union with null, default null).
    - Adding symbols to an enum type.
    - Adding a new type to a union type.

    Note: This is focused on *forward compatibility* i.e. that all data can be read using the most
    recent schema. New data may not be readable with the old schema, but old data can be safely
    up-converted to the new schema.

    See: https://docs.oracle.com/cd/E26161_02/html/GettingStartedGuide/schemaevolution.html

    :param a: The original schema.
    :param b: The evolved schema.
    :raises ValueError: If the schemas are not compatible.
    :return: True if the evolution is valid, False otherwise.
    """
    # Flatten the schemas with names in dot-notation
    schema_a_fields = flatten_avro_schema_fields(schema_a)
    schema_b_fields = flatten_avro_schema_fields(schema_b)
    # Check for new fields in schema_b
    for name, field in schema_b_fields.items():
        # If the field is not in schema_a, it must be a new field with a default value
        if name not in schema_a_fields:
            if "default" not in field.props:
                raise ValueError(f"Field {name} is missing a default value.")
            continue
        # If the field is in both schemas, check for changes
        old_field = schema_a_fields[name]
        # Default value cannot be removed, just changed
        if "default" not in field.props and "default" in old_field.props:
            raise ValueError(f"Field {name} default value cannot be removed.")
        if field.type != old_field.type:
            # If it is an enum, the new enum must be a superset of the old enum
            if isinstance(field.type, EnumSchema) and isinstance(
                old_field.type, EnumSchema
            ):
                old_enum = set(old_field.type.symbols)
                new_enum = set(field.type.symbols)
                if not old_enum.issubset(new_enum):
                    raise ValueError(
                        f"Field {name} enum has changed from {old_enum} to {new_enum}."
                    )
            # If the type is a union, it must be a superset of the old type
            elif isinstance(field.type, UnionSchema) and isinstance(
                old_field.type, UnionSchema
            ):
                old_union = set(old_field.type.schemas)
                new_union = set(field.type.schemas)
                if not old_union.issubset(new_union):
                    raise ValueError(
                        f"Field {name} union has changed from {old_union} to {new_union}."
                    )
            # If the type has changed, it must be a union with null
            elif not isinstance(field.type, UnionSchema):
                raise ValueError(
                    f"Field {name} type has changed from {old_field.type} to {field.type}."
                )
            elif len(field.type.schemas) != 2 or not any(
                isinstance(s, RecordSchema) and s.name == "null"
                for s in field.type.schemas
            ):
                raise ValueError(
                    f"Field {name} type has changed from {old_field.type} to {field.type}."
                )
    # Check for removed fields in schema_b
    for name, field in schema_a_fields.items():
        # If the field is not in schema_b, it must be a removed field with a default value
        if name not in schema_b_fields and "default" not in field.props:
            raise ValueError(f"Field {name} is missing a default value.")
