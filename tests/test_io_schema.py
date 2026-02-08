# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

import pytest
from avro.schema import RecordSchema
from avrokit.io import avro_schema
from avrokit.io.schema import validate_avro_schema_evolution, add_avro_schema_fields


@pytest.mark.parametrize(
    "schema_a,schema_b",
    [
        # Adding a new field with a default value
        (
            {
                "name": "Record",
                "type": "record",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"},
                    {
                        "name": "address",
                        "type": {
                            "name": "Address",
                            "type": "record",
                            "fields": [
                                {"name": "street", "type": "string"},
                                {"name": "city", "type": "string"},
                                {"name": "state", "type": "string"},
                                {"name": "zip", "type": "string"},
                            ],
                        },
                    },
                ],
            },
            {
                "name": "Record",
                "type": "record",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"},
                    {
                        "name": "address",
                        "type": {
                            "name": "Address",
                            "type": "record",
                            "fields": [
                                {"name": "street", "type": "string"},
                                {"name": "city", "type": "string"},
                                {"name": "state", "type": "string"},
                                {"name": "zip", "type": "string"},
                                # Also add an optional field in the nested record
                                {
                                    "name": "country",
                                    "type": ["null", "string"],
                                    "default": None,
                                },
                            ],
                        },
                    },
                    # A new field with a default value
                    {"name": "active", "type": "boolean", "default": True},
                ],
            },
        ),
        # Removing a field with a default value
        (
            {
                "name": "Record",
                "type": "record",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"},
                    {"name": "active", "type": "boolean", "default": True},
                ],
            },
            {
                "name": "Record",
                "type": "record",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"},
                ],
            },
        ),
        # Adding a symbol to an enum
        (
            {
                "name": "Record",
                "type": "record",
                "fields": [
                    {
                        "name": "status",
                        "type": {
                            "type": "enum",
                            "name": "Status",
                            "symbols": ["ACTIVE", "INACTIVE"],
                        },
                    }
                ],
            },
            {
                "name": "Record",
                "type": "record",
                "fields": [
                    {
                        "name": "status",
                        "type": {
                            "type": "enum",
                            "name": "Status",
                            # Added a new symbol 'PENDING'
                            "symbols": ["ACTIVE", "INACTIVE", "PENDING"],
                        },
                    }
                ],
            },
        ),
        # Adding a new type to a union type
        (
            {
                "name": "Record",
                "type": "record",
                "fields": [
                    {
                        "name": "status",
                        "type": ["null", "string"],
                    }
                ],
            },
            {
                "name": "Record",
                "type": "record",
                "fields": [
                    {
                        "name": "status",
                        # Added a new type 'int' to the union
                        "type": ["null", "string", "int"],
                    }
                ],
            },
        ),
        # Change a default value on an existing field
        (
            {
                "name": "Record",
                "type": "record",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int", "default": 0},
                ],
            },
            {
                "name": "Record",
                "type": "record",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int", "default": 1},
                ],
            },
        ),
    ],
)
def test_validate_avro_schema_evolution_valid(schema_a: dict, schema_b: dict):
    validate_avro_schema_evolution(avro_schema(schema_a), avro_schema(schema_b))


@pytest.mark.parametrize(
    "schema_a,schema_b",
    [
        # Remove a field without a default value
        (
            {
                "name": "Record",
                "type": "record",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"},
                    {
                        "name": "address",
                        "type": {
                            "name": "Address",
                            "type": "record",
                            "fields": [
                                {"name": "street", "type": "string"},
                                {"name": "city", "type": "string"},
                                {"name": "state", "type": "string"},
                                {"name": "zip", "type": "string"},
                            ],
                        },
                    },
                ],
            },
            {
                "name": "Record",
                "type": "record",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"},
                ],
            },
        ),
        # Remove a symbol from an enum
        (
            {
                "name": "Record",
                "type": "record",
                "fields": [
                    {
                        "name": "status",
                        "type": {
                            "type": "enum",
                            "name": "Status",
                            "symbols": ["ACTIVE", "INACTIVE", "PENDING"],
                        },
                    }
                ],
            },
            {
                "name": "Record",
                "type": "record",
                "fields": [
                    {
                        "name": "status",
                        "type": {
                            "type": "enum",
                            "name": "Status",
                            # Removed the symbol 'PENDING'
                            "symbols": ["ACTIVE", "INACTIVE"],
                        },
                    }
                ],
            },
        ),
        # Remove a type from a union type
        (
            {
                "name": "Record",
                "type": "record",
                "fields": [
                    {
                        "name": "status",
                        "type": ["null", "string", "int"],
                    }
                ],
            },
            {
                "name": "Record",
                "type": "record",
                "fields": [
                    {
                        "name": "status",
                        # Removed the type 'int' from the union
                        "type": ["null", "string"],
                    }
                ],
            },
        ),
        # Remove a default value from an existing field
        (
            {
                "name": "Record",
                "type": "record",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int", "default": 0},
                ],
            },
            {
                "name": "Record",
                "type": "record",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"},
                ],
            },
        ),
    ],
)
def test_validate_avro_schema_evolution_invalid(schema_a: dict, schema_b: dict):
    with pytest.raises(ValueError):
        validate_avro_schema_evolution(avro_schema(schema_a), avro_schema(schema_b))


def test_add_avro_schema_fields():
    schema = avro_schema(
        {
            "name": "Record",
            "type": "record",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"},
            ],
        }
    )
    fields = [
        {"name": "address", "type": "string"},
        {"name": "phone", "type": "string"},
    ]
    new_schema = add_avro_schema_fields(schema, fields)
    if not isinstance(new_schema, RecordSchema):
        raise ValueError("Schema is not a valid Avro record schema.")
    new_schema_dict = new_schema.to_json()
    if not isinstance(new_schema_dict, dict):
        raise ValueError("Schema is not a valid Avro record schema.")
    assert new_schema_dict["fields"] == [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"},
        {"name": "address", "type": "string"},
        {"name": "phone", "type": "string"},
    ]
