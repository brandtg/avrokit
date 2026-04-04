# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

# SPDX-License-Identifier: Apache-2.0

import pytest
from avro.schema import RecordSchema

from avrokit.io import avro_schema
from avrokit.io.schema import add_avro_schema_fields, validate_avro_schema_evolution


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
                        # Added a new symbol 'PENDING'
                        "type": {
                            "type": "enum",
                            "name": "Status",
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
                    {"name": "status", "type": ["null", "string"]},
                ],
            },
            {
                "name": "Record",
                "type": "record",
                "fields": [
                    # Added a new type 'int' to the union
                    {"name": "status", "type": ["null", "string", "int"]},
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
                        # Removed the symbol 'PENDING'
                        "type": {
                            "type": "enum",
                            "name": "Status",
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
                    {"name": "status", "type": ["null", "string", "int"]},
                ],
            },
            {
                "name": "Record",
                "type": "record",
                "fields": [
                    # Removed the type 'int' from the union
                    {"name": "status", "type": ["null", "string"]},
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


class TestSchemaEvolutionEdgeCases:
    def test_enum_symbol_removal_raises_error(self):
        """Test that narrowing enum symbols raises ValueError."""
        schema_a = {
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
                },
            ],
        }
        schema_b = {
            "name": "Record",
            "type": "record",
            "fields": [
                {
                    "name": "status",
                    "type": {"type": "enum", "name": "Status", "symbols": ["ACTIVE"]},
                },
            ],
        }

        with pytest.raises(ValueError, match="Field status enum has changed"):
            validate_avro_schema_evolution(avro_schema(schema_a), avro_schema(schema_b))

    def test_union_type_removal_raises_error(self):
        """Test that removing types from union raises ValueError."""
        schema_a = {
            "name": "Record",
            "type": "record",
            "fields": [{"name": "status", "type": ["null", "string", "int"]}],
        }
        schema_b = {
            "name": "Record",
            "type": "record",
            "fields": [{"name": "status", "type": ["null", "string"]}],
        }

        with pytest.raises(ValueError, match="Field status union has changed"):
            validate_avro_schema_evolution(avro_schema(schema_a), avro_schema(schema_b))

    def test_deeply_nested_record_addition(self):
        """Test schema evolution through 3+ levels of nested records."""
        schema_a = {
            "name": "Record",
            "type": "record",
            "fields": [
                {"name": "user_id", "type": "int"},
                {
                    "name": "profile",
                    "type": {
                        "name": "Profile",
                        "type": "record",
                        "fields": [
                            {"name": "email", "type": "string"},
                            {
                                "name": "address",
                                "type": {
                                    "name": "Address",
                                    "type": "record",
                                    "fields": [{"name": "city", "type": "string"}],
                                },
                            },
                        ],
                    },
                },
            ],
        }

        schema_b = {
            "name": "Record",
            "type": "record",
            "fields": [
                {"name": "user_id", "type": "int"},
                {
                    "name": "profile",
                    "type": {
                        "name": "Profile",
                        "type": "record",
                        "fields": [
                            {"name": "email", "type": "string"},
                            {
                                "name": "address",
                                "type": {
                                    "name": "Address",
                                    "type": "record",
                                    "fields": [{"name": "city", "type": "string"}],
                                },
                            },
                            {
                                "name": "country",
                                "type": ["null", "string"],
                                "default": None,
                            },
                        ],
                    },
                },
            ],
        }

        validate_avro_schema_evolution(avro_schema(schema_a), avro_schema(schema_b))

    def test_deeply_nested_record_field_removal(self):
        """Test that removing deeply nested field without default raises error."""
        schema_a = {
            "name": "Record",
            "type": "record",
            "fields": [
                {"name": "user_id", "type": "int"},
                {
                    "name": "profile",
                    "type": {
                        "name": "Profile",
                        "type": "record",
                        "fields": [{"name": "email", "type": "string"}],
                    },
                },
            ],
        }

        schema_b = {
            "name": "Record",
            "type": "record",
            "fields": [
                {"name": "user_id", "type": "int"},
                {
                    "name": "profile",
                    "type": {
                        "name": "Profile",
                        "type": "record",
                        "fields": [],
                    },
                },
            ],
        }

        with pytest.raises(
            ValueError, match="Field profile.email is missing a default value"
        ):
            validate_avro_schema_evolution(avro_schema(schema_a), avro_schema(schema_b))

    def test_mixed_union_complex_type_changes(self):
        """Test evolution of unions containing records and enums."""
        schema_a = {
            "name": "Record",
            "type": "record",
            "fields": [
                {
                    "name": "value",
                    "type": [
                        "null",
                        {"type": "enum", "name": "Color", "symbols": ["RED", "GREEN"]},
                    ],
                },
            ],
        }

        schema_b = {
            "name": "Record",
            "type": "record",
            "fields": [
                {
                    "name": "value",
                    "type": [
                        "null",
                        {"type": "enum", "name": "Color", "symbols": ["RED"]},
                    ],
                },
            ],
        }

        with pytest.raises(ValueError, match="Field value union has changed"):
            validate_avro_schema_evolution(avro_schema(schema_a), avro_schema(schema_b))

    def test_field_type_change_to_non_union_raises_error(self):
        """Test that changing field type to non-union raises error."""
        schema_a = {
            "name": "Record",
            "type": "record",
            "fields": [{"name": "count", "type": "int"}],
        }
        schema_b = {
            "name": "Record",
            "type": "record",
            "fields": [{"name": "count", "type": "string"}],
        }

        with pytest.raises(ValueError, match="Field count type has changed"):
            validate_avro_schema_evolution(avro_schema(schema_a), avro_schema(schema_b))

    def test_field_type_change_to_union_without_null_raises_error(self):
        """Test that changing to union without null raises error."""
        schema_a = {
            "name": "Record",
            "type": "record",
            "fields": [{"name": "count", "type": "int"}],
        }
        schema_b = {
            "name": "Record",
            "type": "record",
            "fields": [{"name": "count", "type": ["string", "long"]}],
        }

        with pytest.raises(ValueError, match="Field count type has changed"):
            validate_avro_schema_evolution(avro_schema(schema_a), avro_schema(schema_b))
