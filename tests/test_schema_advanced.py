# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

"""
Comprehensive tests for Avro schema operations including evolution validation.
"""

import pytest
from avrokit.io.schema import (
    avro_schema,
    add_avro_schema_fields,
    flatten_avro_schema_fields,
    validate_avro_schema_evolution,
)


class TestSchemaEvolutionValidation:
    """Tests for schema evolution validation."""

    def test_add_field_with_default(self):
        """Adding a field with default should be valid."""
        schema_v1 = avro_schema(
            {
                "type": "record",
                "name": "User",
                "fields": [{"name": "id", "type": "int"}],
            }
        )
        schema_v2 = avro_schema(
            {
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "email", "type": "string", "default": ""},
                ],
            }
        )
        # Should not raise
        validate_avro_schema_evolution(schema_v1, schema_v2)

    def test_add_field_without_default_should_fail(self):
        """Adding a field without default should fail."""
        schema_v1 = avro_schema(
            {
                "type": "record",
                "name": "User",
                "fields": [{"name": "id", "type": "int"}],
            }
        )
        schema_v2 = avro_schema(
            {
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "email", "type": "string"},
                ],
            }
        )
        with pytest.raises(ValueError, match="missing a default value"):
            validate_avro_schema_evolution(schema_v1, schema_v2)

    def test_remove_field_with_default(self):
        """Removing a field with default should be valid."""
        schema_v1 = avro_schema(
            {
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "email", "type": "string", "default": ""},
                ],
            }
        )
        schema_v2 = avro_schema(
            {
                "type": "record",
                "name": "User",
                "fields": [{"name": "id", "type": "int"}],
            }
        )
        # Should not raise
        validate_avro_schema_evolution(schema_v1, schema_v2)

    def test_remove_field_without_default_should_fail(self):
        """Removing a field without default should fail."""
        schema_v1 = avro_schema(
            {
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "email", "type": "string"},
                ],
            }
        )
        schema_v2 = avro_schema(
            {
                "type": "record",
                "name": "User",
                "fields": [{"name": "id", "type": "int"}],
            }
        )
        with pytest.raises(ValueError, match="missing a default value"):
            validate_avro_schema_evolution(schema_v1, schema_v2)

    def test_change_default_value(self):
        """Changing default value should be valid."""
        schema_v1 = avro_schema(
            {
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "status", "type": "string", "default": "active"},
                ],
            }
        )
        schema_v2 = avro_schema(
            {
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "status", "type": "string", "default": "pending"},
                ],
            }
        )
        # Should not raise
        validate_avro_schema_evolution(schema_v1, schema_v2)

    def test_add_default_to_existing_field(self):
        """Adding default to existing field should be valid."""
        schema_v1 = avro_schema(
            {
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "email", "type": "string"},
                ],
            }
        )
        schema_v2 = avro_schema(
            {
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "email", "type": "string", "default": ""},
                ],
            }
        )
        # Should not raise
        validate_avro_schema_evolution(schema_v1, schema_v2)

    def test_remove_default_should_fail(self):
        """Removing default from field should fail."""
        schema_v1 = avro_schema(
            {
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "email", "type": "string", "default": ""},
                ],
            }
        )
        schema_v2 = avro_schema(
            {
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "email", "type": "string"},
                ],
            }
        )
        with pytest.raises(ValueError, match="default value cannot be removed"):
            validate_avro_schema_evolution(schema_v1, schema_v2)

    def test_nested_record_add_field_with_default(self):
        """Adding field to nested record with default should be valid."""
        schema_v1 = avro_schema(
            {
                "type": "record",
                "name": "User",
                "fields": [
                    {
                        "name": "address",
                        "type": {
                            "type": "record",
                            "name": "Address",
                            "fields": [{"name": "city", "type": "string"}],
                        },
                    }
                ],
            }
        )
        schema_v2 = avro_schema(
            {
                "type": "record",
                "name": "User",
                "fields": [
                    {
                        "name": "address",
                        "type": {
                            "type": "record",
                            "name": "Address",
                            "fields": [
                                {"name": "city", "type": "string"},
                                {"name": "zipcode", "type": "string", "default": ""},
                            ],
                        },
                    }
                ],
            }
        )
        # Should not raise
        validate_avro_schema_evolution(schema_v1, schema_v2)

    def test_union_add_type(self):
        """Adding type to union should be valid."""
        schema_v1 = avro_schema(
            {
                "type": "record",
                "name": "User",
                "fields": [{"name": "value", "type": ["null", "int"]}],
            }
        )
        schema_v2 = avro_schema(
            {
                "type": "record",
                "name": "User",
                "fields": [{"name": "value", "type": ["null", "int", "string"]}],
            }
        )
        # Should not raise
        validate_avro_schema_evolution(schema_v1, schema_v2)


class TestSchemaFlattening:
    """Tests for schema flattening functionality."""

    def test_flatten_simple_schema(self):
        """Test flattening a simple schema."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"},
                ],
            }
        )
        flat = flatten_avro_schema_fields(schema)
        assert "id" in flat
        assert "name" in flat
        assert len(flat) == 2

    def test_flatten_nested_schema(self):
        """Test flattening a nested schema."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "id", "type": "int"},
                    {
                        "name": "address",
                        "type": {
                            "type": "record",
                            "name": "Address",
                            "fields": [
                                {"name": "city", "type": "string"},
                                {"name": "zipcode", "type": "string"},
                            ],
                        },
                    },
                ],
            }
        )
        flat = flatten_avro_schema_fields(schema)
        assert "id" in flat
        assert "address.city" in flat
        assert "address.zipcode" in flat
        assert len(flat) == 3

    def test_flatten_deeply_nested_schema(self):
        """Test flattening a deeply nested schema."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Level1",
                "fields": [
                    {
                        "name": "level2",
                        "type": {
                            "type": "record",
                            "name": "Level2",
                            "fields": [
                                {
                                    "name": "level3",
                                    "type": {
                                        "type": "record",
                                        "name": "Level3",
                                        "fields": [{"name": "value", "type": "int"}],
                                    },
                                }
                            ],
                        },
                    }
                ],
            }
        )
        flat = flatten_avro_schema_fields(schema)
        assert "level2.level3.value" in flat

    def test_flatten_with_union(self):
        """Test flattening schema with union types."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "optional_name", "type": ["null", "string"]},
                ],
            }
        )
        flat = flatten_avro_schema_fields(schema)
        assert "id" in flat
        assert "optional_name" in flat

    def test_flatten_with_union_of_records(self):
        """Test flattening schema with union containing records."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Event",
                "fields": [
                    {
                        "name": "data",
                        "type": [
                            "null",
                            {
                                "type": "record",
                                "name": "DataA",
                                "fields": [{"name": "field_a", "type": "string"}],
                            },
                            {
                                "type": "record",
                                "name": "DataB",
                                "fields": [{"name": "field_b", "type": "int"}],
                            },
                        ],
                    }
                ],
            }
        )
        flat = flatten_avro_schema_fields(schema)
        assert "data" in flat
        # Should have union variants
        assert any("data.__union__" in key for key in flat.keys())


class TestAddSchemaFields:
    """Tests for adding fields to schemas."""

    def test_add_single_field(self):
        """Test adding a single field to schema."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "User",
                "fields": [{"name": "id", "type": "int"}],
            }
        )
        new_schema = add_avro_schema_fields(
            schema, [{"name": "email", "type": "string", "default": ""}]
        )

        # Verify the new field was added
        schema_dict = new_schema.to_json()
        assert len(schema_dict["fields"]) == 2
        assert schema_dict["fields"][1]["name"] == "email"

    def test_add_multiple_fields(self):
        """Test adding multiple fields to schema."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "User",
                "fields": [{"name": "id", "type": "int"}],
            }
        )
        new_schema = add_avro_schema_fields(
            schema,
            [
                {"name": "email", "type": "string", "default": ""},
                {"name": "age", "type": "int", "default": 0},
            ],
        )

        schema_dict = new_schema.to_json()
        assert len(schema_dict["fields"]) == 3


class TestSchemaEdgeCases:
    """Tests for edge cases in schema handling."""

    def test_empty_record_schema(self):
        """Test schema with no fields."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Empty",
                "fields": [],
            }
        )
        flat = flatten_avro_schema_fields(schema)
        assert len(flat) == 0

    def test_schema_with_enum(self):
        """Test schema with enum type."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "User",
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
            }
        )
        flat = flatten_avro_schema_fields(schema)
        assert "status" in flat

    def test_schema_with_fixed(self):
        """Test schema with fixed type."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "User",
                "fields": [
                    {
                        "name": "hash",
                        "type": {"type": "fixed", "name": "MD5", "size": 16},
                    }
                ],
            }
        )
        flat = flatten_avro_schema_fields(schema)
        assert "hash" in flat

    def test_complex_union_types(self):
        """Test schema with complex union types."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Message",
                "fields": [
                    {
                        "name": "payload",
                        "type": [
                            "null",
                            "int",
                            "string",
                            "boolean",
                            {"type": "array", "items": "string"},
                        ],
                    }
                ],
            }
        )
        flat = flatten_avro_schema_fields(schema)
        assert "payload" in flat

    def test_recursive_schema_reference(self):
        """Test schema with recursive references."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Node",
                "fields": [
                    {"name": "value", "type": "int"},
                    {
                        "name": "children",
                        "type": {"type": "array", "items": "Node"},
                        "default": [],
                    },
                ],
            }
        )
        flat = flatten_avro_schema_fields(schema)
        assert "value" in flat
        assert "children" in flat

    def test_schema_with_all_primitive_types(self):
        """Test schema with all primitive types."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "AllTypes",
                "fields": [
                    {"name": "null_field", "type": "null"},
                    {"name": "boolean_field", "type": "boolean"},
                    {"name": "int_field", "type": "int"},
                    {"name": "long_field", "type": "long"},
                    {"name": "float_field", "type": "float"},
                    {"name": "double_field", "type": "double"},
                    {"name": "bytes_field", "type": "bytes"},
                    {"name": "string_field", "type": "string"},
                ],
            }
        )
        flat = flatten_avro_schema_fields(schema)
        assert len(flat) == 8
        assert "null_field" in flat
        assert "boolean_field" in flat
        assert "int_field" in flat
        assert "long_field" in flat
        assert "float_field" in flat
        assert "double_field" in flat
        assert "bytes_field" in flat
        assert "string_field" in flat

    def test_schema_with_nested_arrays(self):
        """Test schema with nested array types."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "Matrix",
                "fields": [
                    {
                        "name": "data",
                        "type": {
                            "type": "array",
                            "items": {"type": "array", "items": "int"},
                        },
                    }
                ],
            }
        )
        flat = flatten_avro_schema_fields(schema)
        assert "data" in flat

    def test_schema_with_nested_maps(self):
        """Test schema with nested map types."""
        schema = avro_schema(
            {
                "type": "record",
                "name": "NestedMaps",
                "fields": [
                    {
                        "name": "data",
                        "type": {
                            "type": "map",
                            "values": {"type": "map", "values": "string"},
                        },
                    }
                ],
            }
        )
        flat = flatten_avro_schema_fields(schema)
        assert "data" in flat
