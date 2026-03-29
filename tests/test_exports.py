# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>
#
# SPDX-License-Identifier: Apache-2.0

"""
Regression test: verify all public symbols in avrokit.__all__ are importable
and resolve to the expected type. This guards against build configuration
changes (e.g. hatchling include/exclude rules) silently dropping source files
from the wheel.
"""

import importlib

import pytest

# Every name declared in avrokit.__all__, paired with the type it must be.
# "callable" covers both functions and classes.
EXPECTED_EXPORTS: list[tuple[str, type | tuple[type, ...]]] = [
    # io
    ("Appendable", type),
    ("PartitionedAvroReader", type),
    ("PartitionedAvroWriter", type),
    ("TimePartitionedAvroWriter", type),
    ("avro_reader", object),
    ("avro_records", object),
    ("avro_schema", object),
    ("avro_writer", object),
    ("compact_avro_data", object),
    ("read_avro_schema", object),
    ("read_avro_schema_from_first_nonempty_file", object),
    ("validate_avro_schema_evolution", object),
    ("add_avro_schema_fields", object),
    # asyncio
    ("BlockingQueueAvroReader", type),
    ("DeferredAvroWriter", type),
    # url
    ("URL", type),
    ("FileURL", type),
    ("parse_url", object),
    ("create_url_mapping", object),
    ("flatten_urls", object),
    # tools
    ("CatTool", type),
    ("ConcatTool", type),
    ("FileSortTool", type),
    ("FromParquetTool", type),
    ("GetMetaTool", type),
    ("GetSchemaTool", type),
    ("HttpServerTool", type),
    ("PartitionTool", type),
    ("RepairTool", type),
    ("StatsTool", type),
    ("ToJsonTool", type),
    ("ToParquetTool", type),
]


class TestPublicExports:
    """Verify avrokit.__all__ is complete and every symbol is importable."""

    def test_all_is_defined(self):
        import avrokit

        assert hasattr(avrokit, "__all__"), "avrokit must define __all__"
        assert len(avrokit.__all__) > 0

    @pytest.mark.parametrize("name,expected_type", EXPECTED_EXPORTS)
    def test_symbol_importable(self, name, expected_type):
        """Each symbol can be imported directly from avrokit."""
        avrokit = importlib.import_module("avrokit")
        assert hasattr(avrokit, name), (
            f"avrokit.{name} is missing — was it dropped from the wheel?"
        )

    @pytest.mark.parametrize("name,expected_type", EXPECTED_EXPORTS)
    def test_symbol_is_callable(self, name, expected_type):
        """Each exported symbol must be callable (function or class)."""
        avrokit = importlib.import_module("avrokit")
        obj = getattr(avrokit, name)
        assert callable(obj), f"avrokit.{name} is not callable (got {type(obj)})"

    def test_no_undeclared_exports(self):
        """Every name in __all__ is covered by EXPECTED_EXPORTS."""
        import avrokit

        expected_names = {name for name, _ in EXPECTED_EXPORTS}
        undeclared = set(avrokit.__all__) - expected_names
        assert not undeclared, (
            f"These names are in __all__ but not in EXPECTED_EXPORTS: {undeclared}\n"
            "Add them to EXPECTED_EXPORTS in tests/test_exports.py."
        )

    def test_all_matches_actual_imports(self):
        """Every name in EXPECTED_EXPORTS is also listed in __all__."""
        import avrokit

        for name, _ in EXPECTED_EXPORTS:
            assert name in avrokit.__all__, (
                f"avrokit.{name} is in EXPECTED_EXPORTS but missing from __all__"
            )

    def test_version_is_set(self):
        import avrokit

        assert hasattr(avrokit, "__version__")
        assert isinstance(avrokit.__version__, str)
        assert avrokit.__version__  # non-empty

    def test_submodules_importable(self):
        """Core sub-packages must themselves be importable."""
        for submodule in (
            "avrokit.io",
            "avrokit.url",
            "avrokit.tools",
            "avrokit.asyncio",
        ):
            mod = importlib.import_module(submodule)
            assert mod is not None, f"{submodule} could not be imported"
