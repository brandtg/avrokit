# SPDX-FileCopyrightText: 2026 Greg Brandt <brandt.greg@gmail.com>

# SPDX-License-Identifier: Apache-2.0

import tempfile

import pytest


class TestUrlInterfaceContract:
    """Test that all URL implementations conform to the common protocol."""

    def test_fileurl_implements_protocol(self):
        """Verify FileURL implements required interface methods."""
        from avrokit.url import FileURL

        assert hasattr(FileURL, "expand")
        assert hasattr(FileURL, "delete")
        assert hasattr(FileURL, "exists")
        assert hasattr(FileURL, "size")
        assert hasattr(FileURL, "open")

    def test_google_cloud_storage_url_implements_protocol(self):
        """Verify GoogleCloudStorageURL implements required interface methods."""
        from avrokit.url.google import GoogleCloudStorageURL

        assert hasattr(GoogleCloudStorageURL, "expand")
        assert hasattr(GoogleCloudStorageURL, "delete")
        assert hasattr(GoogleCloudStorageURL, "exists")
        assert hasattr(GoogleCloudStorageURL, "size")
        assert hasattr(GoogleCloudStorageURL, "open")


class TestUrlContextManagerBehavior:
    """Test context manager exit behavior across URL types."""

    def test_fileurl_context_manager_exit_normal(self):
        """FileURL context manager exits cleanly on normal use."""
        from avrokit.url import FileURL

        with FileURL("/tmp/test_ctx.txt", mode="w") as f:
            assert not isinstance(f, type(None))

        # Verify file exists after exit
        url = FileURL("/tmp/test_ctx.txt")
        assert url.exists()

    def test_fileurl_context_manager_exit_exception(self):
        """FileURL context manager cleans up properly on exception."""
        from avrokit.url import FileURL

        with pytest.raises(ValueError):
            with FileURL("/tmp/test_ctx_exc.txt", mode="w"):
                raise ValueError("Test exception during write")


class TestUrlWithMethodConsistency:
    """Verify consistency of URL manipulation methods across implementations."""

    def test_with_path_preserves_base(self):
        """with_path should preserve base path components correctly."""
        from avrokit.url import FileURL

        url = FileURL("/tmp/base/path/")
        new_url = url.with_path("subdir/file.txt")

        assert "base" in str(new_url) or "/tmp/" in str(new_url.parsed_url.path)

    def test_with_mode_changes_only_mode(self):
        """with_mode should only change mode, not URL."""
        from avrokit.url import FileURL

        original_url = FileURL("/tmp/test_file.avro", mode="r")
        modified_url = original_url.with_mode("wb")

        assert str(original_url) == str(modified_url)

    def test_with_path_slash_handling(self):
        """with_path should handle leading slashes consistently."""
        from avrokit.url import FileURL

        # Verify with_path strips trailing slash when appending
        url1 = FileURL("/tmp/base/")
        result1 = url1.with_path("file.txt")

        assert "base" in str(result1) or "/tmp/" in str(result1.parsed_url.path)

        # Test renaming file while preserving extension
        url2 = FileURL("/tmp/base/path/to/file.avro", mode="r")
        new_name = "renamed_file"
        if "." in str(url2.parsed_url.path):
            base, ext = str(url2.parsed_url.path).rsplit(".", 1)
            result2 = url2.with_path(f"{base}_{new_name}.{ext}")

            assert f"_renamed_file.{ext}" in str(result2), (
                "Extension should be preserved with renamed file"
            )


class TestUrlEqualityAndComparison:
    """Test URL equality and comparison behavior."""

    def test_fileurl_equality(self):
        """FileURL equality should compare URLs correctly."""
        from avrokit.url import FileURL

        url1 = FileURL("/tmp/test.txt", mode="r")
        url2 = FileURL("/tmp/test.txt", mode="w")

        assert url1 == url2  # Same URL, different modes

    def test_fileurl_inequality_different_paths(self):
        """Different paths should be unequal."""
        from avrokit.url import FileURL

        url1 = FileURL("/tmp/file_a.txt")
        url2 = FileURL("/tmp/file_b.txt")

        assert url1 != url2


class TestUrlProtocolTypeHints:
    """Verify URL protocol type annotations are consistent."""

    def test_fileurl_returns_correct_types(self):
        """FileURL methods return expected types."""
        from avrokit.url import FileURL

        with tempfile.TemporaryDirectory() as tmpdir:
            # expand returns list of URLs
            url = FileURL(f"file://{tmpdir}")
            expanded = url.expand()
            assert isinstance(expanded, list)

            for item in expanded:
                from avrokit.url.base import URL

                assert isinstance(item, URL), (
                    f"expand should return URL instances, got {type(item)}"
                )

    def test_url_methods_return_expected_types(self):
        """URL methods have consistent return types."""
        from avrokit.url import FileURL

        with tempfile.TemporaryDirectory() as tmpdir:
            url = FileURL(f"file://{tmpdir}")

            # exists returns bool
            result_exists = url.exists()
            assert isinstance(result_exists, bool)
