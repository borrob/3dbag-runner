import os
import shutil
import tempfile
from pathlib import Path

from roofhelper.io import SchemeFileHandler
from roofhelper.io.FileHandle import FileHandle


class TestSchemeFileHandlerPathComparison:
    """Test cases for SchemeFileHandler path comparison fixes."""

    def setup_method(self) -> None:
        """Set up test fixtures before each test method."""
        # Create a temporary directory for testing
        self.test_dir = tempfile.mkdtemp()
        self.scheme_handler = SchemeFileHandler(temporary_directory=Path(self.test_dir))

    def teardown_method(self) -> None:
        """Clean up test fixtures after each test method."""
        # Remove the temporary directory and all its contents
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_delete_if_not_local_absolute_vs_relative_paths(self) -> None:
        """
        Test that delete_if_not_local correctly handles absolute vs relative paths.

        This test covers the bug where path comparison failed when one path was
        relative and the other was absolute, even when they referred to the same file.
        """
        # Create a temporary file using the scheme handler
        temp_file_path = self.scheme_handler.create_file(suffix=".txt", text="test content")

        # Verify the file was created and is tracked
        assert temp_file_path.exists()
        assert len(self.scheme_handler.file_handles) == 1

        # Get the file handle that was created
        original_handle = self.scheme_handler.file_handles[0]
        assert original_handle.path == temp_file_path
        assert original_handle.must_dispose is True

        # Change to the directory containing the temp file to create a relative path scenario
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_file_path.parent)

            # Create a relative path that points to the same file
            relative_path = Path(temp_file_path.name)

            # Verify they're different path objects but refer to the same file
            assert relative_path != temp_file_path  # Different Path objects
            assert relative_path.resolve() == temp_file_path.resolve()  # Same resolved path
            assert relative_path.samefile(temp_file_path)  # Same actual file

            # Test deletion with relative path - this should work now
            self.scheme_handler.delete_if_not_local(relative_path)

            # Verify the file was deleted and handle was removed
            assert not temp_file_path.exists()
            assert len(self.scheme_handler.file_handles) == 0

        finally:
            os.chdir(original_cwd)

    def test_delete_if_not_local_same_absolute_paths(self) -> None:
        """Test deletion works with identical absolute paths."""
        temp_file_path = self.scheme_handler.create_file(suffix=".txt", text="test content")

        assert temp_file_path.exists()
        assert len(self.scheme_handler.file_handles) == 1

        # Delete using the exact same path
        self.scheme_handler.delete_if_not_local(temp_file_path)

        assert not temp_file_path.exists()
        assert len(self.scheme_handler.file_handles) == 0

    def test_delete_if_not_local_different_absolute_paths_same_file(self) -> None:
        """Test deletion works with different absolute path representations of the same file."""
        temp_file_path = self.scheme_handler.create_file(suffix=".txt", text="test content")

        assert temp_file_path.exists()
        assert len(self.scheme_handler.file_handles) == 1

        # Create an alternative absolute path with ".." that resolves to the same file
        parent_dir = temp_file_path.parent
        alternative_path = parent_dir / "subdir" / ".." / temp_file_path.name

        # Verify they resolve to the same file
        assert alternative_path.resolve() == temp_file_path.resolve()

        # Delete using the alternative path
        self.scheme_handler.delete_if_not_local(alternative_path)

        assert not temp_file_path.exists()
        assert len(self.scheme_handler.file_handles) == 0

    def test_delete_if_not_local_no_match(self) -> None:
        """Test that deletion doesn't affect unrelated files."""
        # Create two files
        temp_file1 = self.scheme_handler.create_file(suffix=".txt", text="content1")
        temp_file2 = self.scheme_handler.create_file(suffix=".txt", text="content2")

        assert len(self.scheme_handler.file_handles) == 2

        # Try to delete a non-existent file
        non_existent_path = Path(self.test_dir) / "non_existent.txt"
        self.scheme_handler.delete_if_not_local(non_existent_path)

        # Both files should still exist
        assert temp_file1.exists()
        assert temp_file2.exists()
        assert len(self.scheme_handler.file_handles) == 2

    def test_delete_if_not_local_must_dispose_false(self) -> None:
        """Test that files with must_dispose=False are not deleted."""
        # Manually create a file handle with must_dispose=False
        temp_file_path = Path(self.test_dir) / "manual_file.txt"
        temp_file_path.write_text("manual content")

        # Add a handle that shouldn't be disposed
        manual_handle = FileHandle(temp_file_path.resolve(), must_dispose=False)
        self.scheme_handler.file_handles.append(manual_handle)

        assert len(self.scheme_handler.file_handles) == 1
        assert temp_file_path.exists()

        # Try to delete - should not delete because must_dispose=False
        self.scheme_handler.delete_if_not_local(temp_file_path)

        # File should still exist and handle should still be tracked
        assert temp_file_path.exists()
        assert len(self.scheme_handler.file_handles) == 1

    def test_path_resolution_consistency(self) -> None:
        """Test that all created file handles use resolved (absolute) paths."""
        # Create files using different methods
        self.scheme_handler.create_text_file("test text", ".txt")
        self.scheme_handler.create_file(".log", "log content")

        # All file handles should have resolved paths
        for handle in self.scheme_handler.file_handles:
            assert handle.path.is_absolute()
            assert handle.path == handle.path.resolve()
