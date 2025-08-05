# Testing Documentation

This directory contains comprehensive test cases for the 3dbag-runner project.

## Running Tests

To run all tests:
```bash
uv run pytest
```

To run tests with verbose output:
```bash
uv run pytest -v
```

To run a specific test file:
```bash
uv run pytest tests/test_translate_cityjson.py -v
```

## Dependencies

Tests require pytest, which is included in the project dependencies.
