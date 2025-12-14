# CI/CD Setup

This project uses GitHub Actions for continuous integration and deployment.

## Workflows

### CI Workflow (`.github/workflows/ci.yml`)

Runs on every push to `main` or `develop` branches and on all pull requests.

**Jobs:**

1. **Lint** - Checks code style and quality using `ruff`
   - Runs `ruff check` to find linting issues
   - Runs `ruff format --check` to verify code formatting

2. **Test** - Runs the test suite across multiple Python versions
   - Tests on Python 3.10, 3.11, and 3.12
   - Runs `pytest` with coverage reporting
   - Uploads coverage to Codecov (Python 3.12 only)

3. **Build** - Builds the package distribution
   - Creates source distribution and wheel
   - Validates the package with `twine check`
   - Uploads build artifacts

### Documentation Workflow (`.github/workflows/docs.yml`)

Builds the documentation using MkDocs and Material theme.

**Jobs:**

1. **Build** - Builds the documentation site
   - Installs `mkdocs` and `mkdocs-material`
   - Runs `mkdocs build` to generate static site
   - Uploads documentation artifacts on main branch pushes

### Publish Workflow (`.github/workflows/publish.yml`)

Publishes the package to PyPI.

**Triggers:**

1. **Automatic** - When a GitHub release is published
2. **Manual** - Via workflow dispatch with choice of:
   - TestPyPI (for testing)
   - PyPI (for production)

**Features:**

- Uses trusted publishing (no API tokens needed)
- Requires `id-token: write` permission
- Automatically builds and uploads package

## Setting Up PyPI Publishing

To enable automatic PyPI publishing:

1. **Configure Trusted Publishing on PyPI:**
   - Go to https://pypi.org/manage/account/publishing/
   - Add a new publisher with:
     - Repository: `teaguesterling/lq`
     - Workflow: `publish.yml`
     - Environment name: (leave blank)

2. **Configure Trusted Publishing on TestPyPI (optional):**
   - Go to https://test.pypi.org/manage/account/publishing/
   - Add the same configuration as above

3. **Create a Release:**
   - Tag your commit with a version (e.g., `v0.1.0`)
   - Create a GitHub release from that tag
   - The workflow will automatically publish to PyPI

## Setting Up ReadTheDocs

The documentation is configured for ReadTheDocs hosting:

1. **Import Project:**
   - Go to https://readthedocs.org/dashboard/import/
   - Import the `teaguesterling/lq` repository

2. **Configuration:**
   - The project uses `.readthedocs.yml` for configuration
   - MkDocs will automatically build the documentation
   - Documentation will be available at `lq.readthedocs.io`

## Local Development

### Running Linting

```bash
# Check for linting issues
ruff check .

# Auto-fix linting issues
ruff check --fix .

# Check formatting
ruff format --check .

# Auto-format code
ruff format .
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=lq

# Run specific test file
pytest tests/test_core.py

# Run with verbose output
pytest -v
```

### Building Package

```bash
# Install build tools
pip install build

# Build distribution
python -m build

# Check package
pip install twine
twine check dist/*
```

### Building Documentation

```bash
# Install documentation tools
pip install -e ".[docs]"

# Build documentation
mkdocs build

# Serve documentation locally
mkdocs serve
# Visit http://127.0.0.1:8000
```

## Codecov Integration

The CI workflow uploads coverage reports to Codecov. To enable:

1. Go to https://codecov.io/
2. Sign in with GitHub
3. Enable the repository
4. Coverage reports will appear automatically on PRs

No configuration or tokens needed - Codecov detects GitHub Actions automatically.

## Status Badges

Add these badges to your README.md:

```markdown
[![CI](https://github.com/teaguesterling/lq/workflows/CI/badge.svg)](https://github.com/teaguesterling/lq/actions/workflows/ci.yml)
[![Documentation](https://github.com/teaguesterling/lq/workflows/Documentation/badge.svg)](https://github.com/teaguesterling/lq/actions/workflows/docs.yml)
[![PyPI](https://img.shields.io/pypi/v/lq)](https://pypi.org/project/lq/)
[![Python Version](https://img.shields.io/pypi/pyversions/lq)](https://pypi.org/project/lq/)
[![codecov](https://codecov.io/gh/teaguesterling/lq/branch/main/graph/badge.svg)](https://codecov.io/gh/teaguesterling/lq)
```

## Troubleshooting

### Lint Failures

If the lint job fails:

1. Run `ruff check --fix .` locally
2. Run `ruff format .` locally
3. Commit the fixes

### Test Failures

If tests fail in CI but pass locally:

1. Check the Python version (CI tests multiple versions)
2. Look at the full test output in GitHub Actions
3. Ensure all dependencies are in `pyproject.toml`

### Documentation Build Failures

If documentation fails to build:

1. Run `mkdocs build` locally
2. Check for broken links or missing files
3. Verify all referenced files exist in `docs/`

### PyPI Publishing Failures

If publishing fails:

1. Verify trusted publishing is configured on PyPI
2. Check that the workflow has `id-token: write` permission
3. Ensure the version in `pyproject.toml` is unique
4. Test with TestPyPI first using manual workflow dispatch
