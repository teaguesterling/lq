# Contributing to lq

Thank you for considering contributing to lq! This document provides guidelines and instructions for contributing.

## Development Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/teaguesterling/lq.git
   cd lq
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install in development mode:**
   ```bash
   pip install -e ".[dev]"
   ```

4. **Initialize lq:**
   ```bash
   lq init
   ```

## Development Workflow

### Before Making Changes

1. Create a new branch for your feature or bugfix:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make sure tests pass:
   ```bash
   pytest
   ```

### Making Changes

1. **Write tests first** - Add tests for new features or bug fixes
2. **Keep changes focused** - One feature or fix per PR
3. **Follow coding standards** - Run linting tools before committing

### Code Quality

#### Linting

We use `ruff` for both linting and formatting:

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

#### Type Checking

We use `mypy` for type checking (optional but recommended):

```bash
mypy src/lq
```

### Testing

Run tests with pytest:

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=lq --cov-report=term

# Run specific test file
pytest tests/test_core.py

# Run specific test
pytest tests/test_core.py::test_function_name

# Run with verbose output
pytest -v

# Stop on first failure
pytest -x
```

### Commit Messages

Write clear, descriptive commit messages:

```
Add feature X to support Y

- Implement functionality Z
- Add tests for new feature
- Update documentation
```

Good commit message format:
- First line: Brief summary (50 chars or less)
- Blank line
- Detailed description if needed
- Reference issues: "Fixes #123" or "Related to #456"

### Pull Requests

1. **Update your branch:**
   ```bash
   git fetch origin
   git rebase origin/main
   ```

2. **Run all checks:**
   ```bash
   ruff check --fix .
   ruff format .
   pytest
   ```

3. **Push your changes:**
   ```bash
   git push origin feature/your-feature-name
   ```

4. **Create a Pull Request:**
   - Go to https://github.com/teaguesterling/lq/pulls
   - Click "New Pull Request"
   - Select your branch
   - Fill in the PR template
   - Link related issues

5. **Respond to feedback:**
   - Address review comments
   - Push additional commits if needed
   - Engage in discussion

## Project Structure

```
lq/
├── src/lq/          # Source code
│   ├── cli.py       # Command-line interface
│   ├── query.py     # Query API
│   ├── serve.py     # MCP server
│   └── schema.sql   # SQL schema
├── tests/           # Test suite
├── docs/            # Documentation (Markdown)
├── .github/         # GitHub Actions workflows
└── pyproject.toml   # Project configuration
```

## Documentation

Documentation is built with MkDocs and Material theme.

### Building Documentation

```bash
# Install documentation dependencies
pip install -e ".[docs]"

# Build documentation
mkdocs build

# Serve locally (auto-reload on changes)
mkdocs serve
```

Visit http://127.0.0.1:8000 to view the documentation.

### Writing Documentation

- Documentation files are in `docs/` as Markdown
- Follow existing style and structure
- Include code examples where appropriate
- Update navigation in `mkdocs.yml` if adding new pages

## Continuous Integration

All pull requests run through GitHub Actions CI:

1. **Lint** - Code style and quality checks with `ruff`
2. **Test** - Test suite on Python 3.10, 3.11, and 3.12
3. **Build** - Package build verification
4. **Docs** - Documentation build check

Make sure all checks pass before requesting review.

## Release Process

(For maintainers)

1. **Update version in `pyproject.toml`**

2. **Update CHANGELOG** (if exists)

3. **Commit version bump:**
   ```bash
   git commit -am "Bump version to X.Y.Z"
   ```

4. **Create tag:**
   ```bash
   git tag -a vX.Y.Z -m "Release X.Y.Z"
   git push origin main --tags
   ```

5. **Create GitHub release:**
   - Go to https://github.com/teaguesterling/lq/releases/new
   - Select the tag
   - Write release notes
   - Publish release

6. **Automatic PyPI publishing:**
   - GitHub Actions will automatically publish to PyPI
   - Verify at https://pypi.org/project/lq/

## Getting Help

- **Issues:** https://github.com/teaguesterling/lq/issues
- **Discussions:** https://github.com/teaguesterling/lq/discussions
- **Email:** teaguesterling@gmail.com

## Code of Conduct

- Be respectful and inclusive
- Welcome newcomers
- Focus on constructive feedback
- Keep discussions relevant

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
