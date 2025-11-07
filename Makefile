# Simple and elegant Makefile derived from the almighty https://github.com/pydantic/pydantic
.DEFAULT_GOAL := help
sources = src tests
NUM_THREADS?=8

.PHONY: .uv  ## Check that uv is installed
.uv:
	@uv -V || echo 'Please install uv: https://docs.astral.sh/uv/getting-started/installation/'

.PHONY: .pre-commit  ## Check that pre-commit is installed, or install it
.pre-commit: .uv
	@uv run pre-commit -V || uv pip install pre-commit

.PHONY: install  ## Install the package, dependencies, and pre-commit for local development
install: .uv
	uv sync --frozen --all-extras
	uv run pre-commit install --install-hooks

.PHONY: format  ## Auto-format python source files
format: .uv
	uv run ruff check --fix $(sources)
	uv run ruff format $(sources)

.PHONY: lint  ## Lint python source files
lint: .uv
	uv run ruff check $(sources)
	uv run ruff format --check $(sources)

.PHONY: typecheck  ## Perform type-checking
typecheck: .pre-commit
	uv run pyright $(sources)

.PHONY: test  ## Run all tests
test: .uv
	uv run coverage run -m pytest --durations=10 --log-disable=satpy --log-disable=pyresample

.PHONY: all  ## Run the standard set of checks performed in CI
all: lint typecheck test

.PHONY: clean  ## Clear local caches and build artifacts
clean:
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]'`
	rm -f `find . -type f -name '*~'`
	rm -f `find . -type f -name '.*~'`
	rm -rf .cache
	rm -rf .pytest_cache
	rm -rf .ruff_cache
	rm -rf htmlcov
	rm -rf *.egg-info
	rm -f .coverage
	rm -f .coverage.*
	rm -rf build
	rm -rf dist
	rm -rf site
	rm -rf docs/_build
	rm -rf docs/.changelog.md docs/.version.md docs/.tmp_schema_mappings.html
	rm -rf coverage.xml

.PHONY: docs  ## Generate the docs
docs:
	uv run mkdocs build --strict

.PHONY: docs-serve
docs-serve: ## Build and serve the documentation, for local preview
	uv run mkdocs serve --strict

.PHONY: help  ## Display this message
help:
	@grep -E \
		'^.PHONY: .*?## .*$$' $(MAKEFILE_LIST) | \
		sort | \
		awk 'BEGIN {FS = ".PHONY: |## "}; {printf "\033[36m%-19s\033[0m %s\n", $$2, $$3}'
