.PHONY: help install test lint type-check format clean

all: test lint

install:
	uv sync

test:
	pytest tests -v
	cd interop && ./run_tests.sh

test-cov:
	pytest tests --cov=capnweb --cov-report=html --cov-report=term


check: lint

lint:
	ruff check
	pyrefly check src
	ruff format --check

format:
	ruff format
	ruff check --fix
	ruff format

#
# Performance baseline (see docs/architecture/capnweb-performance-baseline.md)
#
bench:
	uv run python -m benchmarks.run_all

bench-ts:
	@test -e benchmarks/ts_compare/node_modules || \
		ln -sfn ../../tests/interop/node_modules benchmarks/ts_compare/node_modules
	cd benchmarks/ts_compare && npx tsx run_all.ts

bench-all: bench bench-ts

bench-profile:
	uv run python -m benchmarks.profile_hotpaths

clean:
	rm -rf build/ dist/ *.egg-info htmlcov/ .coverage .pytest_cache/ .mypy_cache/ .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

publish: clean
	git push --tags
	uv build
	twine upload dist/*
