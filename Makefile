.PHONY: install lint format typecheck test

install:
	uv sync --all-extras

lint:
	uv run ruff check .

format:
	uv run ruff format .

typecheck:
	uv run mypy pipelines metadata monitoring config

test:
	uv run pytest
