.PHONY: setup lint clean build upload

setup:
	uv sync --locked --group dev

lint:
	uv run --locked pylint src/splunkhecwriter

clean:
	rm -rf __pycache__ build dist src/*.egg-info

build:
	uv build

upload: build
	uv publish
