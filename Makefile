.PHONY: setup lint clean build upload renovate renovate-summary

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

renovate:
	@./bin/renovate/run.sh

renovate-summary:
	@./bin/renovate/run.sh --summary

