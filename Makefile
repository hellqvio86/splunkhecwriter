.PHONY: setup lint test clean build upload renovate renovate-summary

setup:
	uv sync --locked --group dev

lint:
	uv run --locked pylint src/splunkhecwriter

test:
	uv run --locked pytest --cov=splunkhecwriter --cov-fail-under=90 --cov-report=term-missing --cov-report=json tests/
	uv run --locked python .github/scripts/generate_coverage_badge.py

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


hooks: ## Install git pre-commit hook
	@chmod +x .githooks/pre-commit 2>/dev/null || true
	@git config core.hooksPath .githooks
	@echo "Git pre-commit hook installed and core.hooksPath set to .githooks."

secrets: ## Run TruffleHog secret scanning
	@echo "Running TruffleHog secret scan..."
	@if command -v trufflehog >/dev/null 2>&1; then \
		trufflehog --no-update git file://$$(pwd) --only-verified --fail; \
	elif command -v docker >/dev/null 2>&1; then \
		docker run --rm -v $$(pwd):/src -w /src trufflesecurity/trufflehog:latest git file:///src --only-verified --fail; \
	else \
		echo "ERROR: trufflehog or docker is required!" >&2 && exit 1; \
	fi

trivy: ## Run Trivy vulnerability scanning
	@echo "Running Trivy vulnerability scan..."
	@if command -v trivy >/dev/null 2>&1; then \
		trivy fs --severity HIGH,CRITICAL --exit-code 1 .; \
	elif command -v docker >/dev/null 2>&1; then \
		docker run --rm -v $$(pwd):/src -v trivy-cache:/root/.cache -w /src aquasec/trivy:latest fs --severity HIGH,CRITICAL --exit-code 1 .; \
	else \
		echo "ERROR: trivy or docker is required!" >&2 && exit 1; \
	fi

