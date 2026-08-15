SHELL := /bin/bash
REGION ?= ca-central-1
ENV    ?= dev
ACCT   := $(shell aws sts get-caller-identity --query Account --output text)
TFVARS := envs/$(ENV).tfvars

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS=":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

.PHONY: preflight
preflight: ## Check this machine can build the project (no AWS changes)
	./scripts/preflight.sh

.PHONY: lint
lint: ## Lint and format-check Python
	ruff check .
	ruff format --check .

.PHONY: test
test: ## Run unit tests
	pytest

.PHONY: package
package: ## Build the poller Lambda zip (no Docker needed, ~2 MB)
	./scripts/build_poller_zip.sh

.PHONY: image
image: ## OPTIONAL: build and push a container image instead of the zip
	./scripts/build_push_poller.sh

.PHONY: clean
clean: ## Free local disk: build artifacts, caches, sample data
	rm -rf build/ .pytest_cache .ruff_cache htmlcov .coverage
	find . -type d -name __pycache__ -prune -exec rm -rf {} + 2>/dev/null || true
	rm -rf /tmp/feat /tmp/tp-sample
	@echo "cleaned. Docker images (if any): docker system prune -a"

.PHONY: disk
disk: ## Show what this project is using locally
	@echo "repo:      $$(du -sh . 2>/dev/null | cut -f1)"
	@echo "venv:      $$(du -sh .venv 2>/dev/null | cut -f1 || echo none)"
	@echo "build:     $$(du -sh build 2>/dev/null | cut -f1 || echo none)"
	@command -v docker >/dev/null 2>&1 && echo "docker:    $$(docker system df --format '{{.Size}}' 2>/dev/null | head -1 || echo n/a)" || true

.PHONY: init
init: ## terraform init against the remote backend
	cd infra && terraform init \
	  -backend-config="bucket=tfstate-transitpulse-$(ACCT)" \
	  -backend-config="region=$(REGION)"

.PHONY: plan
plan: ## terraform plan
	@test -f build/poller.zip || (echo "run 'make package' first" && exit 1)
	cd infra && terraform fmt -recursive && terraform validate && \
	  terraform plan -var-file=$(TFVARS) -out=tf.plan

.PHONY: apply
apply: ## terraform apply the saved plan
	cd infra && terraform apply tf.plan

.PHONY: destroy
destroy: ## Tear everything down
	cd infra && terraform destroy -var-file=$(TFVARS)

.PHONY: backfill
backfill: ## Backfill N days of ETL: make backfill DAYS=21
	./scripts/backfill.sh $(or $(DAYS),14)

.PHONY: check
check: ## Daily operational health check
	./scripts/daily_check.sh

.PHONY: data
data: ## Daily data check: are we accumulating usable training days?
	./scripts/data_check.sh

.PHONY: agent-install
agent-install: ## Install the ops agent's dependencies (LangChain, Groq)
	pip install -r requirements-agent.txt

.PHONY: agent-tools
agent-tools: ## Smoke-test every agent tool against AWS, no LLM, no Groq tokens
	python -m agent.supervisor --tools-only

.PHONY: agent
agent: ## Run the daily ops agent now and write reports/$(shell date -u +%Y-%m-%d).md
	python -m agent.supervisor --verbose

.PHONY: docs
docs: ## Refresh README figures and charts from live queries
	python -m agent.docs_updater --verbose

.PHONY: docs-figures
docs-figures: ## Refresh README figures only: no charts, no model, no tokens
	python -m agent.docs_updater --verbose --skip-charts --skip-drift

.PHONY: pause
pause: ## Stop ingestion (keeps everything else alive)
	aws events disable-rule --name transitpulse-poll --region $(REGION)
	@echo "ingestion paused"

.PHONY: resume
resume: ## Resume ingestion
	aws events enable-rule --name transitpulse-poll --region $(REGION)
	@echo "ingestion resumed"
