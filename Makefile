.PHONY: test-auth run-dbt list-workflows workflow-status view-logs run-workflow help

# Default GitHub repository - replace with your repository name
REPO := votr-me/votr_dbt

help: ## Display this help message
	@echo "Usage: make <target> [options]"
	@echo ""
	@echo "Targets:"
	@awk -F ':|##' '/^[^\t].+?:.*?##/ { printf "  %-20s %s\n", $$1, $$NF }' $(MAKEFILE_LIST)

check-gh: ## Check if GitHub CLI is installed
	@command -v gh >/dev/null 2>&1 || { echo >&2 "❌ GitHub CLI (gh) is not installed. Visit: https://cli.github.com/"; exit 1; }

test-auth: check-gh ## Run the auth test workflow
	@echo "🔍 Running auth test workflow..."
	@if ! gh workflow run dbt_test.yml --ref main -R $(REPO); then \
		echo "❌ Failed to run auth test workflow. Please check if the workflow file exists in the repository and try again."; \
		exit 1; \
	fi

run-dbt: check-gh ## Run the full DBT workflow
	@echo "🚀 Running DBT workflow..."
	@if ! gh workflow run dbt_test.yml --ref main -R $(REPO); then \
		echo "❌ Failed to run DBT workflow. Ensure the workflow file 'dbt_test.yml' exists and that you have permissions to run it."; \
		exit 1; \
	fi

list-workflows: check-gh ## List available workflows
	@echo "📜 Available workflows:"
	@if ! gh workflow list -R $(REPO); then \
		echo "❌ Unable to list workflows. Please check your repository settings and ensure the GitHub CLI has appropriate access."; \
		exit 1; \
	fi

workflow-status: check-gh ## Check status of recent workflow runs
	@echo "📊 Recent workflow runs:"
	@if ! gh run list -R $(REPO) -L 5; then \
		echo "❌ Unable to retrieve workflow status. Ensure that your GitHub CLI is authenticated and has access to the repository."; \
		exit 1; \
	fi

view-logs: check-gh ## View logs of the latest workflow run
	@echo "📄 Fetching logs for the latest workflow run..."
	@RUN_ID=$(shell gh run list -R $(REPO) -L 1 --json databaseId --jq '.[0].databaseId')
	@if [ -z "$(RUN_ID)" ]; then \
		echo "❌ Failed to retrieve the latest workflow run ID. Make sure there are recent runs and that you have repository access."; \
		exit 1; \
	fi
	@if ! gh run view $(RUN_ID) --log -R $(REPO); then \
		echo "❌ Unable to fetch logs for the latest workflow run with ID $(RUN_ID). Check if the workflow run has log files or if there are access restrictions."; \
		exit 1; \
	fi

run-workflow: check-gh ## Run specified workflow with real-time monitoring. Usage: make run-workflow workflow=<workflow-id or name>
	@if [ -z "$(workflow)" ]; then \
		echo "❌ Error: Please specify the workflow ID or name, e.g., make run-workflow workflow=dbt_test.yml"; \
		exit 1; \
	fi
	@echo "🚀 Triggering workflow '$(workflow)' on branch 'main' in repository '$(REPO)'..."
	@RUN_ID=$$(gh workflow run "$(workflow)" --ref main -R "$(REPO)" 2>&1 | grep -o 'https://github.com/$(REPO)/actions/runs/[0-9]*' | grep -o '[0-9]*'); \
	if [ -z "$$RUN_ID" ]; then \
		echo "❌ Failed to trigger workflow. Ensure that '$(workflow)' exists in the '.github/workflows/' directory and that you have permission to run it."; \
		exit 1; \
	fi; \
	echo "✅ Workflow triggered with run ID: $$RUN_ID"; \
	echo "📡 Streaming live output for workflow run $$RUN_ID..."; \
	if ! gh run watch "$$RUN_ID" -R "$(REPO)"; then \
		echo "❌ Error streaming logs for workflow run ID $$RUN_ID. Ensure that the run is still active or check your network connection."; \
		exit 1; \
	fi
