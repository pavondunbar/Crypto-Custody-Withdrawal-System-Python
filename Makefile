.PHONY: up down demo demo-full logs db-balances db-journal db-audit db-recon \
       test clean status

# -----------------------------------------------
# Crypto Custody Withdrawal System
# -----------------------------------------------

up: ## Start all services
	docker-compose up -d --build
	@echo ""
	@echo "Services starting. Run 'make logs' to follow output."

down: ## Stop all services and remove volumes
	docker-compose down -v
	@echo "All services stopped and volumes removed."

demo: ## Run the withdrawal demo (full state machine walkthrough)
	docker-compose run --rm withdrawal-service python withdrawal.py

demo-full: ## Run full end-to-end demo (all services + Kafka publishing)
	docker-compose up -d --build
	@echo ""
	@echo "Infrastructure starting... waiting for services."
	@echo ""
	@sleep 5
	docker-compose run --rm withdrawal-service python withdrawal.py --wait-for-publish

logs: ## Follow logs from all services
	docker-compose logs -f

db-balances: ## Show derived account balances from journal entries
	@docker-compose exec postgres psql -U ledger_user -d ledger_db \
		-c "SELECT account_id, asset, balance FROM account_balances;"

db-journal: ## Show all journal entries
	@docker-compose exec postgres psql -U ledger_user -d ledger_db -c \
		"SELECT journal_id, coa_code, debit, credit, entry_type, created_at \
		 FROM journal_entries ORDER BY created_at, coa_code;"

db-audit: ## Show audit trail
	@docker-compose exec postgres psql -U ledger_user -d ledger_db -c \
		"SELECT trace_id, actor_role, action, resource_type, created_at \
		 FROM audit_events ORDER BY created_at;"

db-recon: ## Show reconciliation run history
	@docker-compose exec postgres psql -U ledger_user -d ledger_db -c \
		"SELECT run_id, status, total_accounts, mismatches_found, started_at \
		 FROM reconciliation_runs ORDER BY started_at DESC;"

status: ## Show running containers
	@docker-compose ps

test: ## Run core tests (no Docker required)
	python -m pytest tests/ -v

clean: ## Remove __pycache__ and .pyc files
	find . -type d -name __pycache__ -exec rm -r {} + 2>/dev/null || true
	find . -name '*.pyc' -delete 2>/dev/null || true

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'
