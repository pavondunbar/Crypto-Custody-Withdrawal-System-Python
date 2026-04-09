.PHONY: up down demo demo-full logs db-balances db-journal db-audit db-recon \
       test clean status build restart ps shell-pg shell-kafka health \
       integrity kafka-tail topics

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

build: ## Build all images without starting services
	docker-compose build

restart: ## Restart all services without rebuilding
	docker-compose restart

ps: ## Show running containers (alias for status)
	@docker-compose ps

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

shell-pg: ## Open interactive psql shell in PostgreSQL
	@docker-compose exec postgres psql -U ledger_user -d ledger_db

shell-kafka: ## Open bash shell in Kafka container
	@docker-compose exec kafka bash

health: ## Check health of all services
	@echo "--- PostgreSQL ---"
	@docker-compose exec postgres pg_isready -U ledger_user -d ledger_db || true
	@echo ""
	@echo "--- Signing Gateway ---"
	@docker-compose exec signing-gateway python -c \
		"import urllib.request, json; \
		r = urllib.request.urlopen('http://localhost:8000/health'); \
		print(json.loads(r.read()))" 2>/dev/null || echo "  unreachable"
	@echo ""
	@echo "--- MPC Nodes ---"
	@for node in mpc-node-1 mpc-node-2 mpc-node-3; do \
		printf "  $$node: "; \
		docker-compose exec $$node python -c \
			"import urllib.request, json; \
			r = urllib.request.urlopen('http://localhost:8001/health'); \
			print(json.loads(r.read()))" 2>/dev/null || echo "unreachable"; \
	done
	@echo ""
	@echo "--- Container Status ---"
	@docker-compose ps --format "table {{.Name}}\t{{.Status}}"

integrity: ## Verify ledger integrity (balanced journals, no orphans)
	@echo "--- Journal Balance Check (unbalanced journal_ids) ---"
	@docker-compose exec postgres psql -U ledger_user -d ledger_db -c \
		"SELECT journal_id, SUM(debit) AS total_debit, SUM(credit) AS total_credit, \
		        SUM(debit) - SUM(credit) AS imbalance \
		 FROM journal_entries \
		 GROUP BY journal_id \
		 HAVING SUM(debit) <> SUM(credit);"
	@echo ""
	@echo "--- Orphaned Status History (no matching transaction) ---"
	@docker-compose exec postgres psql -U ledger_user -d ledger_db -c \
		"SELECT sh.id, sh.transaction_id, sh.status \
		 FROM transaction_status_history sh \
		 LEFT JOIN transactions t ON t.id = sh.transaction_id \
		 WHERE t.id IS NULL;"
	@echo ""
	@echo "--- Reconciliation Summary ---"
	@docker-compose exec postgres psql -U ledger_user -d ledger_db -c \
		"SELECT status, COUNT(*) AS runs, SUM(mismatches_found) AS total_mismatches \
		 FROM reconciliation_runs GROUP BY status;"
	@echo ""
	@echo "--- Unresolved Dead-Letter Queue ---"
	@docker-compose exec postgres psql -U ledger_user -d ledger_db -c \
		"SELECT COUNT(*) AS unresolved_events FROM dead_letter_queue \
		 WHERE resolved_at IS NULL;"

kafka-tail: ## Tail messages from all custody Kafka topics
	@docker-compose exec kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--whitelist 'custody\.withdrawal\..*' \
		--from-beginning --timeout-ms 10000

topics: ## List all Kafka topics
	@docker-compose exec kafka kafka-topics \
		--bootstrap-server localhost:9092 --list

test: ## Run core tests (no Docker required)
	python -m pytest tests/ -v

clean: ## Remove __pycache__ and .pyc files
	find . -type d -name __pycache__ -exec rm -r {} + 2>/dev/null || true
	find . -name '*.pyc' -delete 2>/dev/null || true

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'
