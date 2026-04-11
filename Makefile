# =============================================================================
# Makefile — smartphone-pipeline
# Comandos utilitários para desenvolvimento, operação e manutenção do pipeline.
#
# Uso:  make <comando>
#       make help          (lista todos os comandos disponíveis)
# =============================================================================

# ── Variáveis ─────────────────────────────────────────────────────────────────
COMPOSE_FILE    := docker-compose.yml
DOCKER_COMPOSE  := docker compose -f $(COMPOSE_FILE)

# Lê do .env se existir; caso contrário usa os defaults do .env.example
-include .env
POSTGRES_USER   ?= pipeline_user
POSTGRES_DB     ?= smartphones
POSTGRES_PORT   ?= 5432
STREAMLIT_PORT  ?= 8501

# Nomes dos containers (definidos no docker-compose.yml)
CT_POSTGRES      := smartphones_postgres
CT_COLLECTOR     := smartphones_collector
CT_CONSUMER      := smartphones_consumer
CT_ORCHESTRATION := smartphones_orchestration
CT_PREFECT       := smartphones_prefect
CT_DASHBOARD     := smartphones_dashboard

# ── Cores ANSI (compatível com zsh/bash no Mac e Linux) ───────────────────────
RESET  := \033[0m
BOLD   := \033[1m
GREEN  := \033[0;32m
YELLOW := \033[0;33m
RED    := \033[0;31m
CYAN   := \033[0;36m
GRAY   := \033[0;90m

# ── Helpers de log ────────────────────────────────────────────────────────────
define log_info
	@printf "$(CYAN)$(BOLD)▶$(RESET) $(1)\n"
endef

define log_ok
	@printf "$(GREEN)$(BOLD)✔$(RESET) $(1)\n"
endef

define log_warn
	@printf "$(YELLOW)$(BOLD)⚠$(RESET) $(1)\n"
endef

define log_error
	@printf "$(RED)$(BOLD)✖$(RESET) $(1)\n"
endef

# =============================================================================
# DEFAULT — make sem argumentos exibe o help
# =============================================================================
.DEFAULT_GOAL := help

# =============================================================================
# HELP
# =============================================================================

.PHONY: help
help: ## Lista todos os comandos disponíveis com descrição
	@printf "\n$(BOLD)$(CYAN)📱 smartphone-pipeline — Comandos disponíveis$(RESET)\n"
	@printf "$(GRAY)────────────────────────────────────────────────────────$(RESET)\n"
	@awk 'BEGIN {FS = ":.*##"; group=""} \
		/^## / { \
			group=substr($$0, 4); \
			printf "\n$(BOLD)%s$(RESET)\n", group \
		} \
		/^[a-zA-Z_-]+:.*?##/ { \
			printf "  $(GREEN)%-22s$(RESET) %s\n", $$1, $$2 \
		}' $(MAKEFILE_LIST)
	@printf "\n$(GRAY)Exemplo: make up | make logs | make collect$(RESET)\n\n"

# =============================================================================
## Infraestrutura
# =============================================================================

.PHONY: up
up: ## Sobe todos os containers em background
	$(call log_info,Iniciando todos os serviços...)
	@$(DOCKER_COMPOSE) up -d --remove-orphans
	$(call log_ok,Serviços no ar.)
	@printf "  $(CYAN)Dashboard$(RESET)  → http://localhost:$(STREAMLIT_PORT)\n"
	@printf "  $(CYAN)Prefect UI$(RESET) → http://localhost:4200\n"

.PHONY: down
down: ## Derruba todos os containers (preserva volumes)
	$(call log_warn,Derrubando todos os serviços...)
	@$(DOCKER_COMPOSE) down
	$(call log_ok,Todos os containers parados.)

.PHONY: build
build: ## Rebuilda todas as imagens sem cache
	$(call log_info,Reconstruindo imagens \(sem cache\)...)
	@$(DOCKER_COMPOSE) build --no-cache
	$(call log_ok,Build concluído.)

.PHONY: restart
restart: down up ## Reinicia todos os containers (down + up)

.PHONY: logs
logs: ## Exibe logs de todos os serviços em tempo real
	@$(DOCKER_COMPOSE) logs -f --tail=100

.PHONY: logs-collector
logs-collector: ## Exibe logs apenas do collector
	@$(DOCKER_COMPOSE) logs -f --tail=100 collector

.PHONY: logs-consumer
logs-consumer: ## Exibe logs apenas do consumer
	@$(DOCKER_COMPOSE) logs -f --tail=100 consumer

.PHONY: logs-prefect
logs-prefect: ## Exibe logs do Prefect server e orchestration
	@$(DOCKER_COMPOSE) logs -f --tail=100 prefect-server orchestration

.PHONY: logs-dashboard
logs-dashboard: ## Exibe logs apenas do dashboard Streamlit
	@$(DOCKER_COMPOSE) logs -f --tail=100 dashboard

# =============================================================================
## Banco de Dados
# =============================================================================

.PHONY: psql
psql: ## Abre o terminal interativo do PostgreSQL
	$(call log_info,Conectando ao PostgreSQL \($(POSTGRES_DB)\)...)
	@docker exec -it $(CT_POSTGRES) \
		psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

.PHONY: db-reset
db-reset: ## ⚠ DESTRÓI e recria o banco do zero (apaga todos os dados)
	$(call log_warn,ATENÇÃO: isso apagará TODOS os dados do banco.)
	@printf "$(RED)Tem certeza? Digite 'sim' para continuar: $(RESET)"; \
	read ans; \
	if [ "$$ans" = "sim" ]; then \
		printf "$(YELLOW)▶ Parando serviços dependentes...$(RESET)\n"; \
		$(DOCKER_COMPOSE) stop collector consumer orchestration dashboard 2>/dev/null || true; \
		printf "$(YELLOW)▶ Recriando banco de dados...$(RESET)\n"; \
		docker exec $(CT_POSTGRES) \
			psql -U $(POSTGRES_USER) -d postgres \
			-c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='$(POSTGRES_DB)';" \
			-c "DROP DATABASE IF EXISTS $(POSTGRES_DB);" \
			-c "CREATE DATABASE $(POSTGRES_DB) OWNER $(POSTGRES_USER);"; \
		docker exec $(CT_POSTGRES) \
			psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) \
			-f /docker-entrypoint-initdb.d/init.sql; \
		$(DOCKER_COMPOSE) start consumer orchestration dashboard 2>/dev/null || true; \
		printf "$(GREEN)✔ Banco recriado. Rode: make seed && make dbt-run$(RESET)\n"; \
	else \
		printf "$(YELLOW)⚠ Operação cancelada.$(RESET)\n"; \
	fi

.PHONY: db-count
db-count: ## Conta registros em cada tabela (raw, staging, marts)
	$(call log_info,Contando registros por tabela...)
	@docker exec $(CT_POSTGRES) psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) \
		-c "SELECT tabela, registros FROM (\
			SELECT 'raw.products'                     AS tabela, COUNT(*) AS registros FROM raw.products \
			UNION ALL \
			SELECT 'staging.stg_products',              COUNT(*) FROM staging.stg_products \
			UNION ALL \
			SELECT 'marts.fct_products',                COUNT(*) FROM marts.fct_products \
			UNION ALL \
			SELECT 'marts.dim_sellers',                 COUNT(*) FROM marts.dim_sellers \
			UNION ALL \
			SELECT 'marts.agg_market_overview',         COUNT(*) FROM marts.agg_market_overview \
			UNION ALL \
			SELECT 'marts.agg_free_shipping',           COUNT(*) FROM marts.agg_free_shipping \
			UNION ALL \
			SELECT 'marts.agg_discount_vs_volume',      COUNT(*) FROM marts.agg_discount_vs_volume \
			UNION ALL \
			SELECT 'marts.agg_condition_distribution',  COUNT(*) FROM marts.agg_condition_distribution \
			UNION ALL \
			SELECT 'marts.agg_price_histogram',         COUNT(*) FROM marts.agg_price_histogram \
			UNION ALL \
			SELECT 'marts.agg_price_evolution',         COUNT(*) FROM marts.agg_price_evolution \
			UNION ALL \
			SELECT 'marts.agg_price_variation',         COUNT(*) FROM marts.agg_price_variation \
		) t ORDER BY tabela;" \
	2>/dev/null || { $(call log_error,Falha ao conectar ao banco. Rode: make up); exit 1; }

# =============================================================================
## Pipeline
# =============================================================================

.PHONY: collect
collect: ## Executa o collector manualmente uma vez (não afeta o agendamento)
	$(call log_info,Executando coleta Amazon BR manualmente...)
	@$(DOCKER_COMPOSE) run --rm collector python main.py
	$(call log_ok,Coleta concluída. Verifique: make db-count)

.PHONY: dbt-deps
dbt-deps: ## Instala dependências do dbt (dbt_utils)
	$(call log_info,Instalando pacotes dbt...)
	@docker exec $(CT_ORCHESTRATION) \
		dbt deps --profiles-dir /app/dbt --project-dir /app/dbt
	$(call log_ok,dbt deps concluído.)

.PHONY: dbt-run
dbt-run: ## Executa todos os modelos dbt (staging + marts)
	$(call log_info,Executando modelos dbt...)
	@docker exec $(CT_ORCHESTRATION) \
		dbt run --profiles-dir /app/dbt --project-dir /app/dbt
	$(call log_ok,dbt run concluído. Dashboard atualizado.)

.PHONY: dbt-test
dbt-test: ## Executa os testes de qualidade do dbt
	$(call log_info,Executando testes dbt...)
	@docker exec $(CT_ORCHESTRATION) \
		dbt test --profiles-dir /app/dbt --project-dir /app/dbt
	$(call log_ok,Testes dbt concluídos.)

.PHONY: dbt-docs
dbt-docs: ## Gera documentação do dbt e serve em http://localhost:8080
	$(call log_info,Gerando documentação dbt...)
	@docker exec $(CT_ORCHESTRATION) \
		dbt docs generate --profiles-dir /app/dbt --project-dir /app/dbt
	$(call log_ok,Docs gerados em /app/dbt/target/.)
	$(call log_warn,Inicie um servidor HTTP manualmente para visualizar:)
	@printf "  docker exec -it $(CT_ORCHESTRATION) sh -c \
		'cd /app/dbt && python -m http.server 8080 --directory target'\n"
	@printf "  Acesse: http://localhost:8080\n"

.PHONY: seed
seed: ## Carrega os dados históricos simulados (historical_prices.csv)
	$(call log_info,Carregando seed de preços históricos...)
	@docker exec $(CT_ORCHESTRATION) \
		dbt seed --profiles-dir /app/dbt --project-dir /app/dbt
	$(call log_ok,Seed carregado. Rode: make dbt-run)

# =============================================================================
## Testes
# =============================================================================

.PHONY: test
test: ## Roda todos os testes pytest do projeto
	$(call log_info,Executando pytest \(todos os testes\)...)
	@$(DOCKER_COMPOSE) run --rm collector \
		sh -c "pip install pytest pytest-asyncio --quiet 2>/dev/null && pytest /app -v" \
	|| $(call log_warn,Nenhum teste encontrado ou falha nos testes.)

.PHONY: test-collector
test-collector: ## Roda apenas os testes do collector
	$(call log_info,Executando testes do collector...)
	@$(DOCKER_COMPOSE) run --rm collector \
		sh -c "pip install pytest --quiet 2>/dev/null && pytest /app -v -k 'collector or amazon or parse'" \
	|| $(call log_warn,Nenhum teste do collector encontrado.)

# =============================================================================
## Utilitários
# =============================================================================

.PHONY: health
health: ## Verifica o status de saúde de todos os serviços
	@printf "\n$(BOLD)$(CYAN)Status dos Serviços$(RESET)\n"
	@printf "$(GRAY)──────────────────────────────────────────────────$(RESET)\n"
	@for svc in postgres redis prefect-server consumer orchestration dashboard; do \
		cid=$$($(DOCKER_COMPOSE) ps -q $$svc 2>/dev/null); \
		if [ -z "$$cid" ]; then \
			printf "  $(RED)●$(RESET) %-22s $(RED)stopped$(RESET)\n" "$$svc"; \
		else \
			state=$$(docker inspect --format='{{.State.Status}}' $$cid 2>/dev/null || echo "unknown"); \
			health=$$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}-{{end}}' $$cid 2>/dev/null || echo "-"); \
			if [ "$$state" = "running" ]; then \
				printf "  $(GREEN)●$(RESET) %-22s $(GRAY)%s$(RESET)\n" "$$svc" "$$health"; \
			else \
				printf "  $(RED)●$(RESET) %-22s $(RED)$$state$(RESET)\n" "$$svc"; \
			fi; \
		fi; \
	done
	@printf "$(GRAY)──────────────────────────────────────────────────$(RESET)\n"
	@printf "  $(CYAN)Dashboard$(RESET)   → http://localhost:$(STREAMLIT_PORT)\n"
	@printf "  $(CYAN)Prefect UI$(RESET)  → http://localhost:4200\n"
	@printf "  $(CYAN)PostgreSQL$(RESET)  → localhost:$(POSTGRES_PORT)\n\n"

.PHONY: clean
clean: ## Remove containers parados e imagens órfãs (preserva volumes de dados)
	$(call log_warn,Removendo containers parados e imagens não utilizadas...)
	@$(DOCKER_COMPOSE) down --remove-orphans
	@docker system prune -f
	$(call log_ok,Limpeza concluída. Volumes de dados preservados.)

.PHONY: reset
reset: ## ⚠ Limpeza total + rebuild + up + seed (ambiente completamente do zero)
	$(call log_warn,RESET COMPLETO: volumes e dados serão apagados!)
	@printf "$(RED)Tem certeza? Digite 'sim' para continuar: $(RESET)"; \
	read ans; \
	if [ "$$ans" = "sim" ]; then \
		$(DOCKER_COMPOSE) down -v --remove-orphans; \
		docker system prune -f; \
		$(MAKE) --no-print-directory build; \
		$(MAKE) --no-print-directory up; \
		printf "$(YELLOW)▶ Aguardando serviços ficarem healthy (30s)...$(RESET)\n"; \
		sleep 30; \
		$(MAKE) --no-print-directory seed; \
		$(MAKE) --no-print-directory dbt-run; \
		$(MAKE) --no-print-directory health; \
		printf "\n$(GREEN)$(BOLD)✔ Reset concluído — pipeline pronto para uso!$(RESET)\n"; \
	else \
		printf "$(YELLOW)⚠ Operação cancelada.$(RESET)\n"; \
	fi

# Declara todos os targets como phony para evitar conflito com arquivos de mesmo nome
.PHONY: help up down build restart logs logs-collector logs-consumer logs-prefect \
        logs-dashboard psql db-reset db-count collect dbt-deps dbt-run dbt-test \
        dbt-docs seed test test-collector health clean reset
