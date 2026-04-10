# Build context deve ser a raiz do projeto (docker-compose usa context: .)
# para poder copiar collector/ e dbt/ junto com orchestration/.
FROM python:3.9-slim

WORKDIR /app

# Dependências de sistema: libpq para psycopg2, git para dbt deps
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       libpq-dev gcc git \
    && rm -rf /var/lib/apt/lists/*

# ── Dependências Python ────────────────────────────────────────────────────
# Copiadas antes do código para aproveitar cache de camadas do Docker
COPY orchestration/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Código do collector ────────────────────────────────────────────────────
# Copiado para /app/collector/ para ser executado como subprocess
# (coleta Amazon BR e publica no Redis Stream)
COPY collector/ /app/collector/

# ── Projeto dbt ───────────────────────────────────────────────────────────
COPY dbt/ /app/dbt/

# Pré-instala os pacotes dbt (dbt_utils) em tempo de build.
# O "|| true" evita falha caso o servidor Prefect ainda não esteja disponível
# para outras verificações de ambiente — dbt deps só precisa de rede para PyPI.
RUN cd /app/dbt \
    && dbt deps --profiles-dir /app/dbt \
    && echo "dbt deps OK"

# ── Código de orquestração ────────────────────────────────────────────────
COPY orchestration/ /app/

# Modo padrão: serve com agendamento @hourly conectado ao prefect-server.
# Para rodar uma vez manualmente: docker run ... python pipeline.py --once
CMD ["python", "pipeline.py"]
