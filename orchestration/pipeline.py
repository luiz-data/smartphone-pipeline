"""
pipeline.py — Orquestração do pipeline de smartphones com Prefect 2.x.

Fluxo de execução:
  1. collect_and_publish  — coleta Amazon BR via RapidAPI → Redis Stream
  2. wait_for_consumer    — aguarda consumer persistir tudo no PostgreSQL
  3. run_dbt_deps         — instala pacotes dbt (dbt_utils)
  4. run_dbt_staging      — materializa stg_products (view)
  5. run_dbt_marts        — materializa fct, dim, agg (table/incremental)
  6. run_dbt_tests        — valida qualidade de dados (falha = PARTIAL)

Estados do flow:
  COMPLETED — todas as 6 etapas concluíram com sucesso
  PARTIAL   — etapas 1-5 OK, mas dbt tests reportou falhas nos dados
  FAILED    — falha crítica em etapa 1-5 após esgotamento dos retries

Modos de execução:
  python pipeline.py          → serve com schedule @hourly (modo Docker)
  python pipeline.py --once   → executa uma única vez e sai
"""

import os
import sys
import subprocess
import time
import logging
from datetime import timedelta

import redis as redis_lib
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

load_dotenv()

# ── Configuração via variáveis de ambiente ─────────────────────────────────
REDIS_HOST       = os.environ.get("REDIS_HOST",       "redis")
REDIS_PORT       = int(os.environ.get("REDIS_PORT",   "6379"))
REDIS_STREAM     = os.environ.get("REDIS_STREAM",     "smartphones_raw")
CONSUMER_GROUP   = os.environ.get("CONSUMER_GROUP",   "smartphones_consumer_group")
DBT_PROJECT_DIR  = os.environ.get("DBT_PROJECT_DIR",  "/app/dbt")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/app/dbt")
COLLECTOR_DIR    = os.environ.get("COLLECTOR_DIR",     "/app/collector")

# Timeout máximo aguardando o consumer (segundos)
CONSUMER_WAIT_TIMEOUT_S  = int(os.environ.get("CONSUMER_WAIT_TIMEOUT_S",  "300"))
# Intervalo entre polls do Redis (segundos)
CONSUMER_POLL_INTERVAL_S = int(os.environ.get("CONSUMER_POLL_INTERVAL_S", "10"))


# ── Helper: subprocess com log estruturado ─────────────────────────────────

def _run_subprocess(
    cmd: list,
    cwd: str,
    timeout: int = 600,
    logger=None,
) -> subprocess.CompletedProcess:
    """
    Executa um comando como subprocess, loga stdout/stderr linha a linha
    via logger do Prefect e lança RuntimeError se o exit code não for 0.

    O ambiente do processo filho herda todas as variáveis de ambiente do
    container, incluindo as carregadas do .env pelo docker-compose.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info(f"$ {' '.join(cmd)}  [cwd={cwd}]")

    result = subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=timeout,
        env={**os.environ},
    )

    for line in result.stdout.splitlines():
        if line.strip():
            logger.info(f"  {line}")

    for line in result.stderr.splitlines():
        if line.strip():
            (logger.warning if result.returncode == 0 else logger.error)(f"  {line}")

    if result.returncode != 0:
        raise RuntimeError(
            f"Comando falhou (exit {result.returncode}): {' '.join(cmd)}"
        )

    return result


# ── Tasks ──────────────────────────────────────────────────────────────────

@task(
    name="collect_and_publish",
    description=(
        "Executa o collector: busca smartphones na Amazon BR via RapidAPI "
        "e publica cada produto no Redis Stream 'smartphones_raw'."
    ),
    retries=3,
    retry_delay_seconds=30,
)
def collect_and_publish() -> None:
    """
    Roda `python main.py` em /app/collector como subprocess isolado.

    O processo filho herda as variáveis de ambiente (RAPIDAPI_KEY,
    AMAZON_MAX_PAGES, REDIS_HOST, etc.) e executa a lógica completa
    de coleta + publicação já implementada no collector/main.py.

    Timeout de 10 minutos cobre 10 páginas com backoff exponencial (30-120s).

    Retries (3x, 30s): trata erros de rede e rate limit 429 da RapidAPI.
    """
    logger = get_run_logger()
    logger.info("Iniciando coleta Amazon BR via RapidAPI")

    _run_subprocess(
        cmd=["python", "main.py"],
        cwd=COLLECTOR_DIR,
        timeout=600,
        logger=logger,
    )

    logger.info("Coleta concluída — produtos publicados no Redis Stream")


@task(
    name="wait_for_consumer",
    description=(
        "Faz polling do Redis Stream até o consumer processar todas as "
        "mensagens (pending=0 e lag=0 no consumer group)."
    ),
    retries=5,
    retry_delay_seconds=20,
)
def wait_for_consumer() -> None:
    """
    Conecta ao Redis e verifica o estado do consumer group a cada
    CONSUMER_POLL_INTERVAL_S segundos, aguardando duas condições:

      pending = 0  → nenhuma mensagem entregue ao consumer e ainda não ACKada
                     (Pending Entry List vazia)
      lag     = 0  → nenhuma mensagem no stream ainda não entregue ao grupo
                     (consumer acompanhou o produtor)

    Quando ambas são verdadeiras, todos os produtos foram persistidos
    no PostgreSQL pelo consumer.

    Lança TimeoutError após CONSUMER_WAIT_TIMEOUT_S (padrão 300s),
    o que aciona o mecanismo de retry do Prefect (5x, 20s entre tentativas).
    """
    logger = get_run_logger()
    logger.info(
        f"Aguardando consumer processar {REDIS_STREAM} "
        f"(timeout={CONSUMER_WAIT_TIMEOUT_S}s)"
    )

    client = redis_lib.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )

    # Aguarda pelo menos um ciclo de bloqueio do consumer (BLOCK=5s)
    # antes de começar a verificar para evitar falso positivo inicial.
    logger.info("Aguardando 15s para o consumer iniciar o processamento...")
    time.sleep(15)

    elapsed = 0
    pending = -1
    lag     = -1

    while elapsed < CONSUMER_WAIT_TIMEOUT_S:
        # ── Pending Entry List (mensagens entregues, não ACKadas) ─────────
        try:
            info    = client.xpending(REDIS_STREAM, CONSUMER_GROUP)
            pending = info.get("pending", 0) if isinstance(info, dict) else 0
        except redis_lib.exceptions.ResponseError:
            # Consumer group ainda não existe: stream vazio ou consumer
            # ainda não criou o grupo. Tratamos como 0 pending.
            pending = 0

        # ── Lag (mensagens no stream não entregues ao grupo) ─────────────
        try:
            groups = client.xinfo_groups(REDIS_STREAM)
            lag = 0
            for g in groups:
                if g.get("name") == CONSUMER_GROUP:
                    # 'lag' disponível no Redis 7+ (usado no compose)
                    lag = g.get("lag", 0) or 0
                    break
        except redis_lib.exceptions.ResponseError:
            lag = 0

        logger.info(
            f"Redis Stream — pending={pending} | lag={lag} | elapsed={elapsed}s"
        )

        if pending == 0 and lag == 0:
            logger.info("Consumer processou todas as mensagens")
            return

        time.sleep(CONSUMER_POLL_INTERVAL_S)
        elapsed += CONSUMER_POLL_INTERVAL_S

    raise TimeoutError(
        f"Consumer não processou todas as mensagens em {CONSUMER_WAIT_TIMEOUT_S}s "
        f"(pending={pending}, lag={lag}). "
        f"Verifique os logs do container smartphones_consumer."
    )


@task(
    name="run_dbt_deps",
    description="Garante que os pacotes dbt (dbt_utils) estão instalados.",
    retries=2,
    retry_delay_seconds=30,
)
def run_dbt_deps() -> None:
    """
    Executa `dbt deps --profiles-dir /app/dbt`.
    Idempotente: não reinstala se os pacotes já estiverem em dbt_packages/.
    A imagem Docker já roda este comando no build, mas mantemos aqui como
    salvaguarda para ambientes de desenvolvimento fora do Docker.

    Retries (2x, 30s): trata falhas de rede ao baixar pacotes do dbt Hub.
    """
    logger = get_run_logger()
    logger.info("Verificando dependências dbt")

    _run_subprocess(
        cmd=["dbt", "deps", "--profiles-dir", DBT_PROFILES_DIR],
        cwd=DBT_PROJECT_DIR,
        timeout=120,
        logger=logger,
    )
    logger.info("dbt deps OK")


@task(
    name="run_dbt_staging",
    description=(
        "Executa dbt run --select staging: cria/atualiza a view "
        "stg_products com dados limpos e tipados."
    ),
    retries=2,
    retry_delay_seconds=60,
)
def run_dbt_staging() -> None:
    """
    Materializa a camada staging (view) sobre raw.products:
      - Deduplicação por product_id + collected_at
      - Extração de marca via heurística de título
      - Tipagem, COALESCE e recálculo de discount_pct

    Retries (2x, 60s): trata falhas transitórias de conexão com PostgreSQL.
    """
    logger = get_run_logger()
    logger.info("Executando dbt staging")

    _run_subprocess(
        cmd=[
            "dbt", "run",
            "--select",       "staging",
            "--profiles-dir", DBT_PROFILES_DIR,
        ],
        cwd=DBT_PROJECT_DIR,
        timeout=300,
        logger=logger,
    )
    logger.info("dbt staging concluído")


@task(
    name="run_dbt_marts",
    description=(
        "Executa dbt run --select marts: materializa fct_products "
        "(incremental), dim_sellers e todos os modelos agg_*."
    ),
    retries=2,
    retry_delay_seconds=60,
)
def run_dbt_marts() -> None:
    """
    Materializa a camada marts:
      - fct_products       → incremental/merge (P1–P10)
      - dim_sellers        → table (P3, P10)
      - agg_market_overview → table (P1)
      - agg_free_shipping  → table (P2, P8)
      - agg_price_evolution → table (P5)
      - agg_price_variation → table (P7)
      - agg_price_histogram → table (P9)
      - agg_condition_distribution → table (P6)
      - agg_discount_vs_volume     → table (P4)

    Retries (2x, 60s): trata falhas transitórias de conexão com PostgreSQL.
    """
    logger = get_run_logger()
    logger.info("Executando dbt marts")

    _run_subprocess(
        cmd=[
            "dbt", "run",
            "--select",       "marts",
            "--profiles-dir", DBT_PROFILES_DIR,
        ],
        cwd=DBT_PROJECT_DIR,
        timeout=300,
        logger=logger,
    )
    logger.info("dbt marts concluído")


@task(
    name="run_dbt_tests",
    description=(
        "Executa dbt test: valida not_null, unique, accepted_values "
        "e testes customizados de preço e desconto."
    ),
    retries=1,
    retry_delay_seconds=30,
)
def run_dbt_tests() -> None:
    """
    Executa todos os testes de qualidade definidos em stg_products.yml,
    fct_products.yml e em tests/assert_*.sql.

    Falha nesta task → o flow retorna estado PARTIAL (não FAILED):
    os dados já foram persistidos e transformados; a falha aqui indica
    um problema de qualidade a ser investigado, não perda de dados.

    Retries (1x, 30s): dá uma segunda chance antes de registrar PARTIAL.
    """
    logger = get_run_logger()
    logger.info("Executando testes de qualidade dbt")

    _run_subprocess(
        cmd=[
            "dbt", "test",
            "--profiles-dir", DBT_PROFILES_DIR,
        ],
        cwd=DBT_PROJECT_DIR,
        timeout=300,
        logger=logger,
    )
    logger.info("Todos os testes dbt passaram")


# ── Flow principal ─────────────────────────────────────────────────────────

@flow(
    name="pipeline_smartphones",
    description=(
        "Pipeline completo: Amazon BR → Redis Stream → PostgreSQL → dbt. "
        "Coleta a cada hora, transforma e disponibiliza para o dashboard."
    ),
    log_prints=True,
)
def pipeline_smartphones() -> str:
    """
    Orquestra as 6 etapas em sequência estrita (cada uma depende da anterior).

    Sequência:
      1. collect_and_publish  retries=3 delay=30s
      2. wait_for_consumer    retries=5 delay=20s
      3. run_dbt_deps         retries=2 delay=30s
      4. run_dbt_staging      retries=2 delay=60s
      5. run_dbt_marts        retries=2 delay=60s
      6. run_dbt_tests        retries=1 delay=30s  ← falha = PARTIAL

    Retorno: "COMPLETED" | "PARTIAL"
    O flow só falha (FAILED) se uma etapa 1-5 esgotar seus retries.
    """
    logger = get_run_logger()
    logger.info("═══ Pipeline Smartphones — iniciando ═══")

    # ── 1. Coleta ──────────────────────────────────────────────────────────
    logger.info("── [1/6] Coletar e publicar produtos Amazon BR")
    collect_and_publish()

    # ── 2. Aguardar consumer ───────────────────────────────────────────────
    logger.info("── [2/6] Aguardar consumer Redis → PostgreSQL")
    wait_for_consumer()

    # ── 3. dbt deps ────────────────────────────────────────────────────────
    logger.info("── [3/6] Verificar dependências dbt")
    run_dbt_deps()

    # ── 4. dbt staging ─────────────────────────────────────────────────────
    logger.info("── [4/6] Transformar staging (stg_products)")
    run_dbt_staging()

    # ── 5. dbt marts ───────────────────────────────────────────────────────
    logger.info("── [5/6] Materializar marts (fct, dim, agg)")
    run_dbt_marts()

    # ── 6. dbt tests (não-crítico) ─────────────────────────────────────────
    logger.info("── [6/6] Executar testes de qualidade dbt")
    try:
        run_dbt_tests()
        status = "COMPLETED"
        logger.info("═══ Pipeline finalizado — estado: COMPLETED ═══")
    except Exception as exc:
        status = "PARTIAL"
        logger.warning(
            "═══ dbt tests reportou falhas — estado: PARTIAL ═══\n"
            f"Os dados foram preservados. Para investigar:\n"
            f"  docker exec smartphones_orchestration "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR}\n"
            f"Detalhe: {exc}"
        )

    return status


# ── Entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if "--once" in sys.argv:
        # ── Modo manual: executa uma vez e sai ────────────────────────────
        # Útil para testes locais ou execução avulsa via docker run.
        # Exemplo: docker exec smartphones_orchestration python pipeline.py --once
        print("Modo --once: executando pipeline uma única vez")
        pipeline_smartphones()
    else:
        # ── Modo serviço: registra deployment e fica em loop ─────────────
        # Conecta ao servidor Prefect (PREFECT_API_URL do .env),
        # registra o deployment "smartphones-hourly" com IntervalSchedule
        # de 1 hora e processa runs conforme chegam do servidor.
        #
        # O servidor Prefect persiste o histórico de runs, logs e estados
        # na UI em http://localhost:4200 (ou prefect-server:4200 no Docker).
        print("Modo serviço: registrando deployment e iniciando scheduler @hourly")
        pipeline_smartphones.serve(
            name="smartphones-hourly",
            interval=timedelta(hours=1),
            tags=["smartphones", "amazon-br", "production"],
            description=(
                "Pipeline horário: coleta Amazon BR → Redis → PostgreSQL → dbt. "
                "Acesse a UI do Prefect em http://localhost:4200 para monitorar."
            ),
        )
