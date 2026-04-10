import json
from typing import Optional

import psycopg2
import psycopg2.extras

import config
from logger import get_logger

logger = get_logger("persistence")

# Conexão singleton — reutilizada entre chamadas, reconectada se fechada
_conn: Optional[psycopg2.extensions.connection] = None

# ──────────────────────────────────────────────────────────────────────────────
# DDL
# ──────────────────────────────────────────────────────────────────────────────

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw.products (
    id               SERIAL PRIMARY KEY,

    -- Metadados de ingestão
    ingestion_id     TEXT        NOT NULL,
    batch_id         TEXT        NOT NULL,
    source           TEXT        NOT NULL,
    collected_at     TIMESTAMPTZ NOT NULL,
    inserted_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Identificação do produto
    product_id       TEXT,
    title            TEXT,
    brand            TEXT,
    condition        TEXT,
    url              TEXT,
    thumbnail_url    TEXT,

    -- Preço
    price            NUMERIC,
    original_price   NUMERIC,
    discount_pct     NUMERIC,
    currency         TEXT,

    -- Avaliações
    rating           NUMERIC,
    num_ratings      INTEGER,

    -- Badges Amazon
    is_best_seller   BOOLEAN,
    is_amazon_choice BOOLEAN,
    is_prime         BOOLEAN,

    -- Logística
    free_shipping    BOOLEAN,
    delivery_text    TEXT,

    -- Volume e ofertas
    sales_volume     TEXT,
    num_offers       INTEGER,

    -- Payload bruto para auditoria e reprocessamento
    raw_payload      JSONB,

    -- Chave de idempotência: mesmo produto coletado no mesmo instante
    -- não é duplicado, mesmo que a mensagem seja reentregue pelo Redis
    CONSTRAINT uq_product_collected UNIQUE (product_id, collected_at)
);
"""

_INSERT_SQL = """
INSERT INTO raw.products (
    ingestion_id, batch_id, source, collected_at,
    product_id, title, brand, condition, url, thumbnail_url,
    price, original_price, discount_pct, currency,
    rating, num_ratings,
    is_best_seller, is_amazon_choice, is_prime,
    free_shipping, delivery_text, sales_volume, num_offers,
    raw_payload
)
VALUES (
    %(ingestion_id)s, %(batch_id)s, %(source)s, %(collected_at)s,
    %(product_id)s, %(title)s, %(brand)s, %(condition)s, %(url)s, %(thumbnail_url)s,
    %(price)s, %(original_price)s, %(discount_pct)s, %(currency)s,
    %(rating)s, %(num_ratings)s,
    %(is_best_seller)s, %(is_amazon_choice)s, %(is_prime)s,
    %(free_shipping)s, %(delivery_text)s, %(sales_volume)s, %(num_offers)s,
    %(raw_payload)s
)
ON CONFLICT (product_id, collected_at) DO NOTHING
RETURNING id;
"""

# ──────────────────────────────────────────────────────────────────────────────
# Conexão
# ──────────────────────────────────────────────────────────────────────────────

def _get_connection() -> psycopg2.extensions.connection:
    """
    Retorna a conexão singleton com o PostgreSQL.
    Reconecta automaticamente se a conexão estiver fechada ou quebrada.
    """
    global _conn
    if _conn is None or _conn.closed:
        logger.info("Conectando ao PostgreSQL")
        _conn = psycopg2.connect(
            host=config.POSTGRES_HOST,
            port=config.POSTGRES_PORT,
            dbname=config.POSTGRES_DB,
            user=config.POSTGRES_USER,
            password=config.POSTGRES_PASSWORD,
        )
        _conn.autocommit = False
    return _conn


# ──────────────────────────────────────────────────────────────────────────────
# API pública
# ──────────────────────────────────────────────────────────────────────────────

def ensure_table() -> None:
    """
    Cria a tabela raw.products se ela ainda não existir.
    Deve ser chamada uma vez na inicialização do consumer.
    """
    conn = _get_connection()
    with conn.cursor() as cur:
        cur.execute(_CREATE_TABLE_SQL)
    conn.commit()
    logger.info("Tabela raw.products verificada")


def persist(product: dict) -> bool:
    """
    Persiste um produto no PostgreSQL usando INSERT ... ON CONFLICT DO NOTHING.

    Retorna:
        True  — produto inserido com sucesso (linha nova)
        False — conflito detectado (produto + collected_at já existia)

    Lança exceção em qualquer outro erro de banco, fazendo rollback antes.
    O chamador é responsável por decidir entre retry e dead letter.
    """
    conn = _get_connection()

    # Converte raw_payload de dict para string JSON compatível com o tipo JSONB
    row = dict(product)
    raw = row.get("raw_payload")
    if isinstance(raw, dict):
        row["raw_payload"] = json.dumps(raw, ensure_ascii=False, default=str)
    elif raw is None:
        row["raw_payload"] = None

    try:
        with conn.cursor() as cur:
            cur.execute(_INSERT_SQL, row)
            # RETURNING id devolve a linha apenas se foi inserida;
            # fetchone() retorna None em caso de ON CONFLICT DO NOTHING
            inserted = cur.fetchone() is not None
        conn.commit()
        return inserted

    except Exception:
        conn.rollback()
        raise
