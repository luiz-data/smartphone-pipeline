"""
stream_reader.py — lê mensagens do Redis Stream com consumer group.

Garantias implementadas:
  - At-least-once: XACK só é enviado após persistência confirmada.
  - Idempotência: a camada de persistência usa ON CONFLICT DO NOTHING,
    então reentregas do mesmo msg_id são seguras.
  - Dead letter: após CONSUMER_MAX_RETRIES falhas consecutivas, a mensagem
    vai para REDIS_FAILED_STREAM e é removida do PEL com XACK.
  - Pending recovery: XAUTOCLAIM reclama mensagens travadas no PEL de
    qualquer consumer (útil quando um worker cai antes de dar ACK).
"""

import json
from typing import Optional

import redis
import redis.exceptions

import config
from logger import get_logger
from persistence import persist

logger = get_logger("stream_reader")

# Conexão singleton com o Redis
_redis_client: Optional[redis.Redis] = None

# Contador de falhas por msg_id — mantido em memória durante a execução.
# Em caso de reinício do processo, XAUTOCLAIM irá reprocessar mensagens
# pendentes e o contador recomeça do zero, o que é aceitável para o cenário
# at-least-once (o pior caso é tentar mais 3 vezes antes do dead letter).
_fail_counts: dict = {}


# ──────────────────────────────────────────────────────────────────────────────
# Conexão
# ──────────────────────────────────────────────────────────────────────────────

def get_redis() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            decode_responses=True,
        )
    return _redis_client


# ──────────────────────────────────────────────────────────────────────────────
# Inicialização do consumer group
# ──────────────────────────────────────────────────────────────────────────────

def ensure_consumer_group() -> None:
    """
    Cria o consumer group se ainda não existir.
    id="0" faz o group começar pelo início do stream (processa mensagens
    antigas se houver); troque para "$" para processar apenas novas.
    mkstream=True cria o stream se ele não existir.
    """
    client = get_redis()
    try:
        client.xgroup_create(
            name=config.REDIS_STREAM,
            groupname=config.CONSUMER_GROUP,
            id="0",
            mkstream=True,
        )
        logger.info(
            "Consumer group criado",
            extra={"extra": {
                "group":  config.CONSUMER_GROUP,
                "stream": config.REDIS_STREAM,
            }}
        )
    except redis.exceptions.ResponseError as exc:
        if "BUSYGROUP" in str(exc):
            # Grupo já existe — comportamento esperado em reinício
            logger.info(
                "Consumer group já existe",
                extra={"extra": {"group": config.CONSUMER_GROUP}}
            )
        else:
            raise


# ──────────────────────────────────────────────────────────────────────────────
# Processamento de uma mensagem individual
# ──────────────────────────────────────────────────────────────────────────────

def _handle_message(client: redis.Redis, msg_id: str, fields: dict) -> None:
    """
    Processa uma mensagem do stream:
      1. Desserializa o payload JSON.
      2. Tenta persistir no PostgreSQL.
      3. Em sucesso (insert ou duplicata): XACK e limpa contador de falhas.
      4. Em exceção: incrementa contador; se >= MAX_RETRIES → dead letter + XACK.
    """
    # ── 1. Parse do payload ──────────────────────────────────────────────────
    try:
        payload_str = fields.get("payload", "{}")
        product = json.loads(payload_str)
    except (json.JSONDecodeError, TypeError) as exc:
        logger.error(
            "Payload inválido — enviando para dead letter imediatamente",
            extra={"extra": {"msg_id": msg_id, "error": str(exc)}}
        )
        _send_to_dead_letter(client, msg_id, fields, str(exc))
        return

    product_id = product.get("product_id", "unknown")

    # ── 2. Persistência ───────────────────────────────────────────────────────
    try:
        inserted = persist(product)

        if inserted:
            logger.info(
                "Produto persistido",
                extra={"extra": {"product_id": product_id, "msg_id": msg_id}}
            )
        else:
            logger.info(
                "Produto duplicado — ignorado (idempotência)",
                extra={"extra": {"product_id": product_id, "msg_id": msg_id}}
            )

        # ── 3. ACK após persistência confirmada ───────────────────────────────
        client.xack(config.REDIS_STREAM, config.CONSUMER_GROUP, msg_id)
        _fail_counts.pop(msg_id, None)

    except Exception as exc:
        # ── 4. Falha: incrementa contador e decide retry ou dead letter ───────
        _fail_counts[msg_id] = _fail_counts.get(msg_id, 0) + 1
        fail_count = _fail_counts[msg_id]

        logger.warning(
            "Falha ao persistir produto",
            extra={"extra": {
                "product_id": product_id,
                "msg_id":     msg_id,
                "fail_count": fail_count,
                "max_retries": config.CONSUMER_MAX_RETRIES,
                "error":      str(exc),
            }}
        )

        if fail_count >= config.CONSUMER_MAX_RETRIES:
            logger.error(
                "Máximo de tentativas atingido — enviando para dead letter",
                extra={"extra": {"product_id": product_id, "msg_id": msg_id}}
            )
            _send_to_dead_letter(client, msg_id, fields, str(exc))


# ──────────────────────────────────────────────────────────────────────────────
# Dead letter
# ──────────────────────────────────────────────────────────────────────────────

def _send_to_dead_letter(
    client: redis.Redis,
    msg_id: str,
    fields: dict,
    error: str,
) -> None:
    """
    Publica a mensagem problemática no stream de dead letter e faz XACK
    para removê-la do PEL (evita que fique em loop infinito).
    """
    try:
        client.xadd(config.REDIS_FAILED_STREAM, {
            "original_msg_id": msg_id,
            "ingestion_id":    fields.get("ingestion_id", "unknown"),
            "product_id":      fields.get("product_id", "unknown"),
            "batch_id":        fields.get("batch_id", "unknown"),
            "error":           error,
            "payload":         fields.get("payload", "{}"),
        })
    except Exception as dl_exc:
        # Se até o dead letter falhar, apenas loga — não propaga
        logger.error(
            "Falha ao escrever no dead letter stream",
            extra={"extra": {"msg_id": msg_id, "error": str(dl_exc)}}
        )

    # XACK mesmo em caso de falha no dead letter para não travar o PEL
    try:
        client.xack(config.REDIS_STREAM, config.CONSUMER_GROUP, msg_id)
        _fail_counts.pop(msg_id, None)
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────────────────────
# Leitura de mensagens
# ──────────────────────────────────────────────────────────────────────────────

def process_pending() -> int:
    """
    Reclama e reprocessa mensagens do PEL que estão paradas há mais de
    CONSUMER_MIN_IDLE_MS milissegundos (ex.: consumer que caiu antes do ACK).

    Usa XAUTOCLAIM (Redis >= 6.2, disponível no redis:7-alpine do compose).
    Retorna o número de mensagens processadas.
    """
    client = get_redis()

    # xautoclaim devolve [next_cursor, [(msg_id, fields), ...], [deleted_ids]]
    result = client.xautoclaim(
        name=config.REDIS_STREAM,
        groupname=config.CONSUMER_GROUP,
        consumername=config.CONSUMER_NAME,
        min_idle_time=config.CONSUMER_MIN_IDLE_MS,
        start_id="0-0",
        count=config.CONSUMER_BATCH_SIZE,
    )

    messages = result[1]  # lista de (msg_id, fields)
    for msg_id, fields in messages:
        _handle_message(client, msg_id, fields)

    if messages:
        logger.info(
            "Mensagens pendentes reprocessadas",
            extra={"extra": {"count": len(messages)}}
        )

    return len(messages)


def process_new() -> int:
    """
    Lê novas mensagens do stream (id='>') usando o consumer group.
    Bloqueia até CONSUMER_BLOCK_MS ms aguardando novas mensagens.
    Retorna o número de mensagens processadas.
    """
    client = get_redis()

    results = client.xreadgroup(
        groupname=config.CONSUMER_GROUP,
        consumername=config.CONSUMER_NAME,
        streams={config.REDIS_STREAM: ">"},
        count=config.CONSUMER_BATCH_SIZE,
        block=config.CONSUMER_BLOCK_MS,
    )

    if not results:
        return 0

    total = 0
    for _stream_name, messages in results:
        for msg_id, fields in messages:
            _handle_message(client, msg_id, fields)
            total += 1

    return total
