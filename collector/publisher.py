import json

import redis

import config
from logger import get_logger

logger = get_logger("publisher")

# Conexão com o Redis (reutilizada entre chamadas)
_redis_client = None


def get_redis() -> redis.Redis:
    """Retorna a conexão com o Redis, criando se necessário."""
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            decode_responses=True,
        )
    return _redis_client


def publish(product: dict) -> bool:
    """
    Publica um produto no Redis Stream.
    Retorna True se sucesso, False se falhou.
    """
    client = get_redis()

    # Serializa o payload como JSON string para o Redis
    message = {
        "ingestion_id": product["ingestion_id"],
        "batch_id":     product["batch_id"],
        "source":       product["source"],
        "collected_at": product["collected_at"],
        "payload":      json.dumps(product, ensure_ascii=False, default=str),
    }

    try:
        msg_id = client.xadd(config.REDIS_STREAM, message)
        logger.info(
            "Produto publicado no Redis",
            extra={"extra": {
                "product_id":   product["product_id"],
                "ingestion_id": product["ingestion_id"],
                "msg_id":       msg_id,
            }}
        )
        return True

    except Exception as e:
        # Falha ao publicar — manda para o stream de falhas
        logger.error(
            "Falha ao publicar produto",
            extra={"extra": {
                "product_id":   product["product_id"],
                "ingestion_id": product["ingestion_id"],
                "error":        str(e),
            }}
        )
        _send_to_failed_stream(client, product, str(e))
        return False


def _send_to_failed_stream(client: redis.Redis, product: dict, error: str) -> None:
    """Envia mensagem com falha para o stream de dead letter."""
    try:
        client.xadd(config.REDIS_FAILED_STREAM, {
            "ingestion_id": product.get("ingestion_id", "unknown"),
            "product_id":   product.get("product_id", "unknown"),
            "error":        error,
        })
    except Exception:
        # Se até o dead letter falhar, apenas loga — não propaga o erro
        logger.error("Falha ao escrever no dead letter stream")