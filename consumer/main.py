"""
main.py — ponto de entrada do consumer.

Fluxo de inicialização:
  1. Cria a tabela raw.products no PostgreSQL (idempotente).
  2. Cria o consumer group no Redis Stream (idempotente).
  3. Entra em loop infinito:
     a. Reclaima mensagens travadas no PEL (XAUTOCLAIM).
     b. Lê novas mensagens do stream (XREADGROUP com BLOCK).
"""

import time

import config
from logger import get_logger
from persistence import ensure_table
from stream_reader import ensure_consumer_group, process_new, process_pending

logger = get_logger("main")


def run() -> None:
    logger.info(
        "Consumer iniciando",
        extra={"extra": {
            "stream":        config.REDIS_STREAM,
            "group":         config.CONSUMER_GROUP,
            "consumer":      config.CONSUMER_NAME,
            "batch_size":    config.CONSUMER_BATCH_SIZE,
            "max_retries":   config.CONSUMER_MAX_RETRIES,
        }}
    )

    # ── Inicialização ─────────────────────────────────────────────────────────
    ensure_table()
    ensure_consumer_group()

    logger.info("Consumer pronto — aguardando mensagens")

    total_processed = 0

    # ── Loop principal ────────────────────────────────────────────────────────
    while True:
        try:
            # 1. Reprocessa mensagens travadas no PEL de qualquer consumer
            pending_count = process_pending()

            # 2. Lê e processa novas mensagens (bloqueia até CONSUMER_BLOCK_MS)
            new_count = process_new()

            total_processed += pending_count + new_count

            if pending_count or new_count:
                logger.info(
                    "Ciclo concluído",
                    extra={"extra": {
                        "pending":         pending_count,
                        "new":             new_count,
                        "total_processed": total_processed,
                    }}
                )

        except KeyboardInterrupt:
            logger.info("Consumer encerrado pelo operador")
            break

        except Exception as exc:
            # Erro inesperado no loop (ex: Redis caiu, reconexão falhou)
            # Aguarda 5 segundos antes de tentar novamente para não gerar
            # loop de erro em alta frequência.
            logger.error(
                "Erro no loop principal — aguardando 5s antes de reintentar",
                extra={"extra": {"error": str(exc)}}
            )
            time.sleep(5)


if __name__ == "__main__":
    run()
