import uuid
from datetime import datetime, timezone

import config
from logger import get_logger
from mercadolivre import collect_all
from publisher import publish

logger = get_logger("main")


def run():
    """Executa uma rodada completa de coleta e publicação."""
    batch_id = f"batch-{datetime.now(timezone.utc).strftime('%Y-%m-%d-%Hh')}"

    logger.info("Iniciando coleta", extra={"extra": {"batch_id": batch_id}})

    total_published = 0
    total_failed    = 0

    for page_products in collect_all(batch_id):
        for product in page_products:
            success = publish(product)
            if success:
                total_published += 1
            else:
                total_failed += 1

    logger.info(
        "Coleta finalizada",
        extra={"extra": {
            "batch_id":        batch_id,
            "total_published": total_published,
            "total_failed":    total_failed,
        }}
    )


if __name__ == "__main__":
    run()