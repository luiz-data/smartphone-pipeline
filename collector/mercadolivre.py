import time
import uuid
from datetime import datetime, timezone
from typing import Generator

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

import config
from logger import get_logger

logger = get_logger("mercadolivre")


class RateLimitError(Exception):
    """Erro 429 — API temporariamente indisponível."""
    pass


@retry(
    retry=retry_if_exception_type(RateLimitError),
    wait=wait_exponential(multiplier=1, min=30, max=120),
    stop=stop_after_attempt(3),
)
def fetch_page(offset: int, batch_id: str) -> list[dict]:
    """
    Busca uma página de resultados da API do Mercado Livre.
    Aplica backoff exponencial em caso de erro 429.
    """
    url = f"{config.ML_API_BASE_URL}/sites/{config.ML_SITE_ID}/search"
    params = {
        "category": config.ML_CATEGORY_ID,
        "limit":    config.ML_SEARCH_LIMIT,
        "offset":   offset,
    }

    logger.info(
        "Buscando página",
        extra={"extra": {"offset": offset, "batch_id": batch_id}}
    )

    with httpx.Client(timeout=30) as client:
        response = client.get(url, params=params)

    if response.status_code == 429:
        logger.warning(
            "Rate limit atingido — aguardando backoff",
            extra={"extra": {"offset": offset, "batch_id": batch_id}}
        )
        raise RateLimitError("429 Too Many Requests")

    response.raise_for_status()
    data = response.json()

    results = data.get("results", [])
    logger.info(
        "Página coletada",
        extra={"extra": {"offset": offset, "total": len(results), "batch_id": batch_id}}
    )

    return [_extract_fields(item, batch_id) for item in results]


def _extract_fields(item: dict, batch_id: str) -> dict:
    """
    Extrai e normaliza os campos relevantes de um produto.
    Todos os campos são mapeados aqui — nunca espalhados pelo código.
    """
    price          = item.get("price")
    original_price = item.get("original_price")

    # Calcula desconto apenas se os dois preços estiverem disponíveis
    if original_price and original_price > 0 and price:
        discount_pct = round((original_price - price) / original_price * 100, 2)
    else:
        discount_pct = 0.0

    return {
        # Identificação
        "ingestion_id":    str(uuid.uuid4()),
        "batch_id":        batch_id,
        "source":          "mercadolivre",
        "collected_at":    datetime.now(timezone.utc).isoformat(),

        # Produto
        "product_id":      item.get("id"),
        "title":           item.get("title"),
        "brand":           _extract_attribute(item, "BRAND"),
        "condition":       item.get("condition"),
        "url":             item.get("permalink"),
        "thumbnail_url":   item.get("thumbnail"),

        # Preço
        "price":           price,
        "original_price":  original_price,
        "discount_pct":    discount_pct,

        # Vendedor
        "seller_id":       str(item.get("seller", {}).get("id", "")),
        "seller_name":     item.get("seller", {}).get("nickname", ""),

        # Logística
        "free_shipping":   item.get("shipping", {}).get("free_shipping", False),

        # Volume
        "sold_quantity":   item.get("sold_quantity") or 0,

        # Payload bruto para auditoria
        "raw_payload":     item,
    }


def _extract_attribute(item: dict, attribute_id: str) -> str | None:
    """Extrai um atributo específico da lista de atributos do produto."""
    for attr in item.get("attributes", []):
        if attr.get("id") == attribute_id:
            return attr.get("value_name")
    return None


def collect_all(batch_id: str) -> Generator[list[dict], None, None]:
    """
    Itera sobre todas as páginas e retorna os produtos em lotes.
    Usa Generator para não carregar tudo na memória de uma vez.
    """
    for page in range(config.ML_MAX_PAGES):
        offset = page * config.ML_SEARCH_LIMIT
        products = fetch_page(offset, batch_id)

        if not products:
            logger.info("Sem mais resultados", extra={"extra": {"page": page}})
            break

        yield products

        # Pausa entre páginas para não sobrecarregar a API
        time.sleep(config.COLLECTOR_SLEEP_BETWEEN_PAGES)