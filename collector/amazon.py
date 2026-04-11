import re
import time
import uuid
from datetime import datetime, timezone
from typing import Generator, Optional

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

import config
from logger import get_logger

logger = get_logger("amazon")


def _build_headers() -> dict:
    return {
        "x-rapidapi-key":  config.RAPIDAPI_KEY,
        "x-rapidapi-host": config.RAPIDAPI_HOST,
        "Accept":          "application/json",
    }


def _parse_price(raw: Optional[str]) -> Optional[float]:
    """
    Converte string de preço Amazon BR ("R$ 1.999,00") em float (1999.0).
    Retorna None se a string for None ou não puder ser parseada.
    """
    if not raw:
        return None
    # Remove símbolo de moeda, pontos de milhar e troca vírgula decimal por ponto
    cleaned = re.sub(r"[^\d,]", "", raw).replace(",", ".")
    # Se houver mais de um ponto (artefato de milhar), remove todos menos o último
    parts = cleaned.split(".")
    if len(parts) > 2:
        cleaned = "".join(parts[:-1]) + "." + parts[-1]
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_sales_volume(text: str) -> int:
    """
    Converte texto de volume de vendas em número inteiro.
    Exemplos:
        "Mais de 2 mil compras no mês passado" → 2000
        "Mais de 500 compras no mês passado"   → 500
        ""                                      → 0
    """
    if not text:
        return 0
    # Tenta "N mil" primeiro (ex.: "2 mil")
    match = re.search(r"(\d+)\s*mil", text, re.IGNORECASE)
    if match:
        return int(match.group(1)) * 1000
    # Tenta número simples (ex.: "500")
    match = re.search(r"(\d+)", text)
    if match:
        return int(match.group(1))
    return 0


class RateLimitError(Exception):
    """Erro 429 — API temporariamente indisponível."""
    pass


@retry(
    retry=retry_if_exception_type(RateLimitError),
    wait=wait_exponential(multiplier=1, min=30, max=120),
    stop=stop_after_attempt(3),
)
def fetch_page(page: int, batch_id: str) -> list[dict]:
    """
    Busca uma página de resultados da API Amazon BR via RapidAPI.
    Aplica backoff exponencial em caso de erro 429.
    """
    url = f"https://{config.RAPIDAPI_HOST}/search"
    params = {
        "query":   config.AMAZON_SEARCH_QUERY,
        "domain":  config.AMAZON_DOMAIN,
        "page":    page,
        "country": "BR",
    }

    logger.info(
        "Buscando página",
        extra={"extra": {"page": page, "batch_id": batch_id}}
    )

    with httpx.Client(timeout=30) as client:
        response = client.get(url, params=params, headers=_build_headers())

    if response.status_code == 429:
        logger.warning(
            "Rate limit atingido — aguardando backoff",
            extra={"extra": {"page": page, "batch_id": batch_id}}
        )
        raise RateLimitError("429 Too Many Requests")

    response.raise_for_status()
    data = response.json()

    products = data.get("data", {}).get("products", [])
    logger.info(
        "Página coletada",
        extra={"extra": {"page": page, "total": len(products), "batch_id": batch_id}}
    )

    return [_extract_fields(item, batch_id) for item in products]


def _extract_fields(item: dict, batch_id: str) -> dict:
    """
    Extrai e normaliza os campos relevantes de um produto Amazon.
    Todos os campos são mapeados aqui — nunca espalhados pelo código.
    """
    price          = _parse_price(item.get("product_price"))
    original_price = _parse_price(item.get("product_original_price"))

    # Calcula desconto apenas se ambos os preços estiverem disponíveis
    if original_price and original_price > 0 and price:
        discount_pct = round((original_price - price) / original_price * 100, 2)
    else:
        discount_pct = 0.0

    # Rating como float
    try:
        rating = float(item.get("product_star_rating") or 0)
    except (ValueError, TypeError):
        rating = None

    # Frete grátis inferido pelo campo delivery
    delivery_text = item.get("delivery", "") or ""
    free_shipping = "grátis" in delivery_text.lower() or "free" in delivery_text.lower()

    return {
        # Identificação
        "ingestion_id": str(uuid.uuid4()),
        "batch_id":     batch_id,
        "source":       "amazon_br",
        "collected_at": datetime.now(timezone.utc).isoformat(),

        # Produto
        "product_id":    item.get("asin"),
        "title":         item.get("product_title"),
        "brand":         None,   # não retornado na busca; disponível no endpoint /product-details
        "condition":     "new",  # Amazon BR lista produtos novos por padrão na busca
        "url":           item.get("product_url"),
        "thumbnail_url": item.get("product_photo"),

        # Preço
        "price":          price,
        "original_price": original_price,
        "discount_pct":   discount_pct,
        "currency":       item.get("currency", "BRL"),

        # Avaliações
        "rating":      rating,
        "num_ratings": item.get("product_num_ratings") or 0,

        # Badges
        "is_best_seller":   item.get("is_best_seller", False),
        "is_amazon_choice": item.get("is_amazon_choice", False),
        "is_prime":         item.get("is_prime", False),

        # Logística
        "free_shipping":  free_shipping,
        "delivery_text":  delivery_text,

        # Volume de vendas (texto, ex.: "Mais de 1.000 vendidos no último mês")
        "sales_volume": item.get("sales_volume") or "",

        # Número de ofertas de terceiros
        "num_offers": item.get("product_num_offers") or 0,

        # Payload bruto para auditoria
        "raw_payload": item,
    }


def collect_all(batch_id: str) -> Generator[list[dict], None, None]:
    """
    Itera sobre todas as páginas e retorna os produtos em lotes.
    Usa Generator para não carregar tudo na memória de uma vez.
    A API usa paginação 1-based (page=1, 2, 3...).
    """
    for page in range(1, config.AMAZON_MAX_PAGES + 1):
        products = fetch_page(page, batch_id)

        if not products:
            logger.info("Sem mais resultados", extra={"extra": {"page": page}})
            break

        yield products

        # Pausa entre páginas para não sobrecarregar a API
        time.sleep(config.COLLECTOR_SLEEP_BETWEEN_PAGES)
