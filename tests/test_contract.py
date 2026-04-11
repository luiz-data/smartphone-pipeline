"""
Teste de contrato entre collector/amazon.py e consumer/persistence.py.

Garante que todo %(param)s presente no _INSERT_SQL de persistence.py corresponde
a uma chave retornada por _extract_fields em amazon.py.  Se alguém renomear um
campo em um dos lados sem atualizar o outro, este teste falha em CI antes de
qualquer erro chegar ao ambiente de produção.
"""

import re

import pytest

from amazon import _extract_fields
import persistence


# Item de API representativo, com todos os campos opcionais preenchidos.
_ITEM_EXEMPLO = {
    "asin":                   "B0CONTRACT",
    "product_title":          "Smartphone Contrato Teste",
    "product_price":          "R$ 999,00",
    "product_original_price": "R$ 1.299,00",
    "product_star_rating":    "4.3",
    "product_num_ratings":    100,
    "product_url":            "https://www.amazon.com.br/dp/B0CONTRACT",
    "product_photo":          "https://example.com/img.jpg",
    "is_best_seller":         False,
    "is_amazon_choice":       False,
    "is_prime":               True,
    "delivery":               "Entrega padrão",
    "sales_volume":           "Mais de 100 compras no mês passado",
    "product_num_offers":     1,
    "currency":               "BRL",
}


class TestContrato:
    """Contrato estrutural entre o collector e o consumer."""

    def test_contrato_extract_fields_insert_sql(self):
        """Todos os %(param_name)s do _INSERT_SQL devem existir como chaves no
        dicionário retornado por _extract_fields.

        Falha significa: campo renomeado em amazon.py ou em persistence.py sem
        atualizar o outro lado → KeyError em produção durante o INSERT."""

        # Parâmetros declarados no SQL: %(ingestion_id)s → "ingestion_id"
        params_sql = set(re.findall(r"%\((\w+)\)s", persistence._INSERT_SQL))

        # Campos produzidos pelo extrator do collector
        campos_produto = set(_extract_fields(_ITEM_EXEMPLO, "batch-contract").keys())

        # Todos os parâmetros SQL devem ter um campo correspondente no produto
        faltando = params_sql - campos_produto

        assert not faltando, (
            f"Parâmetros SQL sem campo correspondente em _extract_fields: {faltando}\n"
            "Renomeou um campo? Atualize tanto amazon.py quanto _INSERT_SQL."
        )
