"""
Testes unitários para collector/amazon.py.

Cobre as funções de parsing puro (_parse_price, _parse_sales_volume) e a
função de normalização de campos (_extract_fields).  Nenhuma chamada de rede,
Redis ou banco de dados é feita — todas as dependências externas estão mockadas
via conftest.py.
"""

import pytest
from unittest.mock import patch

import amazon
from amazon import _extract_fields, _parse_price, _parse_sales_volume


# ─────────────────────────────────────────────────────────────────────────────
# _parse_price
# ─────────────────────────────────────────────────────────────────────────────


class TestParsePrice:
    """Testa a conversão de strings de preço no formato Amazon BR para float."""

    def test_parse_price_formato_brasileiro(self):
        """'R$ 661,50' deve ser convertido para o float 661.50."""
        assert _parse_price("R$ 661,50") == 661.50

    def test_parse_price_formato_milhares(self):
        """'R$ 1.299,00' com separador de milhar deve ser convertido para 1299.00."""
        assert _parse_price("R$ 1.299,00") == 1299.00

    def test_parse_price_none(self):
        """Entrada None deve retornar None sem levantar exceção."""
        assert _parse_price(None) is None

    def test_parse_price_string_vazia(self):
        """String vazia deve retornar None (não há número para parsear)."""
        assert _parse_price("") is None


# ─────────────────────────────────────────────────────────────────────────────
# _parse_sales_volume
# ─────────────────────────────────────────────────────────────────────────────


class TestParseSalesVolume:
    """Testa a conversão do texto de volume de vendas da Amazon para inteiro."""

    def test_parse_sales_volume_mil(self):
        """'Mais de 2 mil compras no mês passado' deve retornar o inteiro 2000."""
        assert _parse_sales_volume("Mais de 2 mil compras no mês passado") == 2000

    def test_parse_sales_volume_numero_simples(self):
        """'Mais de 500 compras no mês passado' deve retornar o inteiro 500."""
        assert _parse_sales_volume("Mais de 500 compras no mês passado") == 500

    def test_parse_sales_volume_vazio(self):
        """String vazia deve retornar 0 (ausência de informação de volume)."""
        assert _parse_sales_volume("") == 0


# ─────────────────────────────────────────────────────────────────────────────
# _extract_fields
# ─────────────────────────────────────────────────────────────────────────────


class TestExtractFields:
    """Testa a normalização de um item bruto da API Amazon em dicionário canônico."""

    # Campos obrigatórios que devem estar presentes em qualquer produto extraído
    _CAMPOS_ESPERADOS = {
        "ingestion_id",
        "batch_id",
        "source",
        "collected_at",
        "product_id",
        "title",
        "brand",
        "condition",
        "url",
        "thumbnail_url",
        "price",
        "original_price",
        "discount_pct",
        "currency",
        "rating",
        "num_ratings",
        "is_best_seller",
        "is_amazon_choice",
        "is_prime",
        "free_shipping",
        "delivery_text",
        "sales_volume",
        "num_offers",
        "raw_payload",
    }

    def test_extract_fields_campos_obrigatorios(self, produto_amazon_completo):
        """Produto completo deve retornar dicionário com todos os 24 campos esperados."""
        resultado = _extract_fields(produto_amazon_completo, "batch-test-001")
        assert self._CAMPOS_ESPERADOS.issubset(set(resultado.keys()))

    def test_extract_fields_desconto_calculado(self, produto_amazon_completo):
        """discount_pct deve ser calculado como (original - price) / original * 100
        arredondado para 2 casas decimais.
        price=661.50, original=799.00 → (799-661.5)/799*100 ≈ 17.21%."""
        resultado = _extract_fields(produto_amazon_completo, "batch-test-001")
        assert resultado["discount_pct"] == pytest.approx(17.21, abs=0.01)

    def test_extract_fields_frete_gratis_detectado(self, produto_amazon_frete_gratis):
        """Campo delivery contendo 'GRÁTIS' (case-insensitive) deve definir
        free_shipping=True no produto extraído."""
        resultado = _extract_fields(produto_amazon_frete_gratis, "batch-test-002")
        assert resultado["free_shipping"] is True

    def test_extract_fields_sem_preco_original(self, produto_amazon_sem_desconto):
        """Quando original_price é None, discount_pct deve ser 0.0 (sem desconto
        calculável)."""
        resultado = _extract_fields(produto_amazon_sem_desconto, "batch-test-003")
        assert resultado["discount_pct"] == 0.0


# ─────────────────────────────────────────────────────────────────────────────
# collect_all
# ─────────────────────────────────────────────────────────────────────────────


class TestCollectAll:
    """Testa o generator collect_all() que itera páginas da API."""

    def test_para_imediatamente_em_lista_vazia(self):
        """Quando fetch_page retorna lista vazia na primeira chamada, o generator
        deve encerrar sem yield e chamar fetch_page exatamente uma vez."""
        with patch("amazon.fetch_page", return_value=[]) as mock_fetch:
            resultado = list(amazon.collect_all("batch-test"))

        assert resultado == []
        mock_fetch.assert_called_once()

    def test_retorna_duas_paginas_e_para_em_vazia(self):
        """Quando fetch_page retorna duas páginas com dados e depois lista vazia,
        o generator deve fazer yield de exatamente duas páginas e chamar
        fetch_page três vezes (p1, p2, p3=vazia → break)."""
        pagina1 = [{"product_id": "A1", "title": "Produto 1"}]
        pagina2 = [{"product_id": "A2", "title": "Produto 2"}]

        with patch(
            "amazon.fetch_page", side_effect=[pagina1, pagina2, []]
        ) as mock_fetch:
            resultado = list(amazon.collect_all("batch-test"))

        assert resultado == [pagina1, pagina2]
        assert mock_fetch.call_count == 3
