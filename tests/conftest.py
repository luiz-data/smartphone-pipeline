"""
Configuração global de testes: sys.path, mocks de módulos externos e fixtures
compartilhadas entre test_amazon.py e test_persistence.py.

Todos os sys.modules são injetados ANTES de qualquer import dos módulos sob
teste, garantindo que nenhuma chamada real a APIs externas, Redis ou PostgreSQL
ocorra durante a suite.
"""

import os
import sys
from unittest.mock import MagicMock

# ── sys.path ─────────────────────────────────────────────────────────────────
# Resolve o diretório raiz do projeto (smartphone-pipeline/) a partir de tests/
_base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_base, "collector"))
sys.path.insert(0, os.path.join(_base, "consumer"))

# ── Mocks de dependências externas ────────────────────────────────────────────
# Devem ser registrados em sys.modules antes que qualquer import dos módulos
# sob teste ocorra; o pytest carrega conftest.py antes de coletar os testes.

# tenacity — usado como decorator em fetch_page; o mock faz fetch_page virar
# um MagicMock (o que é aceitável, pois não testamos essa função aqui).
sys.modules["tenacity"] = MagicMock()

# httpx — cliente HTTP usado apenas em fetch_page
sys.modules["httpx"] = MagicMock()

# psycopg2 — adaptador PostgreSQL
_mock_psycopg2 = MagicMock()
sys.modules["psycopg2"] = _mock_psycopg2
sys.modules["psycopg2.extras"] = MagicMock()
sys.modules["psycopg2.extensions"] = MagicMock()

# redis — não importado diretamente em amazon.py / persistence.py, mas pode
# aparecer em imports transitivos
sys.modules["redis"] = MagicMock()

# config — variáveis de ambiente; valores irrelevantes para os testes unitários
_mock_config = MagicMock()
_mock_config.RAPIDAPI_KEY = "test-rapidapi-key"
_mock_config.RAPIDAPI_HOST = "amazon-search5.p.rapidapi.com"
_mock_config.AMAZON_SEARCH_QUERY = "smartphone"
_mock_config.AMAZON_DOMAIN = "amazon.com.br"
_mock_config.AMAZON_MAX_PAGES = 3
_mock_config.COLLECTOR_SLEEP_BETWEEN_PAGES = 0
_mock_config.POSTGRES_HOST = "localhost"
_mock_config.POSTGRES_PORT = 5432
_mock_config.POSTGRES_DB = "testdb"
_mock_config.POSTGRES_USER = "testuser"
_mock_config.POSTGRES_PASSWORD = "testpass"
sys.modules["config"] = _mock_config

# logger — logging estruturado; retorna um MagicMock que absorve todos os calls
_mock_logger_mod = MagicMock()
_mock_logger_mod.get_logger.return_value = MagicMock()
sys.modules["logger"] = _mock_logger_mod

# ── Fixtures compartilhadas ───────────────────────────────────────────────────
import pytest  # noqa: E402  (import após manipulação de sys.modules é intencional)


@pytest.fixture
def produto_amazon_completo():
    """Produto Amazon com todos os campos preenchidos, incluindo desconto e frete grátis."""
    return {
        "asin": "B0CNHXTF4N",
        "product_title": "Smartphone Samsung Galaxy S23 128GB Preto",
        "product_price": "R$ 661,50",
        "product_original_price": "R$ 799,00",
        "product_star_rating": "4.5",
        "product_num_ratings": 2847,
        "product_url": "https://www.amazon.com.br/dp/B0CNHXTF4N",
        "product_photo": "https://m.media-amazon.com/images/test.jpg",
        "is_best_seller": False,
        "is_amazon_choice": True,
        "is_prime": True,
        "delivery": "Entrega GRÁTIS",
        "sales_volume": "Mais de 500 compras no mês passado",
        "product_num_offers": 3,
        "currency": "BRL",
    }


@pytest.fixture
def produto_amazon_sem_desconto():
    """Produto Amazon sem preço original — discount_pct deve ser 0.0."""
    return {
        "asin": "B0TEST001",
        "product_title": "Smartphone Teste Sem Desconto",
        "product_price": "R$ 500,00",
        "product_original_price": None,
        "product_star_rating": "4.0",
        "product_num_ratings": 100,
        "product_url": "https://www.amazon.com.br/dp/B0TEST001",
        "product_photo": "https://m.media-amazon.com/images/test2.jpg",
        "is_best_seller": False,
        "is_amazon_choice": False,
        "is_prime": False,
        "delivery": "Entrega padrão em 5 dias",
        "sales_volume": "",
        "product_num_offers": 1,
        "currency": "BRL",
    }


@pytest.fixture
def produto_amazon_frete_gratis():
    """Produto Amazon com frete grátis explícito no campo delivery."""
    return {
        "asin": "B0TEST002",
        "product_title": "Smartphone Teste Frete Grátis",
        "product_price": "R$ 1.299,00",
        "product_original_price": None,
        "product_star_rating": "4.2",
        "product_num_ratings": 500,
        "product_url": "https://www.amazon.com.br/dp/B0TEST002",
        "product_photo": "https://m.media-amazon.com/images/test3.jpg",
        "is_best_seller": False,
        "is_amazon_choice": False,
        "is_prime": True,
        "delivery": "Entrega GRÁTIS",
        "sales_volume": "Mais de 2 mil compras no mês passado",
        "product_num_offers": 2,
        "currency": "BRL",
    }
