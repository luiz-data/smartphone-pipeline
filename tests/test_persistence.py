"""
Testes unitários para consumer/persistence.py.

Todas as chamadas ao PostgreSQL são interceptadas por mocks:  psycopg2.connect,
cursor e commit/rollback nunca tocam um banco real.  A fixture autouse
reset_singleton_conn injeta a conexão mockada antes de cada teste e restaura
o estado do singleton após, garantindo isolamento total entre os casos.
"""

import pytest
from unittest.mock import MagicMock, patch

import persistence


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures locais
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_cursor():
    """Cursor PostgreSQL mockado com contexto gerenciador (__enter__/__exit__)."""
    cur = MagicMock()
    cur.fetchone.return_value = (1,)   # padrão: inserção bem-sucedida
    return cur


@pytest.fixture
def mock_conn(mock_cursor):
    """Conexão PostgreSQL mockada que devolve mock_cursor ao entrar no with-block."""
    conn = MagicMock()
    conn.closed = False   # 0 = aberta, como retorna psycopg2 real
    conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    conn.cursor.return_value.__exit__  = MagicMock(return_value=False)
    return conn


@pytest.fixture(autouse=True)
def reset_singleton_conn(mock_conn):
    """Injeta mock_conn no singleton _conn antes de cada teste e restaura
    o estado original após, evitando vazamento de estado entre testes."""
    original        = persistence._conn
    persistence._conn = mock_conn
    yield
    persistence._conn = original


@pytest.fixture
def produto_db():
    """Dicionário com todos os campos necessários para uma inserção no banco."""
    return {
        "ingestion_id":     "test-ingestion-id-001",
        "batch_id":         "test-batch-001",
        "source":           "amazon_br",
        "collected_at":     "2024-01-15T10:30:00+00:00",
        "product_id":       "B0CNHXTF4N",
        "title":            "Smartphone Samsung Galaxy S23 128GB",
        "brand":            None,
        "condition":        "new",
        "url":              "https://www.amazon.com.br/dp/B0CNHXTF4N",
        "thumbnail_url":    "https://m.media-amazon.com/images/test.jpg",
        "price":            661.50,
        "original_price":   799.00,
        "discount_pct":     17.21,
        "currency":         "BRL",
        "rating":           4.5,
        "num_ratings":      2847,
        "is_best_seller":   False,
        "is_amazon_choice": True,
        "is_prime":         True,
        "free_shipping":    True,
        "delivery_text":    "Entrega GRÁTIS",
        "sales_volume":     "Mais de 500 compras no mês passado",
        "num_offers":       3,
        "raw_payload":      {"asin": "B0CNHXTF4N", "product_title": "Smartphone"},
    }


# ─────────────────────────────────────────────────────────────────────────────
# TestPersist
# ─────────────────────────────────────────────────────────────────────────────


class TestPersist:
    """Testa o comportamento de persist() para insert, conflito e rollback."""

    def test_insert_produto_novo_retorna_true_e_comita(
        self, mock_cursor, mock_conn, produto_db
    ):
        """Produto novo (already_exists=False): persist() deve retornar True,
        acionar execute() no cursor e realizar commit na conexão.
        Unifica a verificação de retorno E dos side-effects no banco."""
        mock_cursor.fetchone.return_value = (1,)   # RETURNING id retornou linha

        resultado = persistence.persist(produto_db)

        assert resultado is True
        mock_cursor.execute.assert_called_once()   # SQL foi enviado ao banco
        mock_conn.commit.assert_called_once()       # transação foi confirmada

    def test_insert_product_idempotente(self, mock_cursor, produto_db):
        """Mesmo produto inserido duas vezes: primeira chamada retorna True,
        segunda retorna False (ON CONFLICT DO NOTHING → fetchone() == None)."""
        mock_cursor.fetchone.side_effect = [(1,), None]

        primeiro = persistence.persist(produto_db)
        segundo  = persistence.persist(produto_db)

        assert primeiro is True
        assert segundo  is False

    def test_insert_product_campos_obrigatorios(
        self, mock_cursor, mock_conn, produto_db
    ):
        """Produto sem product_id deve levantar KeyError (psycopg2 não consegue
        substituir %(product_id)s no SQL) e acionar rollback antes de propagar
        a exceção."""

        def _execute_side_effect(sql, params):
            if "product_id" not in params:
                raise KeyError("product_id")
            mock_cursor.fetchone.return_value = (1,)

        mock_cursor.execute.side_effect = _execute_side_effect

        produto_sem_id = {k: v for k, v in produto_db.items() if k != "product_id"}

        with pytest.raises(KeyError, match="product_id"):
            persistence.persist(produto_sem_id)

        mock_conn.rollback.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# TestGetConnection
# ─────────────────────────────────────────────────────────────────────────────


class TestGetConnection:
    """Testa o comportamento do singleton de conexão com reconexão automática."""

    def test_reconecta_quando_conexao_fechada(self, mock_conn):
        """Quando _conn.closed=True, _get_connection() deve descartar a conexão
        atual, chamar psycopg2.connect() e retornar a nova conexão.

        Usa patch.object para substituir o atributo psycopg2 diretamente no
        módulo persistence, independente do estado de sys.modules no momento
        da execução (que pode ter sido alterado pelo conftest de integração)."""
        nova_conn = MagicMock()
        nova_conn.closed = False

        closed_conn        = MagicMock()
        closed_conn.closed = True
        persistence._conn  = closed_conn   # autouse fixture restaura após o teste

        with patch.object(persistence, "psycopg2") as mock_pg2:
            mock_pg2.connect.return_value = nova_conn

            resultado = persistence._get_connection()

            mock_pg2.connect.assert_called_once()

        assert resultado is nova_conn
