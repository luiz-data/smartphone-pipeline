"""
Testes de integração para consumer/persistence.py contra PostgreSQL real.

Utiliza pytest-postgresql para inicializar um cluster PostgreSQL temporário.
Nenhum mock de banco de dados é utilizado — as chamadas de INSERT, COMMIT e
ROLLBACK são executadas contra o engine real.

Para pular estes testes: pytest tests/ -m "not integration"
Para rodar apenas estes: pytest tests/integration/ -v
"""

import copy

import pytest

# persistence é importado aqui com psycopg2 REAL porque o conftest.py desta
# pasta removeu o mock e descartou o módulo cacheado com mock antes desta
# importação.
import persistence

pytestmark = pytest.mark.integration

_BATCH_ID = "batch-integration-test"

_PRODUTO_BASE = {
    "ingestion_id":     "int-ingestion-001",
    "batch_id":         _BATCH_ID,
    "source":           "amazon_br",
    "collected_at":     "2024-01-15T10:30:00+00:00",
    "product_id":       "B0INTTEST01",
    "title":            "Smartphone Integração Test",
    "brand":            None,
    "condition":        "new",
    "url":              "https://www.amazon.com.br/dp/B0INTTEST01",
    "thumbnail_url":    "https://example.com/img.jpg",
    "price":            999.00,
    "original_price":   1299.00,
    "discount_pct":     23.02,
    "currency":         "BRL",
    "rating":           4.5,
    "num_ratings":      500,
    "is_best_seller":   False,
    "is_amazon_choice": True,
    "is_prime":         True,
    "free_shipping":    True,
    "delivery_text":    "Entrega GRÁTIS",
    "sales_volume":     "Mais de 100 compras no mês passado",
    "num_offers":       2,
    "raw_payload":      {"asin": "B0INTTEST01"},
}


@pytest.fixture(autouse=True)
def setup_persistence(postgresql):
    """Prepara o banco real para cada teste de integração:
    - Cria schema raw e tabela raw.products usando o DDL do módulo
    - Injeta a conexão real no singleton persistence._conn
    - Limpa os dados do batch de teste após cada caso
    """
    with postgresql.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
        cur.execute(persistence._CREATE_TABLE_SQL)
    postgresql.commit()

    original          = persistence._conn
    persistence._conn = postgresql
    yield
    persistence._conn = original

    # Limpeza: garante que dados de teste não vazem entre casos
    with postgresql.cursor() as cur:
        cur.execute(
            "DELETE FROM raw.products WHERE batch_id = %s", (_BATCH_ID,)
        )
    postgresql.commit()


class TestIntegracaoPostgreSQL:
    """Testes contra PostgreSQL real — sem mocks de banco de dados."""

    def test_insert_funciona_corretamente(self):
        """persist() com produto válido deve inserir a linha no PostgreSQL real
        e retornar True indicando que a linha foi criada."""
        resultado = persistence.persist(copy.deepcopy(_PRODUTO_BASE))

        assert resultado is True

    def test_on_conflict_garante_idempotencia_real(self):
        """Inserir o mesmo produto duas vezes: a segunda chamada deve retornar
        False porque ON CONFLICT DO NOTHING no PostgreSQL real não cria
        duplicata — fetchone() retorna None para 0 linhas afetadas."""
        primeira = persistence.persist(copy.deepcopy(_PRODUTO_BASE))
        segunda  = persistence.persist(copy.deepcopy(_PRODUTO_BASE))

        assert primeira is True
        assert segunda  is False

    def test_rollback_em_caso_de_erro(self, postgresql):
        """persist() deve chamar rollback e não commitar quando o PostgreSQL
        rejeita o valor: string 'nao-e-numero' na coluna NUMERIC provoca
        DataError no servidor → persist() relança a exceção sem commitar dados.

        Nota: captura Exception genérica porque o driver da conexão injetada
        pelo pytest-postgresql pode ser psycopg2 ou psycopg3 dependendo da
        versão instalada; o que importa é que a exceção é propagada e o
        rollback impede qualquer dado de ser commited."""
        produto_invalido = copy.deepcopy(_PRODUTO_BASE)
        produto_invalido["discount_pct"] = "nao-e-numero"   # Força DataError

        with pytest.raises(Exception):
            persistence.persist(produto_invalido)

        # Rollback foi acionado: nenhum dado do batch deve estar na tabela
        with postgresql.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM raw.products WHERE batch_id = %s",
                (_BATCH_ID,),
            )
            count = cur.fetchone()[0]

        assert count == 0
