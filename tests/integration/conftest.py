"""
Conftest para testes de integração.

Responsabilidades:
1. Remove o mock de psycopg2 que o conftest.py pai (tests/) injetou em
   sys.modules para os testes unitários.
2. Remove o módulo persistence cacheado com o mock, forçando reimport com
   o driver real quando o arquivo de teste fizer 'import persistence'.
3. Registra as fixtures postgresql_proc / postgresql via pytest-postgresql,
   que inicializa um PostgreSQL temporário real usando o pg_ctl do Homebrew.

IMPORTANTE: este conftest.py é carregado pelo pytest ANTES de coletar os
arquivos de teste desta pasta, logo a remoção dos mocks acontece antes de
qualquer import de persistence nos arquivos de integração.
"""

import sys

# ── Remove mocks de psycopg2 ─────────────────────────────────────────────────
for _key in [k for k in list(sys.modules.keys()) if k.startswith("psycopg2")]:
    del sys.modules[_key]

# Remove persistence cacheado (foi importado com o mock de psycopg2 pelos
# testes unitários); a próxima chamada 'import persistence' cria módulo novo
# usando o psycopg2 real.
sys.modules.pop("persistence", None)

# ── pytest-postgresql ─────────────────────────────────────────────────────────
from pytest_postgresql import factories  # noqa: E402 (após manipulação de sys.modules)

postgresql_proc = factories.postgresql_proc(
    executable="/opt/homebrew/opt/postgresql@16/bin/pg_ctl",
)
postgresql = factories.postgresql("postgresql_proc")
