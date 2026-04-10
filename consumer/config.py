import os
from dotenv import load_dotenv

# Carrega o .env se existir (útil para rodar fora do Docker)
load_dotenv()

# === PostgreSQL ===
POSTGRES_HOST     = os.environ["POSTGRES_HOST"]
POSTGRES_PORT     = int(os.environ["POSTGRES_PORT"])
POSTGRES_DB       = os.environ["POSTGRES_DB"]
POSTGRES_USER     = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

# === Redis ===
REDIS_HOST          = os.environ["REDIS_HOST"]
REDIS_PORT          = int(os.environ["REDIS_PORT"])
REDIS_STREAM        = os.environ["REDIS_STREAM"]
REDIS_FAILED_STREAM = os.environ["REDIS_FAILED_STREAM"]

# === Consumer ===
# Nome do consumer group no Redis Stream
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "smartphones_consumer_group")
# Nome desta instância de consumer (útil ao escalar horizontalmente)
CONSUMER_NAME  = os.environ.get("CONSUMER_NAME", "consumer_1")
# Quantas mensagens buscar por lote do stream
CONSUMER_BATCH_SIZE = int(os.environ.get("CONSUMER_BATCH_SIZE", "10"))
# Tempo máximo de espera por novas mensagens antes de checar pending (ms)
CONSUMER_BLOCK_MS = int(os.environ.get("CONSUMER_BLOCK_MS", "5000"))
# Tentativas antes de mandar para dead letter
CONSUMER_MAX_RETRIES = int(os.environ.get("CONSUMER_MAX_RETRIES", "3"))
# Tempo mínimo que uma mensagem deve estar parada no PEL para ser reclamada (ms)
CONSUMER_MIN_IDLE_MS = int(os.environ.get("CONSUMER_MIN_IDLE_MS", "60000"))
