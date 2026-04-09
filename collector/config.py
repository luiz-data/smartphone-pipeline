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

# === Mercado Livre API ===
ML_API_BASE_URL = os.environ["ML_API_BASE_URL"]
ML_SITE_ID      = os.environ["ML_SITE_ID"]
ML_CATEGORY_ID  = os.environ["ML_CATEGORY_ID"]
ML_SEARCH_LIMIT = int(os.environ["ML_SEARCH_LIMIT"])
ML_MAX_PAGES    = int(os.environ["ML_MAX_PAGES"])

# === Collector ===
COLLECTOR_BATCH_SIZE          = int(os.environ["COLLECTOR_BATCH_SIZE"])
COLLECTOR_SLEEP_BETWEEN_PAGES = int(os.environ["COLLECTOR_SLEEP_BETWEEN_PAGES"])