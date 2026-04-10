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

# === Amazon BR via RapidAPI ===
RAPIDAPI_KEY        = os.environ["RAPIDAPI_KEY"]
RAPIDAPI_HOST       = os.environ["RAPIDAPI_HOST"]
AMAZON_DOMAIN       = os.environ["AMAZON_DOMAIN"]
AMAZON_SEARCH_QUERY = os.environ["AMAZON_SEARCH_QUERY"]
AMAZON_MAX_PAGES    = int(os.environ["AMAZON_MAX_PAGES"])
AMAZON_PAGE_SIZE    = int(os.environ["AMAZON_PAGE_SIZE"])

# === Collector ===
COLLECTOR_BATCH_SIZE          = int(os.environ["COLLECTOR_BATCH_SIZE"])
COLLECTOR_SLEEP_BETWEEN_PAGES = int(os.environ["COLLECTOR_SLEEP_BETWEEN_PAGES"])
