import os

from dotenv import load_dotenv


load_dotenv()


def env_str(key: str, default: str) -> str:
    return os.getenv(key, default)


def env_int(key: str, default: int) -> int:
    value = os.getenv(key)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default



PG_HOST = env_str("PG_HOST", "localhost")
PG_PORT = env_int("PG_PORT", 5432)
PG_DB = env_str("PG_DB", "database")
PG_USER = env_str("PG_USER", "user")
PG_PASSWORD = env_str("PG_PASSWORD", "password")

