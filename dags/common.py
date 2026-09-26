"""Helpers compartilhados pelas DAGs (caminho do projeto + engine Postgres)."""

from pathlib import Path

PROJECT_DIR = str(Path(__file__).resolve().parents[1])


def criar_engine():
    """Cria a engine SQLAlchemy a partir das variáveis PG_* do .env."""
    import os

    from dotenv import load_dotenv
    from sqlalchemy import create_engine

    load_dotenv()

    return create_engine(
        f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}"
        f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}"
    )
