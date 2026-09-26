"""Carga das tabelas landing no PostgreSQL.

Idempotente por dia: cada carga apaga a partição de hoje (``loaded_at::date =
CURRENT_DATE``) e reinsere o lote na **mesma transação**, então reexecutar a
DAG no mesmo dia não duplica linhas e uma falha no meio não perde o dia.
Qualquer exceção propaga para falhar a task do Airflow (com retry).

As tabelas ``*_raw`` têm UNIQUE por dia (``(id, loaded_at::date)``) como
guarda no banco — ver ``sql/create_schema.sql``.

Portão 2 de data quality: o lote é validado contra ``src.validation.SCHEMAS``
**antes** do DELETE — lote reprovado nunca apaga o dado bom do dia.
"""

import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.validation.schemas import SCHEMAS

logger = logging.getLogger(__name__)


def carregar_tabela(
    df: pd.DataFrame,
    tabela: str,
    schema: str,
    delete_sql: str,
    engine: Engine,
) -> int:
    """Valida (DQ), DELETE do dia + INSERT do lote na mesma transação.

    Args:
        df: Lote a carregar (já transformado).
        tabela: Nome da tabela landing (ex.: ``"municipios_raw"``).
        schema: Schema (ex.: ``"ibge"``).
        delete_sql: DELETE da partição do dia.
        engine: Engine SQLAlchemy (a transação é aberta aqui dentro).

    Returns:
        Quantidade de registros carregados.

    Raises:
        pandera.errors.SchemaError: lote reprovado no portão 2 (nada é
            apagado nem inserido — a validação roda antes da transação).
    """
    validator = SCHEMAS.get((schema, tabela))
    if validator is not None:
        validator.validate(df)
        logger.info(f"DQ {schema}.{tabela} OK: {len(df)} registros")
    with engine.begin() as conn:
        conn.execute(text(delete_sql))
        df.to_sql(tabela, conn, schema=schema, if_exists="append", index=False)
    logger.info(f"{schema}.{tabela}: {len(df)} registros carregados")
    return len(df)


def carregar_ibge(engine: Engine, processed_dir: str | Path) -> dict[str, int]:
    """Carrega estados e municípios transformados nas landings ``ibge.*_raw``."""
    base = Path(processed_dir)

    df_municipios = pd.read_parquet(base / "municipios")
    n_municipios = carregar_tabela(
        df_municipios,
        "municipios_raw",
        "ibge",
        "DELETE FROM ibge.municipios_raw WHERE loaded_at::date = CURRENT_DATE",
        engine,
    )

    df_estados = pd.read_parquet(base / "estados")
    n_estados = carregar_tabela(
        df_estados,
        "estados_raw",
        "ibge",
        "DELETE FROM ibge.estados_raw WHERE loaded_at::date = CURRENT_DATE",
        engine,
    )

    return {"municipios": n_municipios, "estados": n_estados}
