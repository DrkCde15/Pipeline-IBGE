"""Data quality do pipeline — dois portões, mesma exceção.

Portão 1 (pós-coleta): validadores leves sobre o JSON bruto da API
(``validar_*_raw``). Barram lote vazio ou com campos estruturais faltando
antes de persistir.

Portão 2 (pós-transformação): schemas Pandera sobre os DataFrames que serão
carregados no Postgres (``SCHEMAS``). Barram duplicatas, nulos em chave e
domínios inválidos (UF) antes do DELETE+INSERT.

Ambos levantam ``DataQualityError`` (raw) ou ``pandera.errors.SchemaError``
(transformado) — a task do Airflow falha alto em vez de propagar dado ruim.
"""

import logging
from typing import Any

import pandera as pa
from pandera import Check, Column, DataFrameSchema

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Lote reprovado no portão de data quality (pós-coleta)."""


# 27 UFs + valores que o transformer pode produzir para linhas incompletas.
UFS = {
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA",
    "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN",
    "RO", "RR", "RS", "SC", "SE", "SP", "TO",
}


# ---------------------------------------------------------------------------
# Portão 1 — JSON bruto das APIs
# ---------------------------------------------------------------------------

def _exigir_lista(dados: Any, fonte: str) -> list:
    if not isinstance(dados, list) or not dados:
        raise DataQualityError(f"{fonte}: lote vazio ou fora do formato esperado (lista).")
    return dados


def validar_estados_raw(dados: list[dict[str, Any]]) -> int:
    """Cada estado precisa de id, sigla e nome."""
    dados = _exigir_lista(dados, "IBGE/estados")
    ruins = [d for d in dados if not (d.get("id") and d.get("sigla") and d.get("nome"))]
    if ruins:
        raise DataQualityError(f"IBGE/estados: {len(ruins)}/{len(dados)} sem id/sigla/nome. Ex.: {ruins[0]}")
    logger.info(f"DQ estados raw OK: {len(dados)} registros")
    return len(dados)


def validar_municipios_raw(dados: list[dict[str, Any]]) -> int:
    """Cada município precisa de id, nome e da hierarquia até a UF."""
    dados = _exigir_lista(dados, "IBGE/municipios")

    def _ok(m: dict) -> bool:
        try:
            uf = m["microrregiao"]["mesorregiao"]["UF"]
            return bool(m.get("id") and m.get("nome") and uf.get("sigla"))
        except (KeyError, TypeError):
            return False

    ruins = [m for m in dados if not _ok(m)]
    if ruins:
        raise DataQualityError(
            f"IBGE/municipios: {len(ruins)}/{len(dados)} sem id/nome/UF. Ex.: {str(ruins[0])[:200]}"
        )
    logger.info(f"DQ municipios raw OK: {len(dados)} registros")
    return len(dados)


# ---------------------------------------------------------------------------
# Portão 2 — DataFrames transformados (Pandera)
# ---------------------------------------------------------------------------

ESTADOS_SCHEMA = DataFrameSchema(
    {
        "id": Column(int, Check.gt(0), unique=True, nullable=False),
        "sigla": Column(str, Check.str_length(2, 2), unique=True, nullable=False),
        "nome": Column(str, nullable=False),
    },
    strict=False,
    name="estados",
)

MUNICIPIOS_SCHEMA = DataFrameSchema(
    {
        "id": Column(int, Check.gt(0), unique=True, nullable=False),
        "nome": Column(str, nullable=False),
        "sigla_uf": Column(str, Check.isin(UFS), nullable=False),
    },
    strict=False,
    name="municipios",
)

SCHEMAS = {
    ("ibge", "estados_raw"): ESTADOS_SCHEMA,
    ("ibge", "municipios_raw"): MUNICIPIOS_SCHEMA,
}
