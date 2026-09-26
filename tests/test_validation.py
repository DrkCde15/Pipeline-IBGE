"""Testes do data quality: portão 1 (raw) e portão 2 (pandera + loader)."""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pandera.errors import SchemaError

from src.loaders.postgres_loader import carregar_tabela
from src.validation.schemas import (
    DataQualityError,
    MUNICIPIOS_SCHEMA,
    validar_estados_raw,
    validar_municipios_raw,
)


def _municipio(id_=3550308, uf="SP"):
    return {
        "id": id_,
        "nome": "São Paulo",
        "microrregiao": {"mesorregiao": {"UF": {"sigla": uf}}},
    }


# --- Portão 1: raw ---

def test_validar_municipios_raw_aprova_lote_bom():
    assert validar_municipios_raw([_municipio(), _municipio(3304557, "RJ")]) == 2


def test_validar_municipios_raw_barra_lote_vazio():
    with pytest.raises(DataQualityError):
        validar_municipios_raw([])


def test_validar_municipios_raw_barra_sem_hierarquia_uf():
    with pytest.raises(DataQualityError):
        validar_municipios_raw([{"id": 1, "nome": "X"}])


def test_validar_estados_raw_barra_sem_sigla():
    with pytest.raises(DataQualityError):
        validar_estados_raw([{"id": 33, "nome": "Rio de Janeiro"}])


# --- Portão 2: pandera ---

def _df_municipios_ok():
    return pd.DataFrame([{"id": 3550308, "nome": "Sao Paulo", "sigla_uf": "SP"}])


def test_municipios_schema_aprova_e_barra_duplicada_e_uf_invalida():
    MUNICIPIOS_SCHEMA.validate(_df_municipios_ok())
    with pytest.raises(SchemaError):
        MUNICIPIOS_SCHEMA.validate(
            pd.DataFrame([
                {"id": 1, "nome": "A", "sigla_uf": "SP"},
                {"id": 1, "nome": "B", "sigla_uf": "RJ"},
            ])
        )
    with pytest.raises(SchemaError):
        MUNICIPIOS_SCHEMA.validate(
            pd.DataFrame([{"id": 1, "nome": "A", "sigla_uf": "XX"}])
        )


def test_loader_barra_lote_reprovado_antes_do_delete():
    engine = MagicMock()
    conn = MagicMock()

    @contextmanager
    def _begin():
        yield conn

    engine.begin.side_effect = _begin
    df_ruim = pd.DataFrame([{"id": 1, "nome": "A", "sigla_uf": "XX"}])

    with patch.object(pd.DataFrame, "to_sql", autospec=True) as m_to_sql:
        with pytest.raises(SchemaError):
            carregar_tabela(df_ruim, "municipios_raw", "ibge", "DELETE ...", engine)

    conn.execute.assert_not_called()  # nada apagado
    m_to_sql.assert_not_called()  # nada inserido


def test_loader_aprova_lote_bom():
    engine = MagicMock()
    conn = MagicMock()

    @contextmanager
    def _begin():
        yield conn

    engine.begin.side_effect = _begin

    with patch.object(pd.DataFrame, "to_sql", autospec=True):
        n = carregar_tabela(_df_municipios_ok(), "municipios_raw", "ibge",
                            "DELETE ...", engine)
    assert n == 1
    conn.execute.assert_called_once()
