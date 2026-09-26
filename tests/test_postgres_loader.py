"""Testes do postgres_loader (sem banco: engine e read_parquet mockados)."""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.loaders.postgres_loader import carregar_ibge, carregar_tabela


def _mock_engine():
    engine = MagicMock()
    conn = MagicMock()

    @contextmanager
    def _begin():
        yield conn

    engine.begin.side_effect = _begin
    return engine, conn


def test_carregar_tabela_apaga_antes_de_inserir_na_mesma_conexao():
    engine, conn = _mock_engine()
    df = pd.DataFrame([
        {"id": 3550308, "nome": "Sao Paulo", "sigla_uf": "SP"},
        {"id": 3304557, "nome": "Rio de Janeiro", "sigla_uf": "RJ"},
    ])

    chamadas = []
    conn.execute.side_effect = lambda *a, **k: chamadas.append("delete")

    with patch.object(pd.DataFrame, "to_sql",
                      autospec=True) as m_to_sql:
        m_to_sql.side_effect = lambda *a, **k: chamadas.append("insert")
        n = carregar_tabela(df, "municipios_raw", "ibge", "DELETE ...", engine)

    assert n == 2
    assert chamadas == ["delete", "insert"]
    _, kwargs = m_to_sql.call_args
    assert kwargs["schema"] == "ibge" and kwargs["if_exists"] == "append"
    assert kwargs["index"] is False


def test_carregar_tabela_falha_no_delete_nao_insere():
    engine, conn = _mock_engine()
    conn.execute.side_effect = RuntimeError("banco fora")
    df = pd.DataFrame({"id": [1]})

    with patch.object(pd.DataFrame, "to_sql", autospec=True) as m_to_sql:
        with pytest.raises(RuntimeError):
            carregar_tabela(df, "t", "s", "DELETE ...", engine)
    m_to_sql.assert_not_called()


def test_carregar_ibge_le_dois_parquets_e_carrega_duas_tabelas():
    engine, _ = _mock_engine()
    df_mun = pd.DataFrame({"id": [1]})
    df_est = pd.DataFrame({"id": [33]})

    with patch("src.loaders.postgres_loader.pd.read_parquet",
               side_effect=[df_mun, df_est]) as m_read, \
         patch("src.loaders.postgres_loader.carregar_tabela",
               side_effect=[5571, 27]) as m_carga:
        totais = carregar_ibge(engine, "/proc")

    assert totais == {"municipios": 5571, "estados": 27}
    lidos = [str(c.args[0]) for c in m_read.call_args_list]
    assert lidos[0].endswith("municipios") and lidos[1].endswith("estados")
    tabelas = [c.args[1] for c in m_carga.call_args_list]
    assert tabelas == ["municipios_raw", "estados_raw"]
