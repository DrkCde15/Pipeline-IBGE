"""Testes do CNPJCollector (sem rede: session.get mockada)."""

import pytest
from unittest.mock import MagicMock, patch

from src.collectors.cnpj_collector import CNPJCollector


def _mock_response(payload):
    resp = MagicMock()
    resp.json.return_value = payload
    resp.raise_for_status.return_value = None
    return resp


def test_consultar_cnpj_usa_endpoint_correto():
    collector = CNPJCollector()
    with patch.object(collector.session, "get",
                      return_value=_mock_response({"cnpj": "00000000000191"})) as m:
        collector.consultar_cnpj("00.000.000/0001-91")
    url = m.call_args[0][0]
    assert url.endswith("/cnpj/00000000000191")
    assert "cnjp" not in url


def test_consultar_cnpj_invalido_levanta_value_error():
    collector = CNPJCollector()
    with pytest.raises(ValueError):
        collector.consultar_cnpj("123")


def test_base_url_com_sufixo_cnpj_nao_duplica():
    collector = CNPJCollector(base_url="https://receitaws.com.br/v1/cnpj")
    assert collector.base_url == "https://receitaws.com.br/v1"
