"""Testes do IBGECollector (sem rede: session.get mockada)."""

from unittest.mock import MagicMock, patch

from src.collectors.ibge_collector import IBGECollector


def _mock_response(payload):
    resp = MagicMock()
    resp.json.return_value = payload
    resp.raise_for_status.return_value = None
    return resp


def test_listar_estados_chama_endpoint_correto():
    collector = IBGECollector()
    estados = [{"id": 33, "sigla": "RJ", "nome": "Rio de Janeiro",
                "regiao": {"id": 3, "sigla": "SE", "nome": "Sudeste"}}]
    with patch.object(collector.session, "get", return_value=_mock_response(estados)) as m:
        resultado = collector.listar_estados()
    assert resultado == estados
    url = m.call_args[0][0]
    assert url == f"{collector.base_url}/localidades/estados"


def test_listar_municipios_com_uf_usa_rota_por_estado():
    collector = IBGECollector()
    with patch.object(collector.session, "get",
                      return_value=_mock_response([{"id": 3304557}])) as m:
        collector.listar_municipios(33)
    url = m.call_args[0][0]
    assert url == f"{collector.base_url}/localidades/estados/33/municipios"


def test_listar_municipios_sem_uf_busca_todos():
    collector = IBGECollector()
    with patch.object(collector.session, "get",
                      return_value=_mock_response([])) as m:
        collector.listar_municipios()
    url = m.call_args[0][0]
    assert url == f"{collector.base_url}/localidades/municipios"


def test_buscar_agregado_monta_url_e_params():
    collector = IBGECollector()
    payload = [{"id": "9324", "resultados": []}]
    with patch.object(collector.session, "get", return_value=_mock_response(payload)) as m:
        resultado = collector.buscar_agregado(6579, 2024, 9324, "N1[all]")
    assert resultado == payload
    url = m.call_args[0][0]
    kwargs = m.call_args[1]
    assert url == f"{collector.agregados_base_url}/6579/periodos/2024/variaveis/9324"
    assert kwargs["params"] == {"localidades": "N1[all]"}
