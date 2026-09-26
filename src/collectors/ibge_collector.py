"""
Coletor de dados do IBGE (Instituto Brasileiro de Geografia e Estatística).
Realiza a coleta de indicadores sociodemográficos e econômicos via API REST.
"""

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Configurações (sobrescrevíveis via .env)
BASE_URL = os.getenv("IBGE_API_BASE", "https://servicodados.ibge.gov.br/api/v1").rstrip("/")
AGREGADOS_BASE_URL = os.getenv(
    "IBGE_AGREGADOS_BASE", "https://servicodados.ibge.gov.br/api/v3/agregados"
).rstrip("/")
try:
    TIMEOUT = int(os.getenv("IBGE_TIMEOUT", "30"))
except ValueError:
    TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 2

OUTPUT_DIR = Path(__file__).parent.parent.parent / "data" / "raw" / "ibge"


class IBGECollector:
    """Coletor de dados do IBGE via API REST."""

    def __init__(self, base_url: str | None = None, agregados_base_url: str | None = None) -> None:
        self.base_url = (base_url or BASE_URL).rstrip("/")
        self.agregados_base_url = (agregados_base_url or AGREGADOS_BASE_URL).rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json", "User-Agent": "PipelineDadosPublicos/1.0"})
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    def _get_with_retry(self, url: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """GET com retry para uma URL absoluta."""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info(f"Requisitando: {url} (tentativa {attempt}/{MAX_RETRIES})")
                response = self.session.get(url, params=params, timeout=TIMEOUT)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout na requisição (tentativa {attempt})")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY * attempt)
            except requests.exceptions.HTTPError as e:
                status = e.response.status_code if e.response is not None else "?"
                logger.error(f"Erro HTTP {status}: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY * attempt)
            except requests.exceptions.RequestException as e:
                logger.error(f"Erro de conexão: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY * attempt)

        logger.error(f"Falha após {MAX_RETRIES} tentativas: {url}")
        return []

    def _request_with_retry(self, endpoint: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Realiza requisição com retry e tratamento de erros."""
        return self._get_with_retry(f"{self.base_url}/{endpoint.lstrip('/')}", params)

    def buscar_agregado(
        self,
        tabela: int,
        periodo: int | str,
        variavel: int,
        localidades: str = "N1[all]",
    ) -> list[dict[str, Any]]:
        """Busca dados do SIDRA via API de Agregados (v3).

        Ex.: buscar_agregado(6579, 2024, 9324, "N1[all]")
        → população residente estimada do Brasil em 2024.

        Args:
            tabela: ID da tabela SIDRA (ex: 6579).
            periodo: Ano/período (ex: 2024) ou "last".
            variavel: ID da variável (ex: 9324).
            localidades: Filtro territorial (ex: "N1[all]", "N3[33]", "N6[all]").

        Returns:
            Lista de dicts no formato da API de agregados.
        """
        url = f"{self.agregados_base_url}/{tabela}/periodos/{periodo}/variaveis/{variavel}"
        dados = self._get_with_retry(url, {"localidades": localidades})
        logger.info(f"Agregado {tabela}/{periodo}/{variavel} [{localidades}]: {len(dados)} itens")
        return dados

    def listar_municipios(self, uf: int | None = None) -> list[dict[str, Any]]:
        """Lista todos os municípios do Brasil ou de um estado específico.

        Args:
            uf: Código da Unidade Federativa (ex: 33 = RJ). None para todos.

        Returns:
            Lista de dicts com dados dos municípios.
        """
        params: dict[str, Any] = {"output": "json"}
        if uf is not None:
            params["localidade"] = uf

        dados = self._request_with_retry("localidades/municipios", params)
        logger.info(f"Total de municípios obtidos: {len(dados)}")
        return dados

    def listar_estados(self) -> list[dict[str, Any]]:
        """Lista todos os estados brasileiros."""
        return self._request_with_retry("localidades/estados", {"output": "json"})

    def listar_indicadores(self) -> list[dict[str, Any]]:
        """Lista indicadores disponíveis no IBGE."""
        return self._request_with_retry("indicadores", {"output": "json"})

    def buscar_indicador(
        self,
        id_indicador: int,
        localidade: str = "N7 [all]",
        periodo: int | None = None,
    ) -> dict[str, Any]:
        """Busca dados de um indicador específico.

        Args:
            id_indicador: ID do indicador IBGE.
            localidade: Filtro de localidade (padrão: todas as unidades da federação).
            período: Ano de referência.

        Returns:
            Dict com os dados do indicador.
        """
        params: dict[str, Any] = {"localidade": localidade}
        if periodo is not None:
            params["periodo"] = periodo

        dados = self._request_with_retry(f"indicadores/{id_indicador}/resultados", params)
        logger.info(f"Indicador {id_indicador}: {len(dados)} resultados obtidos")
        return dados

    def buscar_pib_municipal(self, municipio_id: int) -> list[dict[str, Any]]:
        """Busca dados do PIB municipal.

        Args:
            municipio_id: ID do município no IBGE (código IBGE 7 dígitos).

        Returns:
            Lista com séries históricas do PIB municipal.
        """
        return self._request_with_retry(
            f"bbr/{municipio_id}/indicadores/21",
            {"output": "json"},
        )

    def buscar_populacao(self, municipio_id: int) -> list[dict[str, Any]]:
        """Busca estimativa populacional do município.

        Args:
            municipio_id: ID do município no IBGE.

        Returns:
            Lista com estimativas populacionais.
        """
        return self._request_with_retry(
            f"bbr/{municipio_id}/indicadores/47001",
            {"output": "json"},
        )

    def salvar_json(self, dados: list[dict[str, Any] | dict[str, Any]], nome_arquivo: str) -> Path:
        """Salva os dados em formato JSON.

        Args:
            dados: Dados a serem salvos.
            nome_arquivo: Nome do arquivo de saída.

        Returns:
            Caminho do arquivo salvo.
        """
        caminho = OUTPUT_DIR / 'ibge.json'  # Nome fixo para o arquivo JSON
        with open(caminho, "w", encoding="utf-8") as f:
            json.dump(dados, f, ensure_ascii=False, indent=2)
        logger.info(f"Dados salvos em: {caminho}")
        return caminho

    def executar_coleta_completa(self) -> dict[str, Path]:
        """Executa a coleta completa de dados do IBGE.

        Returns:
            Dict mapeando tipo de dado para caminho do arquivo salvo.
        """
        logger.info("=== Início da coleta completa IBGE ===")

        arquivos: dict[str, Path] = {}

        logger.info("Coletando estados...")
        estados = self.listar_estados()
        arquivos["estados"] = self.salvar_json(estados, "estados.json")

        logger.info("Coletando municípios...")
        municipios = self.listar_municipios()
        arquivos["municipios"] = self.salvar_json(municipios, "municipios.json")

        logger.info("Coletando indicadores disponíveis...")
        indicadores = self.listar_indicadores()
        arquivos["indicadores"] = self.salvar_json(indicadores, "indicadores.json")

        logger.info("=== Fim da coleta completa IBGE ===")
        logger.info(f"Arquivos gerados: {list(arquivos.keys())}")
        return arquivos


def main() -> None:
    """Função principal para executar a coleta do IBGE."""
    collector = IBGECollector()
    arquivos = collector.executar_coleta_completa()

    for tipo, caminho in arquivos.items():
        print(f"  {tipo}: {caminho}")


if __name__ == "__main__":
    main()
