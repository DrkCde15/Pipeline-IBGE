"""
Coletor de dados do IBGE (Instituto Brasileiro de Geografia e Estatística).
Localidades via API v1 e agregados SIDRA via API v3.
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

    def listar_municipios(self, uf: int | str | None = None) -> list[dict[str, Any]]:
        """Lista todos os municípios do Brasil ou de um estado específico.

        Args:
            uf: Código (ex: 33) ou sigla (ex: "RJ") da Unidade Federativa.
                None para todos os municípios do Brasil.

        Returns:
            Lista de dicts com dados dos municípios.
        """
        if uf is not None:
            endpoint = f"localidades/estados/{uf}/municipios"
        else:
            endpoint = "localidades/municipios"

        dados = self._request_with_retry(endpoint, {"orderBy": "nome"})
        logger.info(f"Total de municípios obtidos: {len(dados)}")
        return dados

    def listar_estados(self) -> list[dict[str, Any]]:
        """Lista todos os estados brasileiros."""
        return self._request_with_retry("localidades/estados", {"orderBy": "nome"})

    def salvar_json(self, dados: list[dict[str, Any]] | dict[str, Any], nome_arquivo: str) -> Path:
        """Salva os dados em formato JSON.

        Args:
            dados: Dados a serem salvos.
            nome_arquivo: Nome do arquivo de saída.

        Returns:
            Caminho do arquivo salvo.
        """
        caminho = OUTPUT_DIR / nome_arquivo
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
