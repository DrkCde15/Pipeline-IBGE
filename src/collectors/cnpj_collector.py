"""
Coletor de dados de CNPJ da Receita Federal.
Realiza a consulta de dados cadastrais de empresas brasileiras.
"""

import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Configurações
BASE_URL = "https://receitaws.com.br/v1"
RATE_LIMIT_DELAY = 3  # Segundos entre requisições
MAX_RETRIES = 3
RETRY_DELAY = 5

OUTPUT_DIR = Path(__file__).parent.parent.parent / "data" / "raw" / "cnpj"


class CNPJCollector:
    """Coletor de dados de CNPJ via API da Receita Federal."""

    def __init__(self, rate_limit: int = RATE_LIMIT_DELAY) -> None:
        self.base_url = BASE_URL
        self.rate_limit = rate_limit
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "PipelineDadosPublicos/1.0",
        })
        self._ultimo_request: datetime | None = None
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    def _respeitar_rate_limit(self) -> None:
        """Respeita o limite de requisições por minuto da API."""
        if self._ultimo_request is not None:
            tempo_decorrido = (datetime.now() - self._ultimo_request).total_seconds()
            if tempo_decorrido < self.rate_limit:
                tempo_espera = self.rate_limit - tempo_decorrido
                logger.debug(f"Aguardando {tempo_espera:.1f}s para respeitar rate limit")
                time.sleep(tempo_espera)
        self._ultimo_request = datetime.now()

    def _request_with_retry(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Realiza requisição com retry e tratamento de erros."""
        self._respeitar_rate_limit()
        url = f"{self.base_url}/{endpoint}"

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info(f"Consultando CNPJ: {endpoint} (tentativa {attempt}/{MAX_RETRIES})")
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                dados = response.json()

                # Verificar se a API retornou erro
                if "status" in dados and dados["status"] == "ERROR":
                    logger.warning(f"API retornou erro: {dados.get('message', 'Desconhecido')}")
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_DELAY * attempt)
                    continue

                return dados

            except requests.exceptions.Timeout:
                logger.warning(f"Timeout na requisição (tentativa {attempt})")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY * attempt)
            except requests.exceptions.HTTPError as e:
                logger.error(f"Erro HTTP {response.status_code}: {e}")
                if response.status_code == 429:  # Rate limit excedido
                    logger.warning("Rate limit excedido, aguardando 60 segundos...")
                    time.sleep(60)
                elif attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY * attempt)
            except requests.exceptions.RequestException as e:
                logger.error(f"Erro de conexão: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY * attempt)

        logger.error(f"Falha após {MAX_RETRIES} tentativas: {url}")
        return {}

    def consultar_cnpj(self, cnpj: str) -> dict[str, Any]:
        """Consulta dados de um CNPJ específico.

        Args:
            cnpj: Número do CNPJ (apenas dígitos, 14 caracteres).

        Returns:
            Dict com os dados cadastrais da empresa.
        """
        cnpj_limpo = "".join(filter(str.isdigit, cnpj))
        if len(cnpj_limpo) != 14:
            raise ValueError(f"CNPJ inválido: {cnpj}. Deve conter 14 dígitos.")

        return self._request_with_retry(f"cnjp/{cnpj_limpo}")

    def consultar_cnpjs(self, cnpjs: list[str]) -> list[dict[str, Any]]:
        """Consulta múltiplos CNPJs.

        Args:
            cnpjs: Lista de CNPJs a serem consultados.

        Returns:
            Lista de dicts com dados cadastrais.
        """
        resultados: list[dict[str, Any]] = []
        total = len(cnpjs)

        logger.info(f"Consultando {total} CNPJs...")

        for i, cnpj in enumerate(cnpjs, 1):
            logger.info(f"Progresso: {i}/{total} ({i/total*100:.1f}%)")

            try:
                dados = self.consultar_cnpj(cnpj)
                if dados:
                    dados["_cnpj_consultado"] = cnpj
                    dados["_data_consulta"] = datetime.now().isoformat()
                    resultados.append(dados)
            except ValueError as e:
                logger.warning(f"Skipping CNPJ inválido: {e}")
            except Exception as e:
                logger.error(f"Erro ao consultar CNPJ {cnpj}: {e}")

        logger.info(f"Total de CNPJs consultados com sucesso: {len(resultados)}/{total}")
        return resultados

    def extrair_dados_principais(self, dados: dict[str, Any]) -> dict[str, Any]:
        """Extrai e normaliza os campos principais dos dados do CNPJ.

        Args:
            dados: Dados brutos retornados pela API.

        Returns:
            Dict com campos normalizados.
        """
        return {
            "cnpj": dados.get("cnpj", ""),
            "razao_social": dados.get("nome", ""),
            "nome_fantasia": dados.get("fantasia", ""),
            "situacao_cadastral": dados.get("situacao", ""),
            "data_situacao_cadastral": dados.get("data_situacao_cadastral", ""),
            "motivo_situacao_cadastral": dados.get("motivo_situacao_cadastral", ""),
            "tipo_juridico": dados.get("tipo", ""),
            "porte": dados.get("porte", ""),
            "capital_social": dados.get("capital_social", 0),
            "natureza_juridica": dados.get("natureza_juridica", ""),
            "logradouro": dados.get("logradouro", ""),
            "numero": dados.get("numero", ""),
            "complemento": dados.get("complemento", ""),
            "bairro": dados.get("bairro", ""),
            "cep": dados.get("cep", ""),
            "municipio": dados.get("municipio", ""),
            "uf": dados.get("uf", ""),
            "email": dados.get("email", ""),
            "telefone1": dados.get("telefone1", ""),
            "telefone2": dados.get("telefone2", ""),
            "data_abertura": dados.get("abertura", ""),
            "ultima_atualizacao": dados.get("ultima_atualizacao", ""),
            "atividade_principal": dados.get("atividade_principal", [{}])[0].get("text", "") if dados.get("atividade_principal") else "",
            "atividades_secundarias": json.dumps(
                [a.get("text", "") for a in dados.get("atividades_secundarias", [])],
                ensure_ascii=False,
            ),
            "socios": json.dumps(
                [s.get("nome", "") for s in dados.get("qsa", [])],
                ensure_ascii=False,
            ),
        }

    def salvar_json(self, dados: list[dict[str, Any]] | dict[str, Any], nome_arquivo: str) -> Path:
        """Salva os dados em formato JSON.

        Args:
            dados: Dados a serem salvos.
            nome_arquivo: Nome do arquivo de saída.

        Returns:
            Caminho do arquivo salvo.
        """
        caminho = OUTPUT_DIR / 'cnpj.json'  # Nome fixo para o arquivo JSON
        with open(caminho, "w", encoding="utf-8") as f:
            json.dump(dados, f, ensure_ascii=False, indent=2)
        logger.info(f"Dados salvos em: {caminho}")
        return caminho

    def executar_coleta(self, cnpjs: list[str]) -> Path:
        """Executa a coleta de dados para uma lista de CNPJs.

        Args:
            cnpjs: Lista de CNPJs a serem consultados.

        Returns:
            Caminho do arquivo JSON salvo.
        """
        logger.info("=== Início da coleta de CNPJs ===")

        dados_brutos = self.consultar_cnpjs(cnpjs)
        dados_normalizados = [self.extrair_dados_principais(d) for d in dados_brutos]

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        arquivo = self.salvar_json(dados_normalizados, f"cnpjs_{timestamp}.json")

        logger.info("=== Fim da coleta de CNPJs ===")
        return arquivo


def main() -> None:
    """Função principal para executar a coleta de CNPJs de exemplo."""
    collector = CNPJCollector()

    # Exemplo: consulta de CNPJs públicos conhecidos
    cnpjs_exemplo = [
        "00000000000191",  # Banco do Brasil
        "00360305000104",  # Caixa Econômica
        "00000000000191",  # Petrobras (exemplo duplicado)
    ]

    # Remover duplicatas
    cnpjs_unicos = list(dict.fromkeys(cnpjs_exemplo))
    print(f"Consultando {len(cnpjs_unicos)} CNPJs de exemplo...")

    arquivo = collector.executar_coleta(cnpjs_unicos)
    print(f"Dados salvos em: {arquivo}")


if __name__ == "__main__":
    main()
