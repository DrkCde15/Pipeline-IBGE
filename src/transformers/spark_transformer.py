"""
Transformador PySpark para dados públicos brasileiros.
Realiza transformações, limpeza e enriquecimento de dados coletados.
"""

import logging
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent.parent / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"


class SparkTransformer:
    """Transformador de dados usando PySpark."""

    def __init__(self, app_name: str = "PipelineDadosPublicos") -> None:
        self.spark = (
            SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.driver.memory", "2g")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(f"SparkSession criada: {app_name}")

    def ler_json(self, caminho: str | Path) -> DataFrame:
        """Lê um arquivo JSON como DataFrame Spark.

        Args:
            caminho: Caminho do arquivo JSON.

        Returns:
            DataFrame Spark com os dados.
        """
        caminho_str = str(caminho)
        logger.info(f"Lendo JSON: {caminho_str}")
        return self.spark.read.json(caminho_str, multiLine=True)

    def ler_csv(self, caminho: str | Path, delimiter: str = ";", header: bool = True) -> DataFrame:
        """Lê um arquivo CSV como DataFrame Spark.

        Args:
            caminho: Caminho do arquivo CSV.
            delimiter: Delimitador do CSV.
            header: Se o CSV possui cabeçalho.

        Returns:
            DataFrame Spark com os dados.
        """
        caminho_str = str(caminho)
        logger.info(f"Lendo CSV: {caminho_str}")
        return (
            self.spark.read
            .option("header", str(header).lower())
            .option("delimiter", delimiter)
            .option("inferSchema", "true")
            .csv(caminho_str)
        )

    def transformar_municipios(self, df: DataFrame) -> DataFrame:
        """Transforma dados de municípios do IBGE.

        - Normaliza colunas de texto (trim, uppercase)
        - Converte tipos de dados
        - Remove registros duplicados

        Args:
            df: DataFrame bruto de municípios.

        Returns:
            DataFrame transformado.
        """
        logger.info("Transformando dados de municípios...")

        df_transformado = (
            df
            .withColumn("id", F.col("id").cast(IntegerType()))
            .withColumn("nome", F.initcap(F.trim(F.col("nome"))))
            .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla"))))
            .withColumn("regiao_nome", F.initcap(F.trim(F.col("regiao-nome"))))
            .withColumn("regiao_id", F.col("regiao-id").cast(IntegerType()))
            .withColumn("microrregiao_id", F.col("microrregiao-id").cast(IntegerType()))
            .withColumn("microrregiao_nome", F.initcap(F.trim(F.col("microrregiao-nome"))))
            .drop("sigla", "regiao-nome", "regiao-id", "microrregiao-id", "microrregiao-nome")
            .dropDuplicates(["id"])
            .orderBy("nome")
        )

        total = df_transformado.count()
        logger.info(f"Municípios transformados: {total}")
        return df_transformado

    def transformar_estados(self, df: DataFrame) -> DataFrame:
        """Transforma dados de estados do IBGE.

        Args:
            df: DataFrame bruto de estados.

        Returns:
            DataFrame transformado.
        """
        logger.info("Transformando dados de estados...")

        df_transformado = (
            df
            .withColumn("id", F.col("id").cast(IntegerType()))
            .withColumn("sigla", F.upper(F.trim(F.col("sigla"))))
            .withColumn("nome", F.initcap(F.trim(F.col("nome"))))
            .withColumn("regiao_id", F.col("regiao-id").cast(IntegerType()))
            .withColumn("regiao_nome", F.initcap(F.trim(F.col("regiao-nome"))))
            .drop("regiao-id", "regiao-nome")
            .dropDuplicates(["id"])
            .orderBy("id")
        )

        total = df_transformado.count()
        logger.info(f"Estados transformados: {total}")
        return df_transformado

    def transformar_cnpjs(self, df: DataFrame) -> DataFrame:
        """Transforma dados de CNPJs da Receita Federal.

        - Limpa e valida CNPJs
        - Normaliza textos
        - Converte valores numéricos
        - Filtra apenas empresas ativas (opcional)

        Args:
            df: DataFrame bruto de CNPJs.

        Returns:
            DataFrame transformado.
        """
        logger.info("Transformando dados de CNPJs...")

        # Schema esperado para validação
        df_transformado = (
            df
            .withColumn("cnpj", F.regexp_replace(F.col("cnpj"), r"[^\d]", ""))
            .withColumn("cnpj", F.lpad(F.col("cnpj"), 14, "0"))
            .withColumn("razao_social", F.initcap(F.trim(F.col("razao_social"))))
            .withColumn("nome_fantasia", F.initcap(F.trim(F.col("nome_fantasia"))))
            .withColumn("capital_social", F.col("capital_social").cast(DoubleType()))
            .withColumn(
                "data_abertura",
                F.to_date(F.col("data_abertura"), "dd/MM/yyyy"),
            )
            .withColumn(
                "data_situacao_cadastral",
                F.to_date(F.col("data_situacao_cadastral"), "dd/MM/yyyy"),
            )
            .withColumn("uf", F.upper(F.trim(F.col("uf"))))
            .withColumn("cep", F.regexp_replace(F.col("cep"), r"[^\d]", ""))
            .withColumn("cep", F.lpad(F.col("cep"), 8, "0"))
            .withColumn("eh_ativa", F.when(F.col("situacao_cadastral") == "ATIVA", True).otherwise(False))
            .dropDuplicates(["cnpj"])
        )

        total = df_transformado.count()
        ativas = df_transformado.filter(F.col("eh_ativa")).count()
        logger.info(f"CNPJs transformados: {total} ({ativas} ativos)")
        return df_transformado

    def calcular_estatisticas_cnpj(self, df: DataFrame) -> DataFrame:
        """Calcula estatísticas por estado a partir dos dados de CNPJ.

        Args:
            df: DataFrame de CNPJs transformados.

        Returns:
            DataFrame com estatísticas por estado.
        """
        logger.info("Calculando estatísticas por estado...")

        df_estatisticas = (
            df
            .groupBy("uf")
            .agg(
                F.count("*").alias("total_empresas"),
                F.sum(F.when(F.col("eh_ativa"), 1).otherwise(0)).alias("empresas_ativas"),
                F.round(F.avg("capital_social"), 2).alias("capital_social_medio"),
                F.round(F.sum("capital_social"), 2).alias("capital_social_total"),
                F.countDistinct("atividade_principal").alias("num_atividades"),
                F.min("data_abertura").alias("empresa_mais_antiga"),
                F.max("data_abertura").alias("empresa_mais_recente"),
            )
            .withColumn(
                "percentual_ativas",
                F.round((F.col("empresas_ativas") / F.col("total_empresas")) * 100, 2),
            )
            .orderBy(F.desc("total_empresas"))
        )

        return df_estatisticas

    def filtrar_por_estado(self, df: DataFrame, uf: str) -> DataFrame:
        """Filtra dados por Unidade Federativa.

        Args:
            df: DataFrame a ser filtrado.
            uf: Sigla do estado (ex: "RJ", "SP").

        Returns:
            DataFrame filtrado.
        """
        return df.filter(F.upper(F.col("uf")) == uf.upper())

    def filtrar_por_situacao(self, df: DataFrame, situacao: str = "ATIVA") -> DataFrame:
        """Filtra empresas por situação cadastral.

        Args:
            df: DataFrame de CNPJs.
            situacao: Situação cadastral desejada.

        Returns:
            DataFrame filtrado.
        """
        return df.filter(F.upper(F.col("situacao_cadastral")) == situacao.upper())

    def salvar_parquet(self, df: DataFrame, nome: str) -> Path:
        """Salva DataFrame em formato Parquet.

        Args:
            df: DataFrame a ser salvo.
            nome: Nome do arquivo/diretório Parquet.

        Returns:
            Caminho do arquivo salvo.
        """
        caminho = PROCESSED_DIR / nome
        caminho_str = str(caminho)
        df.write.mode("overwrite").parquet(caminho_str)
        logger.info(f"Parquet salvo em: {caminho_str}")
        return caminho

    def salvar_csv(self, df: DataFrame, nome: str) -> Path:
        """Salva DataFrame em formato CSV.

        Args:
            df: DataFrame a ser salvo.
            nome: Nome do arquivo CSV.

        Returns:
            Caminho do arquivo salvo.
        """
        caminho = PROCESSED_DIR / nome
        caminho_str = str(caminho)
        (
            df.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .option("delimiter", ";")
            .csv(caminho_str)
        )
        logger.info(f"CSV salvo em: {caminho_str}")
        return caminho

    def executar_pipeline_ibge(self, dir_ibge: Path | None = None) -> None:
        """Executa pipeline completo de transformação para dados IBGE.

        Args:
            dir_ibge: Diretório com dados brutos do IBGE.
        """
        ibge_dir = dir_ibge or RAW_DIR / "ibge"
        logger.info("=== Início do pipeline de transformação IBGE ===")

        # Transformar municípios
        caminho_municipios = ibge_dir / "municipios.json"
        if caminho_municipios.exists():
            df_municipios = self.ler_json(caminho_municipios)
            df_municipios = self.transformar_municipios(df_municipios)
            self.salvar_parquet(df_municipios, "municipios")
            self.salvar_csv(df_municipios, "municipios.csv")
            df_municipios.show(10, truncate=False)

        # Transformar estados
        caminho_estados = ibge_dir / "estados.json"
        if caminho_estados.exists():
            df_estados = self.ler_json(caminho_estados)
            df_estados = self.transformar_estados(df_estados)
            self.salvar_parquet(df_estados, "estados")
            self.salvar_csv(df_estados, "estados.csv")
            df_estados.show(10, truncate=False)

        logger.info("=== Fim do pipeline de transformação IBGE ===")

    def executar_pipeline_cnpj(self, dir_cnpj: Path | None = None) -> None:
        """Executa pipeline completo de transformação para dados de CNPJ.

        Args:
            dir_cnpj: Diretório com dados brutos de CNPJ.
        """
        cnpj_dir = dir_cnpj or RAW_DIR / "cnpj"
        logger.info("=== Início do pipeline de transformação CNPJ ===")

        # Buscar o arquivo de CNPJ mais recente
        arquivos = sorted(cnpj_dir.glob("cnpjs_*.json"), reverse=True)
        if not arquivos:
            logger.warning("Nenhum arquivo de CNPJ encontrado")
            return

        caminho_cnpjs = arquivos[0]
        logger.info(f"Processando arquivo: {caminho_cnpjs.name}")

        df_cnpjs = self.ler_json(caminho_cnpjs)
        df_cnpjs = self.transformar_cnpjs(df_cnpjs)

        # Salvar dados transformados
        self.salvar_parquet(df_cnpjs, "cnpjs")
        self.salvar_csv(df_cnpjs, "cnpjs.csv")

        # Calcular e salvar estatísticas
        df_estatisticas = self.calcular_estatisticas_cnpj(df_cnpjs)
        self.salvar_parquet(df_estatisticas, "estatisticas_cnpj_por_estado")
        self.salvar_csv(df_estatisticas, "estatisticas_cnpj_por_estado.csv")

        df_estatisticas.show(27, truncate=False)

        logger.info("=== Fim do pipeline de transformação CNPJ ===")

    def fechar(self) -> None:
        """Encerra a sessão Spark."""
        if self.spark:
            self.spark.stop()
            logger.info("Sessão Spark encerrada")


def main() -> None:
    """Função principal para executar as transformações."""
    transformer = SparkTransformer()

    try:
        # Executar transformações IBGE
        transformer.executar_pipeline_ibge()

        # Executar transformações CNPJ
        transformer.executar_pipeline_cnpj()
    finally:
        transformer.fechar()


if __name__ == "__main__":
    main()
