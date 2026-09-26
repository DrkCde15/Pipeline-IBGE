"""
Transformador PySpark para dados públicos brasileiros.
Realiza transformações, limpeza e enriquecimento de dados coletados.
"""

import logging
from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType
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

        O JSON da API é aninhado (microrregiao > mesorregiao > UF > regiao),
        então as colunas são extraídas por caminho antes de normalizar.

        - Normaliza colunas de texto (trim, initcap/uppercase)
        - Converte tipos de dados
        - Remove registros duplicados

        Args:
            df: DataFrame bruto de municípios.

        Returns:
            DataFrame transformado.
        """
        logger.info("Transformando dados de municípios...")

        uf = "microrregiao.mesorregiao.UF"
        colunas = set(df.columns)

        def _nested_ou_nulo(caminho: str, tipo):
            """Extrai coluna aninhada ou retorna NULL tipado se ela não existir."""
            raiz = caminho.split(".")[0].strip("`")
            if raiz not in colunas:
                return F.lit(None).cast(tipo)
            return F.col(caminho).cast(tipo)

        df_transformado = (
            df
            .withColumn("id", F.col("id").cast(IntegerType()))
            .withColumn("nome", F.initcap(F.trim(F.col("nome"))))
            .withColumn("sigla_uf", F.upper(F.trim(F.col(f"{uf}.sigla"))))
            .withColumn("uf_id", F.col(f"{uf}.id").cast(IntegerType()))
            .withColumn("uf_nome", F.initcap(F.trim(F.col(f"{uf}.nome"))))
            .withColumn("regiao_id", F.col(f"{uf}.regiao.id").cast(IntegerType()))
            .withColumn("regiao_sigla", F.upper(F.trim(F.col(f"{uf}.regiao.sigla"))))
            .withColumn("regiao_nome", F.initcap(F.trim(F.col(f"{uf}.regiao.nome"))))
            .withColumn("mesorregiao_id", F.col("microrregiao.mesorregiao.id").cast(IntegerType()))
            .withColumn("mesorregiao_nome", F.initcap(F.trim(F.col("microrregiao.mesorregiao.nome"))))
            .withColumn("microrregiao_id", F.col("microrregiao.id").cast(IntegerType()))
            .withColumn("microrregiao_nome", F.initcap(F.trim(F.col("microrregiao.nome"))))
            .withColumn("regiao_imediata_id", _nested_ou_nulo("`regiao-imediata`.id", IntegerType()))
            .withColumn(
                "regiao_imediata_nome",
                F.initcap(F.trim(_nested_ou_nulo("`regiao-imediata`.nome", StringType()))),
            )
            .withColumn("regiao_intermediaria_id", _nested_ou_nulo("`regiao-intermediaria`.id", IntegerType()))
            .withColumn(
                "regiao_intermediaria_nome",
                F.initcap(F.trim(_nested_ou_nulo("`regiao-intermediaria`.nome", StringType()))),
            )
            .select(
                "id", "nome", "sigla_uf", "uf_id", "uf_nome",
                "regiao_id", "regiao_sigla", "regiao_nome",
                "mesorregiao_id", "mesorregiao_nome",
                "microrregiao_id", "microrregiao_nome",
                "regiao_imediata_id", "regiao_imediata_nome",
                "regiao_intermediaria_id", "regiao_intermediaria_nome",
            )
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
            .withColumn("regiao_id", F.col("regiao.id").cast(IntegerType()))
            .withColumn("regiao_sigla", F.upper(F.trim(F.col("regiao.sigla"))))
            .withColumn("regiao_nome", F.initcap(F.trim(F.col("regiao.nome"))))
            .select("id", "sigla", "nome", "regiao_id", "regiao_sigla", "regiao_nome")
            .dropDuplicates(["id"])
            .orderBy("id")
        )

        total = df_transformado.count()
        logger.info(f"Estados transformados: {total}")
        return df_transformado

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

    def fechar(self) -> None:
        """Encerra a sessão Spark."""
        if self.spark:
            self.spark.stop()
            logger.info("Sessão Spark encerrada")


def main() -> None:
    """Função principal para executar as transformações."""
    transformer = SparkTransformer()

    try:
        transformer.executar_pipeline_ibge()
    finally:
        transformer.fechar()


if __name__ == "__main__":
    main()
