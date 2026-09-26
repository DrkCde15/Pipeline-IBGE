"""
DAG do Airflow para pipeline de dados de CNPJ (Receita Federal).
Orquestra a coleta, transformação e carga de dados cadastrais de empresas.
"""

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from common import PROJECT_DIR, criar_engine

DEFAULT_ARGS = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

# CNPJs públicos de exemplo (sem duplicatas)
CNPJS_EXEMPLO = [
    "00000000000191",  # Banco do Brasil
    "00360305000104",  # Caixa Econômica Federal
]


def coletar_cnpjs_exemplo() -> None:
    """Executa a coleta de CNPJs de exemplo."""
    from src.collectors.cnpj_collector import CNPJCollector

    collector = CNPJCollector()
    collector.executar_coleta(CNPJS_EXEMPLO)


def transformar_cnpj() -> None:
    """Executa a transformação dos dados de CNPJ com PySpark."""
    from src.transformers.spark_transformer import SparkTransformer

    transformer = SparkTransformer()
    try:
        transformer.executar_pipeline_cnpj()
    finally:
        transformer.fechar()


def carregar_postgres_cnpj() -> None:
    """Carrega dados CNPJ transformados na tabela landing do PostgreSQL."""
    import pandas as pd

    engine = criar_engine()
    processed_dir = f"{PROJECT_DIR}/data/processed"

    try:
        df_cnpjs = pd.read_parquet(f"{processed_dir}/cnpjs")
        df_cnpjs.to_sql("empresas_raw", engine, schema="cnpj", if_exists="append", index=False)
        print(f"CNPJs carregados: {len(df_cnpjs)} registros")
    except Exception as e:
        print(f"Aviso: Não foi possível carregar CNPJs - {e}")


with DAG(
    dag_id="pipeline_dados_cnpj",
    default_args=DEFAULT_ARGS,
    description="Pipeline de dados de CNPJ - Coleta, Transformação e Carga",
    schedule="0 6 * * 1",  # Segunda-feira às 6h
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["cnpj", "receita_federal", "dados_publicos"],
) as dag:

    tarefa_coleta_cnpj = PythonOperator(
        task_id="coletar_cnpjs",
        python_callable=coletar_cnpjs_exemplo,
        doc="Coleta dados de CNPJs via API da Receita Federal",
    )

    tarefa_transformacao_cnpj = PythonOperator(
        task_id="transformar_dados_cnpj",
        python_callable=transformar_cnpj,
        doc="Transforma e limpa dados de CNPJ com PySpark",
    )

    tarefa_carga_cnpj = PythonOperator(
        task_id="carregar_cnpj_postgres",
        python_callable=carregar_postgres_cnpj,
        doc="Carrega dados de CNPJ transformados no PostgreSQL",
    )

    tarefa_coleta_cnpj >> tarefa_transformacao_cnpj >> tarefa_carga_cnpj
