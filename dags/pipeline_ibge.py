"""
DAG do Airflow para pipeline de dados IBGE.
Orquestra a coleta, transformação e carga de dados do IBGE.
"""

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from common import PROJECT_DIR, criar_engine

# Configurações padrão da DAG
DEFAULT_ARGS = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


def coletar_ibge() -> None:
    """Executa a coleta de dados do IBGE."""
    from src.collectors.ibge_collector import IBGECollector

    collector = IBGECollector()
    collector.executar_coleta_completa()


def transformar_ibge() -> None:
    """Executa a transformação dos dados IBGE com PySpark."""
    from src.transformers.spark_transformer import SparkTransformer

    transformer = SparkTransformer()
    try:
        transformer.executar_pipeline_ibge()
    finally:
        transformer.fechar()


def carregar_postgres_ibge() -> None:
    """Carrega dados IBGE transformados nas tabelas landing do PostgreSQL."""
    import pandas as pd

    engine = criar_engine()
    processed_dir = f"{PROJECT_DIR}/data/processed"

    try:
        df_municipios = pd.read_parquet(f"{processed_dir}/municipios")
        df_municipios.to_sql("municipios_raw", engine, schema="ibge", if_exists="append", index=False)
        print(f"Municípios carregados: {len(df_municipios)} registros")
    except Exception as e:
        print(f"Aviso: Não foi possível carregar municípios - {e}")

    try:
        df_estados = pd.read_parquet(f"{processed_dir}/estados")
        df_estados.to_sql("estados_raw", engine, schema="ibge", if_exists="append", index=False)
        print(f"Estados carregados: {len(df_estados)} registros")
    except Exception as e:
        print(f"Aviso: Não foi possível carregar estados - {e}")


with DAG(
    dag_id="pipeline_dados_ibge",
    default_args=DEFAULT_ARGS,
    description="Pipeline completo de dados IBGE - Coleta, Transformação e Carga",
    schedule="@daily",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["ibge", "dados_publicos", "pipeline"],
    doc_md="""
    ## Pipeline de Dados IBGE

    Este pipeline executa:
    1. **Coleta** de dados do IBGE via API
    2. **Transformação** com PySpark
    3. **Carga** no PostgreSQL (tabelas landing `ibge.*_raw`)
    """,
) as dag:

    tarefa_coleta_ibge = PythonOperator(
        task_id="coletar_dados_ibge",
        python_callable=coletar_ibge,
        doc="Coleta dados de estados e municípios do IBGE",
    )

    tarefa_transformacao_ibge = PythonOperator(
        task_id="transformar_dados_ibge",
        python_callable=transformar_ibge,
        doc="Transforma e limpa dados IBGE com PySpark",
    )

    tarefa_carga_ibge = PythonOperator(
        task_id="carregar_ibge_postgres",
        python_callable=carregar_postgres_ibge,
        doc="Carrega dados transformados no PostgreSQL",
    )

    tarefa_coleta_ibge >> tarefa_transformacao_ibge >> tarefa_carga_ibge
