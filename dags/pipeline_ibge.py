"""
DAG do Airflow para pipeline de dados IBGE.
Orquestra a coleta, transformação e carga de dados do IBGE.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

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

# Caminhos do projeto
PROJECT_DIR = "/home/julio-cesar/Documents/Data-Projects/05-pipeline-dados-publicos"


def coletar_ibge() -> None:
    """Executa a coleta de dados do IBGE."""
    from src.collectors.ibge_collector import IBGECollector

    collector = IBGECollector()
    collector.executar_coleta_completa()


def coletar_cnpjs_exemplo() -> None:
    """Executa a coleta de CNPJs de exemplo."""
    from src.collectors.cnpj_collector import CNPJCollector

    collector = CNPJCollector()
    cnpjs_exemplo = [
        "00000000000191",
        "00360305000104",
    ]
    collector.executar_coleta(cnpjs_exemplo)


def transformar_ibge() -> None:
    """Executa a transformação dos dados IBGE com PySpark."""
    from src.transformers.spark_transformer import SparkTransformer

    transformer = SparkTransformer()
    try:
        transformer.executar_pipeline_ibge()
    finally:
        transformer.fechar()


def transformar_cnpj() -> None:
    """Executa a transformação dos dados de CNPJ com PySpark."""
    from src.transformers.spark_transformer import SparkTransformer

    transformer = SparkTransformer()
    try:
        transformer.executar_pipeline_cnpj()
    finally:
        transformer.fechar()


def carregar_postgres_ibge() -> None:
    """Carrega dados IBGE transformados no PostgreSQL."""
    import pandas as pd
    from sqlalchemy import create_engine

    from dotenv import load_dotenv
    import os

    load_dotenv()

    engine = create_engine(
        f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}"
        f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}"
    )

    processed_dir = f"{PROJECT_DIR}/data/processed"

    try:
        df_municipios = pd.read_parquet(f"{processed_dir}/municipios")
        df_municipios.to_sql("municipios", engine, schema="ibge", if_exists="replace", index=False)
        print(f"Municípios carregados: {len(df_municipios)} registros")
    except Exception as e:
        print(f"Aviso: Não foi possível carregar municípios - {e}")

    try:
        df_estados = pd.read_parquet(f"{processed_dir}/estados")
        df_estados.to_sql("estados", engine, schema="ibge", if_exists="replace", index=False)
        print(f"Estados carregados: {len(df_estados)} registros")
    except Exception as e:
        print(f"Aviso: Não foi possível carregar estados - {e}")


def carregar_postgres_cnpj() -> None:
    """Carrega dados CNPJ transformados no PostgreSQL."""
    import pandas as pd
    from sqlalchemy import create_engine

    from dotenv import load_dotenv
    import os

    load_dotenv()

    engine = create_engine(
        f"postgresql://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}"
        f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}"
    )

    processed_dir = f"{PROJECT_DIR}/data/processed"

    try:
        df_cnpjs = pd.read_parquet(f"{processed_dir}/cnpjs")
        df_cnpjs.to_sql("empresas_raw", engine, schema="cnpj", if_exists="replace", index=False)
        print(f"CNPJs carregados: {len(df_cnpjs)} registros")
    except Exception as e:
        print(f"Aviso: Não foi possível carregar CNPJs - {e}")


# ============================================================
# Definição da DAG principal: pipeline_ibge
# ============================================================

with DAG(
    dag_id="pipeline_dados_ibge",
    default_args=DEFAULT_ARGS,
    description="Pipeline completo de dados IBGE - Coleta, Transformação e Carga",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ibge", "dados_publicos", "pipeline"],
    doc_md="""
    ## Pipeline de Dados IBGE

    Este pipeline executa:
    1. **Coleta** de dados do IBGE via API
    2. **Transformação** com PySpark
    3. **Carga** no PostgreSQL
    """,
) as dag_ibge:

    # Tarefa 1: Coletar dados do IBGE
    tarefa_coleta_ibge = PythonOperator(
        task_id="coletar_dados_ibge",
        python_callable=coletar_ibge,
        doc="Coleta dados de estados e municípios do IBGE",
    )

    # Tarefa 2: Transformar dados IBGE
    tarefa_transformacao_ibge = PythonOperator(
        task_id="transformar_dados_ibge",
        python_callable=transformar_ibge,
        doc="Transforma e limpa dados IBGE com PySpark",
    )

    # Tarefa 3: Carregar no PostgreSQL
    tarefa_carga_ibge = PythonOperator(
        task_id="carregar_ibge_postgres",
        python_callable=carregar_postgres_ibge,
        doc="Carrega dados transformados no PostgreSQL",
    )

    # Orquestração
    tarefa_coleta_ibge >> tarefa_transformacao_ibge >> tarefa_carga_ibge


# ============================================================
# Definição da DAG: pipeline_cnpj
# ============================================================

with DAG(
    dag_id="pipeline_dados_cnpj",
    default_args=DEFAULT_ARGS,
    description="Pipeline de dados de CNPJ - Coleta, Transformação e Carga",
    schedule_interval="0 6 * * 1",  # Segunda-feira às 6h
    start_date=days_ago(1),
    catchup=False,
    tags=["cnpj", "receita_federal", "dados_publicos"],
) as dag_cnpj:

    # Tarefa 1: Coletar CNPJs
    tarefa_coleta_cnpj = PythonOperator(
        task_id="coletar_cnpjs",
        python_callable=coletar_cnpjs_exemplo,
        doc="Coleta dados de CNPJs via API da Receita Federal",
    )

    # Tarefa 2: Transformar dados CNPJ
    tarefa_transformacao_cnpj = PythonOperator(
        task_id="transformar_dados_cnpj",
        python_callable=transformar_cnpj,
        doc="Transforma e limpa dados de CNPJ com PySpark",
    )

    # Tarefa 3: Carregar no PostgreSQL
    tarefa_carga_cnpj = PythonOperator(
        task_id="carregar_cnpj_postgres",
        python_callable=carregar_postgres_cnpj,
        doc="Carrega dados de CNPJ transformados no PostgreSQL",
    )

    # Orquestração
    tarefa_coleta_cnpj >> tarefa_transformacao_cnpj >> tarefa_carga_cnpj
