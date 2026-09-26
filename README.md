# Pipeline de Dados Públicos Brasileiros

Pipeline completo para ingestão, transformação e armazenamento de dados públicos brasileiros provenientes da API do IBGE.

## 🎯 Objetivo

Automatizar a coleta de dados do IBGE, aplicar transformações com PySpark e armazenar em PostgreSQL para análises e consultas.

## 📦 Stack Tecnológica

- **Linguagem:** Python 3.10+ (container: Python 3.12)
- **Orquestração:** Apache Airflow 2.11.2 (última série 2.x, EOL abr/2026)
- **Processamento:** PySpark 3.5+
- **Data Quality:** Pandera (portão pós-coleta + portão pós-transformação)
- **Banco de Dados:** PostgreSQL
- **APIs:** IBGE (Localidades v1 + Agregados SIDRA v3)

## 📁 Estrutura do Projeto

```
05-pipeline-dados-publicos/
├── README.md
├── requirements.txt
├── docker-compose.yml
├── .env.example
├── .gitignore
├── 00-create-dados-publicos.sh
├── src/
│   ├── __init__.py
│   ├── collectors/
│   │   └── ibge_collector.py
│   ├── transformers/
│   │   └── spark_transformer.py
│   └── loaders/
│       └── postgres_loader.py
│   └── validation/
│       └── schemas.py
├── sql/
│   └── create_schema.sql
├── airflow/
│   ├── Dockerfile
│   └── dags/
│       ├── __init__.py
│       ├── common.py
│       └── pipeline_ibge.py
├── notebooks/
│   └── 01_exploracao_dados_publicos.ipynb
├── docs/
│   └── revisao-engenharia-dados.md
└── tests/
    ├── __init__.py
    ├── test_ibge_collector.py
    ├── test_postgres_loader.py
    └── test_validation.py
```

## 🚀 Setup

### 1. Instalar dependências

```bash
pip install -r requirements.txt --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.12.txt
```
> Sem o `--constraint`, o pip resolve dependências incompatíveis do Airflow 2 e a instalação quebra. Ajuste o `3.12` para a sua versão local (`python3 --version`).

### 2. Configurar variáveis de ambiente

```bash
cp .env.example .env
# Editar .env com suas credenciais
```

| Variável | Descrição | Padrão |
|---|---|---|
| `PG_HOST` / `PG_PORT` / `PG_DATABASE` / `PG_USER` / `PG_PASSWORD` | Conexão PostgreSQL | `localhost:5432/dados_publicos` |
| `IBGE_API_BASE` | API de localidades (v1) | `https://servicodados.ibge.gov.br/api/v1` |
| `IBGE_AGREGADOS_BASE` | API de agregados SIDRA (v3) | `https://servicodados.ibge.gov.br/api/v3/agregados` |
| `IBGE_TIMEOUT` | Timeout das requisições (s) | `30` |
| `SPARK_MASTER` / `SPARK_APP_NAME` | Spark local | `local[*]` |
| `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` | Login da UI (DEV ONLY) | `admin`/`admin` |
| `AIRFLOW_UID` | UID dos containers (escrever em `./data`) | `1000` (seu `id -u`) |

> `AIRFLOW_UID` precisa ser o seu UID do host (`id -u`): sem isso o Spark no
> container não consegue sobrescrever `data/processed/` e a task de
> transformação falha com `Unable to clear output directory`.

### 3. Criar schema no PostgreSQL

```bash
createdb -U postgres dados_publicos
psql -U postgres -d dados_publicos -f sql/create_schema.sql
```

### 4. Executar o coletor manualmente

```bash
python -m src.collectors.ibge_collector
```

Exemplo de agregado SIDRA (população estimada do Brasil, 2024):

```python
from src.collectors.ibge_collector import IBGECollector

collector = IBGECollector()
dados = collector.buscar_agregado(6579, 2024, 9324, "N1[all]")
```

### 5. Executar transformação PySpark

```bash
python -m src.transformers.spark_transformer
```

### 6. Rodar testes

```bash
pytest tests/ -q
```

14 testes (coletores com HTTP mockado, carga com engine mockada, schemas de
DQ — sem rede nem banco).

### 7. Data quality (2 portões, `src/validation/schemas.py`)

- **Portão 1 (pós-coleta):** lote vazio ou sem campos estruturais (id/sigla,
  hierarquia até a UF) levanta `DataQualityError` antes de persistir o raw.
- **Portão 2 (pós-transformação):** schemas Pandera barram `id` duplicado ou
  nulo, `sigla_uf` fora das 27 UFs e tipos errados — **antes** do DELETE, de
  modo que um lote reprovado nunca apaga o dado bom do dia.

### 8. Explorar no notebook

```bash
jupyter notebook notebooks/01_exploracao_dados_publicos.ipynb
```

O notebook lê `data/processed/` primeiro (parquet do transformer) e só usa o
raw como fallback. Sem dados coletados, a célula de carga falha alto em vez
de pular as análises em silêncio.

### 9. Iniciar Airflow

**Opção A — Podman (recomendado):**

```bash
cp .env.example .env   # se ainda não existe
podman compose up -d --build
```

Sobe Postgres (porta 5433 no host) + scheduler + webserver.
UI em http://localhost:8080 — login `admin`/`admin` (padrão de DEV;
troque com `AIRFLOW_ADMIN_USER`/`AIRFLOW_ADMIN_PASSWORD` antes do `up`).
As credenciais `PG_*` dentro do compose apontam para o banco do container;
fora dele vale o `.env` local.

Aplicar o schema no banco do container:

```bash
podman compose exec postgres psql -U postgres -d dados_publicos -f /schema/create_schema.sql
```

**Opção B — local:**

```bash
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/airflow/dags
airflow db upgrade
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
airflow scheduler &
airflow webserver &
```

Uma DAG: `pipeline_dados_ibge` (diária).
Ela executa coleta → transformação PySpark → carga nas tabelas landing
(`ibge.*_raw`) via `src/loaders/postgres_loader.py`.
A carga é idempotente por dia (DELETE + INSERT na mesma transação) e qualquer
falha reprova a task para acionar o retry — ver `sql/create_schema.sql`
(UNIQUE por dia nas `*_raw`).

## 📊 Dados Coletados

### IBGE — Localidades (v1)

- Regiões, estados, mesorregiões, microrregiões e municípios
- Distritos e subdistritos, regiões imediatas/intermediárias e metropolitanas

### IBGE — Agregados SIDRA (v3)

- Séries por tabela/período/variável e recorte territorial (ex.: tabela 6579, população estimada)

## 🔒 Segurança

- Nunca commite o arquivo `.env` (já coberto pelo `.gitignore`)
- Credenciais devem ser gerenciadas via variáveis de ambiente
- Dados públicos não possuem restrição de acesso
