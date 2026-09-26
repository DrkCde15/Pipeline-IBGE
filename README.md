# Pipeline de Dados Públicos Brasileiros

Pipeline completo para ingestão, transformação e armazenamento de dados públicos brasileiros provenientes de APIs governamentais como IBGE e Receita Federal.

## 🎯 Objetivo

Automatizar a coleta de dados públicos brasileiros, aplicar transformações com PySpark e armazenar em PostgreSQL para análises e consultas.

## 📦 Stack Tecnológica

- **Linguagem:** Python 3.10+
- **Orquestração:** Apache Airflow 2.8+
- **Processamento:** PySpark 3.5+
- **Banco de Dados:** PostgreSQL
- **APIs:** IBGE (Localidades v1 + Agregados SIDRA v3), Receita Federal (CNPJ via ReceitaWS)

## 📁 Estrutura do Projeto

```
05-pipeline-dados-publicos/
├── README.md
├── requirements.txt
├── .env.example
├── .gitignore
├── src/
│   ├── __init__.py
│   ├── collectors/
│   │   ├── ibge_collector.py
│   │   └── cnpj_collector.py
│   └── transformers/
│       └── spark_transformer.py
├── sql/
│   └── create_schema.sql
├── dags/
│   ├── __init__.py
│   ├── common.py
│   ├── pipeline_ibge.py
│   └── pipeline_cnpj.py
├── notebooks/
│   └── 01_exploracao_dados_publicos.ipynb
└── tests/
    ├── __init__.py
    ├── test_ibge_collector.py
    └── test_cnpj_collector.py
```

## 🚀 Setup

### 1. Instalar dependências

```bash
pip install -r requirements.txt
```

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
| `CNPJ_API_BASE` | API de CNPJ | `https://receitaws.com.br/v1/cnpj` |
| `CNPJ_API_KEY` | Token ReceitaWS (opcional) | vazio (3 req/min no plano gratuito) |
| `CNPJ_RATE_LIMIT` | Segundos entre requisições | `3` |

### 3. Criar schema no PostgreSQL

```bash
createdb -U postgres dados_publicos
psql -U postgres -d dados_publicos -f sql/create_schema.sql
```

### 4. Executar coletores manualmente

```bash
python -m src.collectors.ibge_collector
python -m src.collectors.cnpj_collector
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

### 7. Iniciar Airflow (opcional)

```bash
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags
airflow db migrate
airflow scheduler &
airflow api-server &
```

Duas DAGs: `pipeline_dados_ibge` (diária) e `pipeline_dados_cnpj` (segundas 6h).
Cada uma executa coleta → transformação PySpark → carga `append` nas tabelas landing (`ibge.*_raw`, `cnpj.empresas_raw`).

## 📊 Dados Coletados

### IBGE — Localidades (v1)

- Regiões, estados, mesorregiões, microrregiões e municípios
- Distritos e subdistritos, regiões imediatas/intermediárias e metropolitanas

### IBGE — Agregados SIDRA (v3)

- Séries por tabela/período/variável e recorte territorial (ex.: tabela 6579, população estimada)

### Receita Federal

- Dados de empresas (CNPJ)
- Situação cadastral
- Atividades econômicas (CNAE)

## 🔒 Segurança

- Nunca commite o arquivo `.env` (já coberto pelo `.gitignore`)
- Credenciais devem ser gerenciadas via variáveis de ambiente
- Dados públicos não possuem restrição de acesso
