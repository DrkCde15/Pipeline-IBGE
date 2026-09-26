# Pipeline de Dados IBGE

Pipeline completo para ingestão, transformação e armazenamento de dados públicos brasileiros provenientes de APIs governamentais como IBGE e Receita Federal.

## 🎯 Objetivo

Automatizar a coleta de dados públicos brasileiros, aplicar transformações com PySpark e armazenar em PostgreSQL para análises e consultas.

## 📦 Stack Tecnológica

- **Linguagem:** Python 3.10+
- **Orquestração:** Apache Airflow
- **Processamento:** PySpark
- **Banco de Dados:** PostgreSQL
- **APIs:** IBGE, Receita Federal (CNPJ)

## 📁 Estrutura do Projeto

```
pipeline-dados-ibge/
├── README.md
├── requirements.txt
├── .env.example
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
│   └── pipeline_ibge.py
├── notebooks/
│   └── 01_exploracao_dados_publicos.ipynb
└── tests/
    └── __init__.py
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

### 3. Criar schema no PostgreSQL

```bash
psql -U postgres -f sql/create_schema.sql
```

### 4. Executar collectores manualmente

```bash
python -m src.collectors.ibge_collector
python -m src.collectors.cnpj_collector
```

### 5. Executar transformação PySpark

```bash
python -m src.transformers.spark_transformer
```

### 6. Iniciar Airflow (opcional)

```bash
airflow db migrate
airflow scheduler &
airflow webserver &
```

## 📊 Dados Coletados

### IBGE
- Indicadores sociodemográficos por município
- PIB municipal
- Dados censitários

### Receita Federal
- Dados de empresas (CNPJ)
- Situação cadastral
- Atividades econômicas (CNAE)

## 🔒 Segurança

- Nunca commite o arquivo `.env`
- Credenciais devem ser gerenciadas via variáveis de ambiente
- Dados públicos não possuem restrição de acesso
