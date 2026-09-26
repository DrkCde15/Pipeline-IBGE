-- ============================================================
-- Schema do Banco de Dados - Pipeline de Dados Públicos
-- Banco: PostgreSQL
-- ============================================================

-- Criar banco de dados (executar como superusuário)
-- CREATE DATABASE dados_publicos;

-- Conectar no banco: \c dados_publicos

-- ============================================================
-- SCHEMA IBGE (landing do pipeline)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS ibge;

-- ============================================================
-- TABELAS LANDING (carga bruta do pipeline via pandas append)
-- Espelham o output do SparkTransformer; sem PKs para permitir recarga.
-- A modelagem normalizada acima é populada a partir destas.
-- ============================================================

CREATE TABLE IF NOT EXISTS ibge.estados_raw (
    id INTEGER NOT NULL,
    sigla VARCHAR(2),
    nome VARCHAR(100),
    regiao_id INTEGER,
    regiao_sigla VARCHAR(2),
    regiao_nome VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ibge.municipios_raw (
    id INTEGER NOT NULL,
    nome VARCHAR(200),
    sigla_uf VARCHAR(2),
    uf_id INTEGER,
    uf_nome VARCHAR(100),
    regiao_id INTEGER,
    regiao_sigla VARCHAR(2),
    regiao_nome VARCHAR(100),
    mesorregiao_id INTEGER,
    mesorregiao_nome VARCHAR(100),
    microrregiao_id INTEGER,
    microrregiao_nome VARCHAR(100),
    regiao_imediata_id INTEGER,
    regiao_imediata_nome VARCHAR(200),
    regiao_intermediaria_id INTEGER,
    regiao_intermediaria_nome VARCHAR(200),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_estados_raw_id ON ibge.estados_raw(id);
CREATE INDEX IF NOT EXISTS idx_municipios_raw_id ON ibge.municipios_raw(id);
CREATE INDEX IF NOT EXISTS idx_municipios_raw_uf ON ibge.municipios_raw(sigla_uf);

-- Guarda de idempotência por dia (A3): a carga faz DELETE + INSERT atômico
-- por dia, e estes índices impedem duplicata silenciosa dentro do mesmo dia.
CREATE UNIQUE INDEX IF NOT EXISTS uq_estados_raw_id_dia ON ibge.estados_raw(id, (loaded_at::date));
CREATE UNIQUE INDEX IF NOT EXISTS uq_municipios_raw_id_dia ON ibge.municipios_raw(id, (loaded_at::date));

-- ============================================================
-- COMENTÁRIOS NAS TABELAS
-- ============================================================

COMMENT ON TABLE ibge.estados_raw IS 'Landing de estados do IBGE (carga diária idempotente)';
COMMENT ON TABLE ibge.municipios_raw IS 'Landing de municípios do IBGE (carga diária idempotente)';
