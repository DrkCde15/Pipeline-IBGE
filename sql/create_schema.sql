-- ============================================================
-- Schema do Banco de Dados - Pipeline de Dados Públicos
-- Banco: PostgreSQL
-- ============================================================

-- Criar banco de dados (executar como superusuário)
-- CREATE DATABASE dados_publicos;

-- Conectar no banco: \c dados_publicos

-- ============================================================
-- SCHEMA IBGE
-- ============================================================

CREATE SCHEMA IF NOT EXISTS ibge;

-- Tabela de regiões
CREATE TABLE IF NOT EXISTS ibge.regioes (
    id INTEGER PRIMARY KEY,
    nome VARCHAR(50) NOT NULL,
    sigla VARCHAR(2)
);

-- Tabela de estados
CREATE TABLE IF NOT EXISTS ibge.estados (
    id INTEGER PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    sigla VARCHAR(2) NOT NULL UNIQUE,
    regiao_id INTEGER REFERENCES ibge.regioes(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de microrregiões
CREATE TABLE IF NOT EXISTS ibge.microrregioes (
    id INTEGER PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    estado_id INTEGER REFERENCES ibge.estados(id)
);

-- Tabela de municípios
CREATE TABLE IF NOT EXISTS ibge.municipios (
    id INTEGER PRIMARY KEY,
    nome VARCHAR(200) NOT NULL,
    sigla_uf VARCHAR(2) NOT NULL REFERENCES ibge.estados(sigla),
    microrregiao_id INTEGER REFERENCES ibge.microrregioes(id),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    altitude INTEGER,
    populacao BIGINT,
    area_km2 DECIMAL(15, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de indicadores
CREATE TABLE IF NOT EXISTS ibge.indicadores (
    id INTEGER PRIMARY KEY,
    nome VARCHAR(500) NOT NULL,
    descricao TEXT,
    categoria VARCHAR(200),
    periodo VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de valores de indicadores por município
CREATE TABLE IF NOT EXISTS ibge.indicadores_valores (
    id SERIAL PRIMARY KEY,
    indicador_id INTEGER NOT NULL REFERENCES ibge.indicadores(id),
    municipio_id INTEGER NOT NULL REFERENCES ibge.municipios(id),
    ano INTEGER NOT NULL,
    valor DECIMAL(20, 6),
    unidade VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(indicador_id, municipio_id, ano)
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_indicadores_valores_municipio ON ibge.indicadores_valores(municipio_id);
CREATE INDEX IF NOT EXISTS idx_indicadores_valores_ano ON ibge.indicadores_valores(ano);
CREATE INDEX IF NOT EXISTS idx_indicadores_valores_indicador ON ibge.indicadores_valores(indicador_id);
CREATE INDEX IF NOT EXISTS idx_municipios_uf ON ibge.municipios(sigla_uf);

-- ============================================================
-- SCHEMA CNPJ (RECEITA FEDERAL)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS cnpj;

-- Tabela de atividades econômicas (CNAE)
CREATE TABLE IF NOT EXISTS cnpj.cnae (
    id SERIAL PRIMARY KEY,
    codigo VARCHAR(7) NOT NULL UNIQUE,
    descricao TEXT NOT NULL,
    grupo VARCHAR(200),
    classe VARCHAR(200),
    subclasse VARCHAR(200),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de naturezas jurídicas
CREATE TABLE IF NOT EXISTS cnpj.naturezas_juridicas (
    id SERIAL PRIMARY KEY,
    codigo VARCHAR(10) NOT NULL UNIQUE,
    descricao TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de empresas (CNPJ)
CREATE TABLE IF NOT EXISTS cnpj.empresas (
    id SERIAL PRIMARY KEY,
    cnpj VARCHAR(14) NOT NULL UNIQUE,
    razao_social VARCHAR(300) NOT NULL,
    nome_fantasia VARCHAR(300),
    situacao_cadastral VARCHAR(20) NOT NULL,
    data_situacao_cadastral DATE,
    motivo_situacao_cadastral TEXT,
    tipo_juridico VARCHAR(10),
    porte VARCHAR(20),
    capital_social DECIMAL(18, 2),
    natureza_juridica_id INTEGER REFERENCES cnpj.naturezas_juridicas(id),
    data_abertura DATE,
    email VARCHAR(200),
    telefone1 VARCHAR(15),
    telefone2 VARCHAR(15),
    ultima_atualizacao TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de endereços das empresas
CREATE TABLE IF NOT EXISTS cnpj.enderecos (
    id SERIAL PRIMARY KEY,
    empresa_id INTEGER NOT NULL REFERENCES cnpj.empresas(id) ON DELETE CASCADE,
    logradouro VARCHAR(300),
    numero VARCHAR(20),
    complemento VARCHAR(100),
    bairro VARCHAR(100),
    cep VARCHAR(8),
    municipio VARCHAR(100),
    uf VARCHAR(2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de atividades principais das empresas
CREATE TABLE IF NOT EXISTS cnpj.empresas_atividades (
    id SERIAL PRIMARY KEY,
    empresa_id INTEGER NOT NULL REFERENCES cnpj.empresas(id) ON DELETE CASCADE,
    cnae_id INTEGER NOT NULL REFERENCES cnpj.cnae(id),
    principal BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(empresa_id, cnae_id)
);

-- Tabela de sócios
CREATE TABLE IF NOT EXISTS cnpj.socios (
    id SERIAL PRIMARY KEY,
    empresa_id INTEGER NOT NULL REFERENCES cnpj.empresas(id) ON DELETE CASCADE,
    nome VARCHAR(300) NOT NULL,
    qualificacao VARCHAR(100),
    data_entrada DATE,
    pais VARCHAR(100),
    cpf_representante VARCHAR(11),
    nome_representante VARCHAR(300),
    created_at TIMESTAMP DEFAULT TIMESTAMP
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_empresas_cnpj ON cnpj.empresas(cnpj);
CREATE INDEX IF NOT EXISTS idx_empresas_situacao ON cnpj.empresas(situacao_cadastral);
CREATE INDEX IF NOT EXISTS idx_empresas_uf ON cnpj.enderecos(uf);
CREATE INDEX IF NOT EXISTS idx_socios_empresa ON cnpj.socios(empresa_id);
CREATE INDEX IF NOT EXISTS idx_empresas_atividades_empresa ON cnpj.empresas_atividades(empresa_id);

-- ============================================================
-- VIEWS ÚTEIS
-- ============================================================

-- View: Resumo de empresas por estado
CREATE OR REPLACE VIEW cnpj.v_resumo_empresas_estado AS
SELECT
    e.uf,
    COUNT(*) AS total_empresas,
    SUM(CASE WHEN em.situacao_cadastral = 'ATIVA' THEN 1 ELSE 0 END) AS empresas_ativas,
    ROUND(AVG(em.capital_social), 2) AS capital_social_medio,
    SUM(em.capital_social) AS capital_social_total
FROM cnpj.enderecos e
JOIN cnpj.empresas em ON e.empresa_id = em.id
GROUP BY e.uf
ORDER BY total_empresas DESC;

-- View: Empresas com atividade principal
CREATE OR REPLACE VIEW cnpj.v_empresas_atividade AS
SELECT
    em.cnpj,
    em.razao_social,
    em.situacao_cadastral,
    c.descricao AS atividade_principal,
    em.capital_social,
    end.municipio,
    end.uf
FROM cnpj.empresas em
JOIN cnpj.empresas_atividades ea ON em.id = ea.empresa_id AND ea.principal = TRUE
JOIN cnpj.cnae c ON ea.cnae_id = c.id
JOIN cnpj.enderecos end ON em.id = end.empresa_id;

-- View: Municípios com indicadores consolidados
CREATE OR REPLACE VIEW ibge.v_municipios_indicadores AS
SELECT
    m.id,
    m.nome,
    m.sigla_uf,
    iv.indicador_id,
    i.nome AS indicador_nome,
    iv.ano,
    iv.valor,
    iv.unidade
FROM ibge.municipios m
LEFT JOIN ibge.indicadores_valores iv ON m.id = iv.municipio_id
LEFT JOIN ibge.indicadores i ON iv.indicador_id = i.id;

-- ============================================================
-- COMENTÁRIOS NAS TABELAS
-- ============================================================

COMMENT ON TABLE ibge.estados IS 'Estados brasileiros conforme divisão do IBGE';
COMMENT ON TABLE ibge.municipios IS 'Municípios brasileiros conforme divisão do IBGE';
COMMENT ON TABLE ibge.indicadores IS 'Indicadores disponíveis no IBGE';
COMMENT ON TABLE ibge.indicadores_valores IS 'Valores dos indicadores IBGE por município e ano';
COMMENT ON TABLE cnpj.empresas IS 'Dados cadastrais de empresas brasileiras (CNPJ)';
COMMENT ON TABLE cnpj.cnae IS 'Classificação Nacional de Atividades Econômicas';
COMMENT ON TABLE cnpj.socios IS 'Sócios das empresas brasileiras';
