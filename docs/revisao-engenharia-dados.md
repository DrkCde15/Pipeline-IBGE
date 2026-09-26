# Revisão de Engenharia de Dados — 05-pipeline-dados-publicos

**Data:** 26/09/2026
**Escopo:** repositório inteiro (2 coletores, transformer Spark, `sql/create_schema.sql`, 2 DAGs, notebook, tests, README, compose, git). Análise estática + `pytest` local. Spark/Postgres/Airflow não executados.
**Maturidade assumida:** Estágio 1 — estudo/portfólio, consumidor é o próprio autor via notebook/parquet. Sem CI, sem IaC além do compose local, sem evidência de produção.

> Método: skill `data-engineering-review`, calibrada pelo livro *Fundamentos da Engenharia de Dados* (Reis e Housley): ciclo de vida em cinco etapas + seis correntes subjacentes. Achados marcados como **verificado** (visto no código/ambiente) ou **inferido** (deduzido por ausência).

## Veredito

O esqueleto do ciclo de vida existe e está acima do típico estágio 1: raw JSON preservado, Parquet processado, landing modelada, 2 DAGs reais com retry/timeout, 7 testes passando. Porém o pipeline ponta-a-ponta **não fecha**: o ramo CNPJ escreve um arquivo que o transformer nunca lê (A1), e a carga engole exceção e marca sucesso no Airflow (A2). São os dois erros silenciosos que mais importam agora. Em seguida vem a decisão de modelagem pendente: metade do DDL (tabelas normalizadas + 3 views) nunca recebe dados.

## Mapa do ciclo de vida

| Etapa do ciclo | Onde está no repo | Tecnologia | Observação |
|---|---|---|---|
| Geração (fontes) | `src/collectors/ibge_collector.py`, `cnpj_collector.py` | IBGE v1/v3 + ReceitaWS (REST) | Sem contrato; retry/backoff ok; rate limit só no CNPJ |
| Armazenamento | `data/raw/`, `data/processed/`, `sql/create_schema.sql` | JSON + Parquet + Postgres | Raw imutável ok; `*_raw` sem PK/UK; normalizadas órfãs; `data/curated/` vazio |
| Ingestão | `airflow/dags/pipeline_ibge.py`, `pipeline_cnpj.py` | Airflow 3 (compose podman) | Batch diário/semanal; delete+insert intra-dia |
| Transformação | `src/transformers/spark_transformer.py` | PySpark `local[*]` | Código existe, zero testes, só validado por inspeção |
| Disponibilização | views em `create_schema.sql`, `notebooks/01_exploracao...ipynb` | Postgres views + Jupyter | Views sobre tabelas vazias; notebook pula análises em silêncio |

## Scorecard

| Dimensão | Nota (0–3) | Esperado no estágio | Resumo em uma linha |
|---|---|---|---|
| Geração | 2 | 1 | Rotas corretas, retry ok; sem contrato com fonte |
| Armazenamento | 2 | 1 | Raw + Parquet + landing; acima do esperado |
| Ingestão | 1 | 1 | Idempotente só no dia; falha engolida |
| Transformação | 1 | 1 | Funciona, sem testes e sem dono raw→modelado |
| Disponibilização | 1 | 1 | Views vazias; notebook não entrega o que promete |
| Segurança/privacidade | 1 | 1 | Sem segredo no git, mas segredos DEV hardcoded + LGPD aberta |
| Gerenciamento | 1 | 1 | README bom; sem dicionário nem testes de dados |
| DataOps | 1 | 1 | pytest local; sem CI, sem monitoramento |
| Arquitetura | 2 | 1 | Peças adequadas; Spark é custo de aprendizado consciente |
| Orquestração | 2 | 1 | DAGs reais com retry/timeout; sem SLA/alerta, sem `ds` |
| Eng. de software | 2 | 1 | Modular e tipado; sem lockfile/linter |

## Pontos fortes verificados

- Bruto preservado antes de transformar: `ibge_collector.py:124-138`, `cnpj_collector.py:202-216`, Spark usa `overwrite` só em `processed/` (`spark_transformer.py:286,302`).
- Retry com backoff nos dois coletores (`ibge_collector.py:46-69`, `cnpj_collector.py:70-111`) + tratamento de 429 com espera de 60s (`cnpj_collector.py:100-102`).
- `.env` fora do git (`.gitignore:2`, confirmado via `git ls-files` — nenhum `.env` nem `data/raw` versionado).
- 7 testes com mock passando, sem rede (verificado com `pytest tests/ -q`).
- `data/raw/ibge/` com 27 estados + `municipios.json` de ~4,5MB gerado localmente; `data/processed/` com Parquet de estados/municípios.

## Achados

### A1 Pipeline CNPJ quebrado: `salvar_json` ignora o nome e transformer nunca acha o arquivo — Crítica · Esforço P
- **Local:** `src/collectors/cnpj_collector.py:212` vs `src/collectors/cnpj_collector.py:233` vs `src/transformers/spark_transformer.py:352`
- **Status:** verificado
- **Evidência:** `salvar_json(dados, nome_arquivo)` faz `OUTPUT_DIR / 'cnpj.json'` fixo, ignorando o `f"cnpjs_{timestamp}.json"` passado em `executar_coleta`. O transformer só lê `cnpjs_*.json`. Resultado observado: `data/raw/cnpj/` vazio, `data/raw/ibge/` com dados — o ramo CNPJ nunca produz input válido e o transformer só loga `warning` e retorna.
- **Por que importa:** ingestão que parece verde e não entrega nada (erro silencioso clássico, cap. 7).
- **Como corrigir:** usar o parâmetro: `caminho = OUTPUT_DIR / nome_arquivo`. Teste: coletar 1 CNPJ e assertar que `glob("cnpjs_*.json")` encontra o arquivo.
- **Corrigido em 26/09/2026:** `salvar_json` passou a respeitar `nome_arquivo` ✅

### A2 Carga engole exceção e DAG marca sucesso mesmo sem carregar — Crítica · Esforço P
- **Local:** `airflow/dags/pipeline_ibge.py:62-63,69-72`, `airflow/dags/pipeline_cnpj.py:66-67`
- **Status:** verificado
- **Evidência:** `try: ... to_sql(...) except Exception as e: print(f"Aviso: ... - {e}")`. Nenhum `raise`. No Airflow, task termina `success` com tabela vazia/desatualizada.
- **Por que importa:** viola observabilidade (DataOps) e transforma qualquer falha de carga em dado faltante silencioso.
- **Como corrigir:** remover o try/except ou terminar com `raise` após logar. Falha de carga deve falhar a task e acionar retry.
- **Corrigido em 26/09/2026:** removidos os `try/except` com `print`; exceção agora falha a task e aciona retry ✅

### A3 Idempotência só intra-dia: sem PK/UK nas `*_raw`, reprocessar entre dias duplica — Alta · Esforço P
- **Local:** `sql/create_schema.sql:181-239` vs `airflow/dags/pipeline_ibge.py:59,68`, `pipeline_cnpj.py:63`
- **Status:** verificado
- **Evidência:** tabelas landing "sem PKs para permitir recarga". A proteção é `DELETE WHERE loaded_at::date = CURRENT_DATE` antes do `append`, em transação separada do `to_sql`. Funciona no mesmo dia; entre dias acumula +5.571 municípios/dia sem alarme; se falhar entre DELETE e INSERT, perde o dia. `loaded_at` usa `CURRENT_TIMESTAMP` do banco, não a data lógica da DAG.
- **Por que importa:** viola o inegociável nº 2 (reprocessar não duplica / não perde).
- **Como corrigir:** DELETE+INSERT na mesma transação (`to_sql(con=conn)`) + UNIQUE por dia `(id, loaded_at::date)` como guarda no banco.
- **Corrigido em 26/09/2026:** carga atômica + 3 UNIQUE indexes por dia ✅

### A4 Tabelas normalizadas e views nunca populadas — Alta · Esforço M
- **Local:** `sql/create_schema.sql:15-173,250-291` vs `airflow/dags/*`
- **Status:** verificado
- **Evidência:** nada escreve em `ibge.municipios/estados`, `cnpj.empresas/enderecos/...`. As 3 views (`v_resumo_empresas_estado`, `v_empresas_atividade`, `v_municipios_indicadores`) consultam tabelas permanentemente vazias.
- **Por que importa:** decisão de modelagem sem dono (cap. 8) — metade do DDL é peso morto e confunde sobre "onde está o dado certo".
- **Como corrigir:** decidir explicitamente: (a) job SQL `raw→modelado`, ou (b) remover normalizadas/views e assumir landing + Parquet como consumo no estágio 1.

### A5 Notebook pula as análises principais sem avisar — Alta · Esforço P
- **Local:** `notebooks/01_exploracao_dados_publicos.ipynb:87-88,111,157,308`
- **Status:** verificado
- **Evidência:** guards `if 'sigla' in df.columns` / `if 'regiao-nome' in ...` / `if 'x' in dir()`. O JSON real tem sigla aninhada (`microrregiao.mesorregiao.UF`), então as células de UF/região só imprimem nada. `Restart & Run All` "passa" sem analisar. Notebook lê `data/raw/`, não `data/processed/`.
- **Por que importa:** disponibilização que parece funcionar e não entrega a decisão (cap. 9).
- **Como corrigir:** `pd.json_normalize` ao carregar ou ler do `processed/`; trocar guards silenciosos por `assert` com mensagem.

### A6 Dados pessoais em texto puro, sem tratamento — Média · Esforço M
- **Local:** `src/collectors/cnpj_collector.py:168-200`, `sql/create_schema.sql:110-166`
- **Status:** verificado
- **Evidência:** nomes de sócios (`qsa`), e-mail, telefones persistidos em JSON/Parquet/Postgres sem mascaramento, minimização ou retenção.
- **Por que importa:** LGPD (acréscimo da skill, fora do livro): exige base legal, minimização e apagamento sob demanda.
- **Como corrigir:** no estágio 1, documentar no README quais campos são pessoais + mascarar e-mail/telefone na landing; definir retenção nas `_raw`.

### A7 Sem CI — testes existem mas ninguém os roda; sem testes de dados — Média · Esforço P
- **Local:** não encontrado no repositório (sem `.github/`)
- **Status:** verificado (ausência)
- **Evidência:** 7 testes passam localmente; nada impede merge quebrando. Zero testes para `spark_transformer.py` (transformações centrais) e zero testes de dados (unicidade de `id`/`cnpj`, não-nulos).
- **Por que importa:** DataOps estágio 1 pede ao menos CI mínimo (cap. 2).
- **Como corrigir:** workflow de ~20 linhas `pip install -r requirements.txt && pytest`; depois 2–3 asserts de dados (ex.: `dropDuplicates(["id"])` realmente único).

### A8 Dinheiro como `float`, datas como texto nas raw — Média · Esforço P
- **Local:** `src/transformers/spark_transformer.py:197`, `sql/create_schema.sql:211-239`
- **Status:** verificado
- **Evidência:** `capital_social` cast para `DoubleType()` no Spark, mas `DECIMAL(18,2)` no Postgres; `data_abertura/data_situacao/ultima_atualizacao` viajam como `VARCHAR` nas `empresas_raw`.
- **Por que importa:** float para dinheiro perde centavos; data como texto quebra ordenação/filtro por partição (cap. 8).
- **Como corrigir:** `DecimalType(18,2)` no Spark + `to_date` antes da carga; ou documentar conversão explícita na carga.

### A9 Mount quebrado + README desatualizado sobre layout — Média · Esforço P
- **Local:** `docker-compose.yml:59`, `README.md:27-46`, `00-create-dados-publicos.sh:1-8`
- **Status:** verificado
- **Evidência:** compose monta `./docker/postgres-init:/docker-entrypoint-initdb.d`, mas `docker/` não existe (`ls docker` → no such file). O script real `00-create-dados-publicos.sh` está na raiz e nunca é montado. README descreve `docker/airflow/Dockerfile` e lista `docker/` duas vezes; real é `airflow/Dockerfile`.
- **Por que importa:** setup quebra na primeira tentativa em máquina limpa (inegociável nº 5).
- **Como corrigir:** mover script para `docker/postgres-init/` ou corrigir o volume; sincronizar árvore do README com `git ls-files`.

### A10 `datetime.now()` naive amarra dado ao momento da execução — Baixa · Esforço P
- **Local:** `src/collectors/cnpj_collector.py:63,68,149,232`
- **Status:** verificado
- **Evidência:** rate-limit, `_data_consulta` e nome `cnpjs_%Y%m%d_%H%M%S.json` usam `now()` sem fuso, em vez da data lógica da DAG.
- **Por que importa:** backfill impossível, ordenação ambígua (cap. 7).
- **Como corrigir:** quando houver backfill real, nomear/particionar por `ds` da DAG e usar `datetime.now(timezone.utc)`.

### A11 Segredos DEV hardcoded no compose/init — Baixa · Esforço P
- **Local:** `docker-compose.yml:24,31-32`, `00-create-dados-publicos.sh:6`
- **Status:** verificado
- **Evidência:** `FERNET_KEY` fixa, `AIRFLOW_ADMIN_*` default `admin/admin`, `CREATE USER postgres WITH PASSWORD 'postgres' SUPERUSER`. Marcado como DEV ONLY nos comentários — ok para portfólio, bloqueante para qualquer exposição.
- **Por que importa:** corrente segurança (cap. 10).
- **Como corrigir:** nada agora além de manter o aviso; antes de expor: gerar Fernet, exigir senha via env, remover superuser.

### A12 Sem lockfile/linter; imports mortos — Baixa · Esforço P
- **Local:** `requirements.txt:1-12`, `src/collectors/cnpj_collector.py:10`, `src/transformers/spark_transformer.py:8,15-17`
- **Status:** verificado
- **Evidência:** só `>=`, sem lock; `timedelta`, `Optional`, `StructField/StructType` importados e não usados.
- **Por que importa:** `pyspark`/`airflow` quebram fácil entre minors; linter evita esse ruído.
- **Como corrigir:** `pip freeze > requirements-lock.txt` (ou pip-tools) + `ruff` no CI do A7.

### A13 Spark para ~5,5 mil linhas; `data/curated/` vazio — Info · Esforço —
- **Local:** `src/transformers/spark_transformer.py:32-39`, `data/curated/` (vazio)
- **Status:** verificado (volume medido: `municipios.json` ~4,5MB)
- **Evidência:** job `local[*]` + JVM para dados que cabem em memória; pandas faria igual com 1/10 da complexidade. `curated/` sugere camada bronze/prata/ouro nunca implementada.
- **Por que não é achado:** projeto de aprendizado — a escolha se justifica pelo objetivo curricular. Só não levar o padrão para pipeline real. Decidir se `curated/` vive ou sai.

## Roadmap

1. **Concluído (26/09/2026)** — A1 (nome do arquivo CNPJ), A2 (falha visível na carga), A3 (carga atômica + UNIQUE por dia).
2. **Agora** — A4 (decidir raw→modelado — destrava o resto), A9 (fix mount + README).
3. **Em seguida** — A5 (notebook sobre `processed/`), A6 (nota LGPD + mascaramento), A7 (CI mínimo), A8 (Decimal/data).
4. **Deliberadamente adiado** — particionamento analítico, catálogo/dicionário formal, monitoramento de frescor/volume/drift, SLA/alerta, streaming ou micro-otimização: sem consumidor além do autor, é over-engineering no estágio 1.

## O que esta análise não cobriu

Produção e volumes reais (nunca rodou fora do local), custos, permissões de rede/nuvem, qualidade real dos dados coletados, execução de Spark/Postgres/Airflow (ausentes neste ambiente), e tudo que viva fora do repo (credenciais reais, console Airflow, eventual dashboard).

## Perguntas em aberto

1. As normalizadas + views são o futuro (job raw→modelado) ou peso morto?
2. CNPJ: pode armazenar sócios/e-mails ou anonimizo já na coleta (escopo LGPD)?
3. Spark é requisito curricular (manter A13) ou posso simplificar trechos para pandas?
