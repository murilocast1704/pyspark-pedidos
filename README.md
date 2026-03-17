# PySpark Pedidos — Relatório de Pagamentos Recusados

Projeto em PySpark orientado a objetos que gera um relatório de pedidos com pagamentos **recusados** (`status=false`) e classificados como **legítimos** (`avaliacao_fraude.fraude=false`) no ano de **2025**.

Desenvolvido no ambiente **AWS Cloud9** utilizando Python 3.10 e PySpark 3.5.0.

---

## Estrutura do Projeto

```
projeto/
├── main.py                              # Aggregation Root — ponto de entrada e injeção de dependências
├── pyproject.toml                       # Configuração do build e dependências
├── requirements.txt                     # Dependências principais
├── MANIFEST.in                          # Arquivos incluídos no pacote
├── README.md                            # Documentação
├── data/                                # Datasets de entrada
│   ├── pedidos/                         # Arquivos CSV de pedidos
│   └── pagamentos/                      # Arquivos JSON de pagamentos
├── output/                              # Saída gerada após execução
│   └── relatorio_pedidos/               # Relatório final em Parquet
├── src/                                 # Código-fonte da aplicação
│   ├── config/
│   │   └── app_config.py                # Configurações centralizadas (paths, filtros, ordenação)
│   ├── session/
│   │   └── spark_session_manager.py     # Criação e encerramento da SparkSession
│   ├── io/
│   │   ├── data_reader.py               # Leitura CSV/JSON com schemas explícitos
│   │   └── data_writer.py               # Escrita do relatório em Parquet
│   ├── business/
│   │   └── pedidos_logic.py             # Lógica de negócio: filtros, join, cálculo e ordenação
│   └── pipeline/
│       └── pipeline_orchestrator.py     # Orquestração de todas as etapas do pipeline
└── tests/
    └── test_pedidos_logic.py            # Testes unitários com pytest
```

---

## Pré-requisitos

| Ferramenta | Versão mínima |
|------------|---------------|
| Python     | 3.10          |
| Java (JDK) | 11            |
| PySpark    | 3.5.0         |

---

## Configurando o Ambiente AWS Cloud9

O projeto foi desenvolvido e validado no **AWS Cloud9**. Siga os passos abaixo para criar e configurar o ambiente.

### Passo 1 — Criar o ambiente via CloudShell

> **Atenção:** verifique a região no topo à direita do console AWS. Normalmente é **Norte da Virgínia (us-east-1)**. Não altere essa configuração.

1. Acesse o console da AWS e na barra de busca superior digite **CloudShell**
2. Clique no link **CloudShell** — um shell será disponibilizado
3. Execute o comando abaixo para criar o ambiente Cloud9 automaticamente:

```bash
curl -sS https://raw.githubusercontent.com/infobarbosa/data-engineering-cloud9/main/assets/scripts/lab-data-eng-cloud9-environment.sh | bash
```

> A criação leva de **2 a 3 minutos**.

### Passo 2 — Acessar o ambiente Cloud9

1. Na barra de busca superior digite **Cloud9**
2. Clique no link **Cloud9** — será aberto o painel com a lista de ambientes
3. Clique em **Em aberto** para abrir o IDE do ambiente criado
4. Uma nova aba será aberta com o IDE do Cloud9

### Passo 3 — Configurar o ambiente

No terminal do Cloud9 (painel inferior da tela), execute:

```bash
curl -sS https://raw.githubusercontent.com/infobarbosa/data-engineering-cloud9/main/assets/scripts/setup_cloud9_env.sh | bash
```

---

## Instalação do Projeto

Com o ambiente Cloud9 configurado, execute no terminal:

```bash
# 1. Navegue até a pasta de trabalho
cd ~/environment

# 2. Clone o repositório do projeto
git clone https://github.com/murilocast1704/pyspark-pedidos.git
cd pyspark-pedidos

# 3. Crie e ative o ambiente virtual
python -m venv .venv
source .venv/bin/activate

# 4. Instale as dependências
pip install -r requirements.txt
```

---

## Datasets

```bash
# Pedidos (CSV)
git clone https://github.com/infobarbosa/datasets-csv-pedidos /tmp/pedidos
cp -r /tmp/pedidos/data/pedidos/* data/pedidos/

# Pagamentos (JSON)
git clone https://github.com/infobarbosa/dataset-json-pagamentos /tmp/pagamentos
cp -r /tmp/pagamentos/data/pagamentos/* data/pagamentos/
```

---

## Schemas dos Datasets

### Pedidos (CSV — separador `;`)

| Coluna         | Tipo          | Descrição                           |
|----------------|---------------|-------------------------------------|
| id_pedido      | string        | Identificador único do pedido       |
| produto        | string        | Nome do produto                     |
| valor_unitario | decimal(10,2) | Valor unitário do produto           |
| quantidade     | int           | Quantidade comprada                 |
| data_criacao   | timestamp     | Data de criação do pedido           |
| uf             | string        | Estado (UF) onde o pedido foi feito |
| id_cliente     | int           | Identificador do cliente            |

### Pagamentos (JSON)

| Coluna                  | Tipo          | Descrição                                    |
|-------------------------|---------------|----------------------------------------------|
| id_pedido               | string        | Identificador do pedido (chave de join)      |
| forma_pagamento         | string        | Forma de pagamento (PIX, BOLETO, CARTAO)     |
| valor_pagamento         | double        | Valor do pagamento                           |
| status                  | boolean       | `true` = aprovado / `false` = recusado       |
| data_processamento      | string        | Data de processamento no formato ISO         |
| avaliacao_fraude.fraude | boolean       | `true` = fraude / `false` = legítimo         |
| avaliacao_fraude.score  | double        | Score de risco de fraude (0.0 a 1.0)         |

### Relatório Gerado (Parquet)

| Coluna             | Tipo          | Descrição                                        |
|--------------------|---------------|--------------------------------------------------|
| id_pedido          | string        | Identificador único do pedido                    |
| uf                 | string        | Estado onde o pedido foi feito                   |
| forma_pagamento    | string        | Forma de pagamento                               |
| valor_total_pedido | decimal(10,2) | Valor total calculado (valor_unitario * quantidade) |
| data_criacao       | timestamp     | Data de criação do pedido                        |

---

## Execução

```bash
python main.py
```

O relatório será gravado em **Parquet** no diretório `output/relatorio_pedidos/`.

Para visualizar o resultado, com o venv ativo execute:

```bash
python resultado.py
```

---

## Testes Unitários

Os testes cobrem todos os métodos da classe `PedidosLogic` e são executados com **pytest**.

### Executar os testes

```bash
# Rodar todos os testes com detalhes
pytest tests/ -v

# Com relatório de cobertura
pytest tests/ -v --cov=src
```

### Testes implementados

| Teste | Método testado | O que valida |
|-------|----------------|--------------|
| `test_filtrar_pagamentos_recusados_legitimos` | `filtrar_pagamentos_recusados_legitimos()` | Retém apenas registros com `status=False` e `avaliacao_fraude.fraude=False`, descartando aprovados e fraudulentos |
| `test_filtrar_ano` | `filtrar_ano()` | Retém apenas pedidos do ano 2025, descartando registros de outros anos |
| `test_join_pedidos_pagamentos` | `join_pedidos_pagamentos()` | Valida que o join entre pedidos e pagamentos retorna apenas registros com `id_pedido` correspondente nos dois datasets |
| `test_calcular_valor_total` | `calcular_valor_total()` | Verifica que `valor_total_pedido` é calculado corretamente como `valor_unitario * quantidade` |
| `test_selecionar_e_ordenar_colunas` | `selecionar_e_ordenar()` | Confirma que o relatório final contém exatamente as colunas: `id_pedido`, `uf`, `forma_pagamento`, `valor_total_pedido`, `data_criacao` |

### Resultado esperado

```
tests/test_pedidos_logic.py::test_filtrar_pagamentos_recusados_legitimos PASSED
tests/test_pedidos_logic.py::test_filtrar_ano                            PASSED
tests/test_pedidos_logic.py::test_join_pedidos_pagamentos                PASSED
tests/test_pedidos_logic.py::test_calcular_valor_total                   PASSED
tests/test_pedidos_logic.py::test_selecionar_e_ordenar_colunas           PASSED

5 passed in ~15s
```

---

## Regras de Negócio

**Filtros aplicados:**
- Apenas pedidos do ano de **2025** (campo `data_criacao`)
- Pagamentos com `status = false` (recusados)
- Pagamentos com `avaliacao_fraude.fraude = false` (legítimos)

**Ordenação do relatório:** `uf` → `forma_pagamento` → `data_criacao`

**Cálculo do valor total:** `valor_unitario × quantidade` — o dataset de pedidos não possui campo de valor total direto
