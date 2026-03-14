# PySpark Pedidos — Relatório de Pagamentos Recusados

Projeto em PySpark orientado a objetos que gera um relatório de pedidos com pagamentos **recusados** (`status=false`) e classificados como **legítimos** (`fraude=false`) no ano de **2025**.

---

## Estrutura do Projeto

```
projeto/
├── main.py                              # Ponto de entrada do pipeline
├── pyproject.toml                       # Configuração do build e dependências
├── requirements.txt                     # Dependências principais
├── MANIFEST.in                          # Arquivos incluídos no pacote
├── README.md                            # Documentação
├── data/                                # Datasets de entrada
│   ├── pedidos/                         # CSV de pedidos
│   └── pagamentos/                      # JSON de pagamentos
├── output/                              # Saída em Parquet
│   └── relatorio_pedidos/
├── src/                                 # Código-fonte
│   ├── config/app_config.py             # Configurações centralizadas
│   ├── session/spark_session_manager.py # Gerenciamento da SparkSession
│   ├── io/data_reader.py                # Leitura CSV/JSON com schema explícito
│   ├── io/data_writer.py                # Escrita em Parquet
│   ├── business/pedidos_logic.py        # Lógica de negócio (filtros, join, ordenação)
│   └── pipeline/pipeline_orchestrator.py
└── tests/
    └── test_pedidos_logic.py            # Testes unitários (pytest)
```

---

## Pré-requisitos

| Ferramenta | Versão mínima |
|------------|---------------|
| Python     | 3.10          |
| Java (JDK) | 11            |
| PySpark    | 3.5.0         |

---

## Configuração do Ambiente

```bash
# Clone o repositório
git clone https://github.com/murilocast1704/pyspark-pedidos.git
cd pyspark-pedidos

# Crie e ative o ambiente virtual
python -m venv .venv
source .venv/bin/activate      # Linux/macOS
# .venv\Scripts\activate       # Windows

# Instale as dependências
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

## Execução

```bash
python main.py
```

O relatório será gravado em **Parquet** no diretório `output/relatorio_pedidos/`.

### Leitura do resultado (opcional)

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
df = spark.read.parquet("output/relatorio_pedidos")
df.show()
```

---

## Testes Unitários

```bash
pytest tests/ -v

# Com relatório de cobertura
pytest tests/ -v --cov=src
```

---

## Relatório Gerado

| Coluna              | Descrição                        |
|---------------------|----------------------------------|
| id_pedido           | Identificador único do pedido    |
| uf                  | Estado onde o pedido foi feito   |
| forma_pagamento     | Forma de pagamento               |
| valor_total_pedido  | Valor total do pedido            |
| data_criacao        | Data de criação do pedido        |

**Filtros aplicados:**
- Apenas pedidos do ano de **2025**
- Pagamentos com `status = false` (recusados)
- Pagamentos com `avaliacao_fraude.fraude = false` (legítimos)

**Ordenação:** `uf` → `forma_pagamento` → `data_criacao`
