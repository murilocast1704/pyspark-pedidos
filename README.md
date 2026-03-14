PySpark Pedidos — Relatório de Pagamentos

Projeto em PySpark orientado a objetos que gera um relatório de pedidos com pagamentos
recusados (STATUS=false) e classificados como legítimos (FRAUDE=false) no ano de 2025.

Estrutura do Projeto

projeto/
├── main.py                          # Ponto de entrada do pipeline
├── pyproject.toml                   # Configuração do build e dependências
├── requirements.txt                 # Dependências principais
├── MANIFEST.in                      # Arquivos incluídos no pacote
├── README.md                        # Documentação
├── data/                            # Datasets de entrada
│   ├── pedidos/                     # CSV de pedidos
│   └── pagamentos/                  # JSON de pagamentos
├── output/                          # Saída em Parquet
│   └── relatorio_pedidos/
├── src/                             # Código-fonte
│   ├── config/app_config.py         # Configurações centralizadas
│   ├── session/spark_session_manager.py
│   ├── io/data_reader.py            # Leitura CSV/JSON com schema explícito
│   ├── io/data_writer.py            # Escrita em Parquet
│   ├── business/pedidos_logic.py    # Lógica de negócio (filtros, join, ordenação)
│   └── pipeline/pipeline_orchestrator.py
└── tests/test_pedidos_logic.py      # Testes unitários (pytest)

Pré-requisitos 
Python 3.10 
Java (JDK) 11
PySpark 3.5.0

# Clone o repositório
git clone https://github.com/murilocast1704/pyspark-pedidos.git
cd pyspark-pedidos

# Crie e ative o ambiente virtual
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
.venv\Scripts\activate      # Windows

# Instale as dependências
pip install -r requirements.txt

Baixe os datasets fazendo git clone
# Pedidos (CSV)
git clone https://github.com/infobarbosa/datasets-csv-pedidos /tmp/pedidos

# Pagamentos (JSON)
git clone https://github.com/infobarbosa/dataset-json-pagamentos /tmp/pagamentos

Depois copie os datasets para as pastas 'data/pedidos/' e 'data/pagamentos/'

cp -r /tmp/pedidos/data/pedidos/* data/pedidos/
cp -r /tmp/pagamentos/data/pagamentos/* data/pagamentos/

Execução

python main.py

O relatório será gravado em Parquet no diretório output/pedidos_recusados_legitimos_parquet/.

Leitura do resultado (opcional)

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
df = spark.read.parquet("output/relatorio_pedidos")
df.show()

Testes unitários
pytest -v
Ou com relatório de cobertura:
pytest -v --cov=src

Relatório gerado

Coluna              Descrição
====================================================
ID_PEDIDO           Identificador único do pedido
UF                  Estado onde o pedido foi feito
FORMA_PAGAMENTO     Forma de pagamento
VALOR_PEDIDO        Valor total do pedido
DATA_PEDIDO         Data de criação do pedido

Filtros aplicados:
Apenas pedidos do ano de 2025
Pagamentos com STATUS = false (recusados)
Pagamentos com FRAUDE = false (legítimos)

Ordenação: UF → FORMA_PAGAMENTO → DATA_PEDIDO
