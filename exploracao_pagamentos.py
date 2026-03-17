from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName("dataeng-pagamentos").getOrCreate()

# schema
pagamentos_schema = StructType([
    StructField("id_pedido", StringType(), True),
    StructField("forma_pagamento", StringType(), True),
    StructField("valor_pagamento", DecimalType(10, 2), True),
    StructField("status", BooleanType(), True),
    StructField("data_processamento", StringType(), True),
    StructField("avaliacao_fraude", StructType([
        StructField("fraude", BooleanType(), True),
        StructField("score", DoubleType(), True)
    ]), True)
])

# leitura dos arquivos JSON
df_pagamentos = spark.read \
    .schema(pagamentos_schema) \
    .json("./projeto/dataset-json-pagamentos/data/pagamentos/*.json.gz")

df_pagamentos.printSchema()

df_pagamentos.select(
    "*",
    "avaliacao_fraude.*"
).drop("avaliacao_fraude").show()