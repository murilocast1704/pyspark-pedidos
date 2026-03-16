from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("dataeng-exemplo-dataframe").getOrCreate()

schema = """
ID_PEDIDO STRING,
PRODUTO STRING,
VALOR_UNITARIO Decimal(10,2), 
QUANTIDADE int,
DATA_CRIACAO timestamp,
UF STRING,
ID_CLIENTE int """

df = spark.read \
    .format("csv") \
    .option("sep", ";") \
    .option("header", True) \
    .schema(schema) \
    .load("./projeto/datasets-csv-pedidos/data/pedidos/*")

df.printSchema()

df.show()

df.filter(df.ID_PEDIDO == "b041ce31-10ae-4af1-becc-68507500cf2a").show()


## Ler o resultado do pipeline
df_resultado = spark.read.parquet("projeto/output/relatorio_pedidos/")
df_resultado.printSchema()
print("Total de linhas:", df_resultado.count())
df_resultado.show()