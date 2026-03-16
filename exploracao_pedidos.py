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