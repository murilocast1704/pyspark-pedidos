from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

df = spark.read.parquet("/home/ubuntu/environment/pyspark-pedidos/output/relatorio_pedidos")
df.printSchema()
df.show(20)
