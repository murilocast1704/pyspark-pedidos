from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType,
    DecimalType, DoubleType,
)
from src.config.app_config import AppConfig

class DataReader:

    # Schema explícito — pedidos CSV
    SCHEMA_PEDIDOS: StructType = StructType([
        StructField("id_pedido",      StringType(),       nullable=True),
        StructField("produto",        StringType(),       nullable=True),
        StructField("valor_unitario", DecimalType(10, 2), nullable=True),
        StructField("quantidade",     IntegerType(),      nullable=True),
        StructField("data_criacao",   TimestampType(),    nullable=True),
        StructField("uf",             StringType(),       nullable=True),
        StructField("id_cliente",     IntegerType(),      nullable=True),
    ])

    # Schema explícito — pagamentos JSON
    SCHEMA_PAGAMENTOS: StructType = StructType([
        StructField("id_pedido",          StringType(),  nullable=True),
        StructField("forma_pagamento",    StringType(),  nullable=True),
        StructField("valor_pagamento",    DoubleType(),  nullable=True),
        StructField("status",             BooleanType(), nullable=True),
        StructField("data_processamento", StringType(),  nullable=True),
        StructField("avaliacao_fraude", StructType([
            StructField("fraude", BooleanType(), nullable=True),
            StructField("score",  DoubleType(),  nullable=True),
        ]), nullable=True),
    ])

    def __init__(self, spark: SparkSession, config: AppConfig):
        self._spark = spark
        self._config = config

    def read_pedidos(self) -> DataFrame:
        return (
            self._spark.read
            .schema(self.SCHEMA_PEDIDOS)
            .option("header", "true")
            .option("sep", ";")
            .csv(self._config.PEDIDOS_PATH)
        )

    def read_pagamentos(self) -> DataFrame:
        return (
            self._spark.read
            .schema(self.SCHEMA_PAGAMENTOS)
            .json(self._config.PAGAMENTOS_PATH)
        )