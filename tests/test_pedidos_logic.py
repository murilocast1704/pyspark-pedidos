"""
tests/test_pedidos_logic.py
Testes unitários para a classe PedidosLogic.
"""

import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType,
    DecimalType, DoubleType,
)
from decimal import Decimal
from datetime import datetime

from src.config.app_config import AppConfig
from src.business.pedidos_logic import PedidosLogic


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("test-pedidos-logic")
        .master("local[1]")
        .getOrCreate()
    )


@pytest.fixture
def config():
    return AppConfig()


@pytest.fixture
def logic(config):
    return PedidosLogic(config)


# ---------------------------------------------------------------------------
# Fixtures com schemas em minúsculo
# ---------------------------------------------------------------------------

@pytest.fixture
def df_pagamentos(spark):
    schema = StructType([
        StructField("id_pedido",          StringType(),       True),
        StructField("forma_pagamento",    StringType(),       True),
        StructField("valor_pagamento",    DecimalType(10, 2), True),
        StructField("status",             BooleanType(),      True),
        StructField("data_processamento", TimestampType(),    True),
        StructField("avaliacao_fraude", StructType([
            StructField("fraude", BooleanType(), True),
            StructField("score",  DoubleType(),  True),
        ]), True),
    ])
    data = [
        Row(id_pedido="p1", forma_pagamento="Pix",    valor_pagamento=Decimal("100.00"), status=False, data_processamento=datetime(2025,1,1), avaliacao_fraude=Row(fraude=False, score=0.1)),  # KEEP
        Row(id_pedido="p2", forma_pagamento="Cartao", valor_pagamento=Decimal("200.00"), status=True,  data_processamento=datetime(2025,1,2), avaliacao_fraude=Row(fraude=False, score=0.2)),  # aprovado → DROP
        Row(id_pedido="p3", forma_pagamento="Boleto", valor_pagamento=Decimal("300.00"), status=False, data_processamento=datetime(2025,1,3), avaliacao_fraude=Row(fraude=True,  score=0.9)),  # fraude → DROP
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def df_pedidos(spark):
    schema = StructType([
        StructField("id_pedido",      StringType(),       True),
        StructField("produto",        StringType(),       True),
        StructField("valor_unitario", DecimalType(10, 2), True),
        StructField("quantidade",     IntegerType(),      True),
        StructField("data_criacao",   TimestampType(),    True),
        StructField("uf",             StringType(),       True),
        StructField("id_cliente",     IntegerType(),      True),
    ])
    data = [
        Row(id_pedido="p1", produto="Notebook", valor_unitario=Decimal("2500.00"), quantidade=1, data_criacao=datetime(2025,3,10), uf="SP", id_cliente=1),
        Row(id_pedido="p2", produto="Mouse",    valor_unitario=Decimal("150.00"),  quantidade=2, data_criacao=datetime(2024,6,5),  uf="RJ", id_cliente=2),  # 2024 → DROP
        Row(id_pedido="p3", produto="Teclado",  valor_unitario=Decimal("300.00"),  quantidade=1, data_criacao=datetime(2025,1,20), uf="MG", id_cliente=3),
    ]
    return spark.createDataFrame(data, schema)


# ---------------------------------------------------------------------------
# Testes
# ---------------------------------------------------------------------------

def test_filtrar_pagamentos_recusados_legitimos(logic, df_pagamentos):
    resultado = logic.filtrar_pagamentos_recusados_legitimos(df_pagamentos)
    ids = [row["id_pedido"] for row in resultado.collect()]
    assert ids == ["p1"], f"Esperado ['p1'], obtido {ids}"


def test_filtrar_ano(logic, df_pedidos):
    resultado = logic.filtrar_ano(df_pedidos)
    ids = sorted([row["id_pedido"] for row in resultado.collect()])
    assert ids == ["p1", "p3"], f"Esperado ['p1', 'p3'], obtido {ids}"


def test_join_pedidos_pagamentos(logic, df_pedidos, df_pagamentos):
    pag = logic.filtrar_pagamentos_recusados_legitimos(df_pagamentos)
    ped = logic.filtrar_ano(df_pedidos)
    resultado = logic.join_pedidos_pagamentos(ped, pag)
    ids = [row["id_pedido"] for row in resultado.collect()]
    assert ids == ["p1"], f"Esperado ['p1'], obtido {ids}"


def test_calcular_valor_total(logic, df_pedidos, df_pagamentos):
    pag = logic.filtrar_pagamentos_recusados_legitimos(df_pagamentos)
    ped = logic.filtrar_ano(df_pedidos)
    joined = logic.join_pedidos_pagamentos(ped, pag)
    resultado = logic.calcular_valor_total(joined)
    row = resultado.collect()[0]
    assert row["valor_total_pedido"] == Decimal("2500.00") * 1


def test_selecionar_e_ordenar_colunas(logic, df_pedidos, df_pagamentos):
    pag = logic.filtrar_pagamentos_recusados_legitimos(df_pagamentos)
    ped = logic.filtrar_ano(df_pedidos)
    joined = logic.join_pedidos_pagamentos(ped, pag)
    com_total = logic.calcular_valor_total(joined)
    resultado = logic.selecionar_e_ordenar(com_total)
    assert set(resultado.columns) == {"id_pedido", "uf", "forma_pagamento", "valor_total_pedido", "data_criacao"}