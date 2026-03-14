import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config.app_config import AppConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class PedidosLogic:
    """Encapsula toda a lógica de negócio do pipeline de pedidos."""

    def __init__(self, config: AppConfig):
        self._config = config

    def filtrar_pagamentos_recusados_legitimos(self, df_pagamentos: DataFrame) -> DataFrame:
        try:
            logger.info("Filtrando pagamentos recusados e legítimos...")
            resultado = df_pagamentos.filter(
                (F.col("status") == self._config.STATUS_PAGAMENTO) &
                (F.col("avaliacao_fraude.fraude") == self._config.STATUS_FRAUDE)
            )
            logger.info("Filtro de pagamentos concluído.")
            return resultado
        except Exception as e:
            logger.error("Erro ao filtrar pagamentos: %s", e)
            raise

    def filtrar_ano(self, df_pedidos: DataFrame) -> DataFrame:
        try:
            logger.info("Filtrando pedidos do ano %s...", self._config.ANO_FILTRO)
            resultado = df_pedidos.filter(
                F.year(F.col("data_criacao")) == self._config.ANO_FILTRO
            )
            logger.info("Filtro de ano concluído.")
            return resultado
        except Exception as e:
            logger.error("Erro ao filtrar por ano: %s", e)
            raise

    def join_pedidos_pagamentos(
        self, df_pedidos: DataFrame, df_pagamentos: DataFrame
    ) -> DataFrame:
        try:
            logger.info("Realizando join entre pedidos e pagamentos...")
            resultado = df_pedidos.join(
                df_pagamentos,
                on="id_pedido",
                how="inner",
            )
            logger.info("Join concluído.")
            return resultado
        except Exception as e:
            logger.error("Erro ao realizar join: %s", e)
            raise

    def calcular_valor_total(self, df: DataFrame) -> DataFrame:
        try:
            logger.info("Calculando valor total do pedido...")
            resultado = df.withColumn(
                "valor_total_pedido",
                F.col("valor_unitario") * F.col("quantidade"),
            )
            logger.info("Cálculo de valor total concluído.")
            return resultado
        except Exception as e:
            logger.error("Erro ao calcular valor total: %s", e)
            raise

    def selecionar_e_ordenar(self, df: DataFrame) -> DataFrame:
        try:
            logger.info("Selecionando colunas e ordenando o relatório...")
            resultado = (
                df.select(
                    F.col("id_pedido"),
                    F.col("uf"),
                    F.col("forma_pagamento"),
                    F.col("valor_total_pedido"),
                    F.col("data_criacao"),
                )
                .orderBy("uf", "forma_pagamento", "data_criacao")
            )
            logger.info("Seleção e ordenação concluídas.")
            return resultado
        except Exception as e:
            logger.error("Erro ao selecionar/ordenar colunas: %s", e)
            raise