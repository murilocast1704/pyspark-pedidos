import logging

from pyspark.sql import SparkSession

from src.config.app_config import AppConfig
from src.io.data_reader import DataReader
from src.io.data_writer import DataWriter
from src.business.pedidos_logic import PedidosLogic

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Coordena todas as etapas do pipeline de dados."""

    def __init__(
        self,
        spark: SparkSession,
        config: AppConfig,
        reader: DataReader,
        writer: DataWriter,
        business_logic: PedidosLogic,
    ):
        self._spark = spark
        self._config = config
        self._reader = reader
        self._writer = writer
        self._logic = business_logic

    def run(self) -> None:
        logger.info("=== Iniciando pipeline ===")

        # Leitura
        df_pedidos = self._reader.read_pedidos()
        df_pagamentos = self._reader.read_pagamentos()

        # Lógica de negócio
        df_pedidos_2025 = self._logic.filtrar_ano(df_pedidos)
        df_pag_filtrado = self._logic.filtrar_pagamentos_recusados_legitimos(df_pagamentos)
        df_joined = self._logic.join_pedidos_pagamentos(df_pedidos_2025, df_pag_filtrado)
        df_com_total = self._logic.calcular_valor_total(df_joined)
        df_relatorio = self._logic.selecionar_e_ordenar(df_com_total)

        # Escrita
        logger.info("Gravando relatório em %s...", self._config.OUTPUT_PATH)
        self._writer.write(df_relatorio)

        logger.info("=== Pipeline finalizado com sucesso ===")