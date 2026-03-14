from src.config.app_config import AppConfig
from src.session.spark_session_manager import SparkSessionManager
from src.io.data_reader import DataReader
from src.io.data_writer import DataWriter
from src.business.pedidos_logic import PedidosLogic
from src.pipeline.pipeline_orchestrator import PipelineOrchestrator
 
 
def main():
    # 1. Configuração 
    config = AppConfig()
 
    # 2. Gerenciamento da sessão Spark
    session_manager = SparkSessionManager(config)
    spark = session_manager.get_session()
 
    # 3. Leitura e escrita de dados (I/O)
    reader = DataReader(spark, config)
    writer = DataWriter(config)
 
    # 4. Lógica de negócio
    business_logic = PedidosLogic(config)
 
    # 5. Orquestração do pipeline — injeção de todas as dependências
    orchestrator = PipelineOrchestrator(
        spark=spark,
        config=config,
        reader=reader,
        writer=writer,
        business_logic=business_logic,
    )
 
    orchestrator.run()
 
    session_manager.stop()
 
 
if __name__ == "__main__":
    main()