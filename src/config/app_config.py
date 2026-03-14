class AppConfig:
    """Centraliza todas as configurações da aplicação."""
 
    # Spark
    APP_NAME: str = "PySpark Pedidos - Relatório Pagamentos Recusados"
    MASTER: str = "local[*]"
 
    # Paths de entrada
    PEDIDOS_PATH: str = "data/pedidos"
    PAGAMENTOS_PATH: str = "data/pagamentos"
 
    # Path de saída
    OUTPUT_PATH: str = "output/relatorio_pedidos"
    OUTPUT_FORMAT: str = "parquet"
 
    # Filtros de negócio
    ANO_FILTRO: int = 2025
    STATUS_PAGAMENTO: bool = False
    STATUS_FRAUDE: bool = False
 
    # Ordenação do relatório
    ORDER_COLUMNS: list = ["uf", "forma_pagamento", "data_criacao"]