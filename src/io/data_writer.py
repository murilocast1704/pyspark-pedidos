from pyspark.sql import DataFrame
from src.config.app_config import AppConfig
 
 
class DataWriter:
    def __init__(self, config: AppConfig):
        self._config = config
    
    def write(self, df: DataFrame) -> None:
        (
            df.write
            .mode("overwrite")
            .format(self._config.OUTPUT_FORMAT)
            .save(self._config.OUTPUT_PATH)
        )