from pyspark.sql import SparkSession
from src.config.app_config import AppConfig
 
class SparkSessionManager:
 
    def __init__(self, config: AppConfig):
        self._config = config
        self._session: SparkSession = self._create_session()
 
    def _create_session(self) -> SparkSession:
        return (
            SparkSession.builder
            .appName(self._config.APP_NAME)
            .master(self._config.MASTER)
            .config("spark.sql.session.timeZone", "America/Sao_Paulo")
            .getOrCreate()
        )
 
    def get_session(self) -> SparkSession:
        return self._session
 
    def stop(self) -> None:
        if self._session:
            self._session.stop()