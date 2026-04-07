from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str
    REDIS_URL: str
    AZURE_STORAGE_CONNECTION_STRING: str
    AZURE_CONTAINER_NAME: str
    MODEL_PATH: str
    DATA_YAML_PATH: str
    ENVIRONMENT: str = "development"

    class Config:
        env_file = ".env"

settings = Settings()