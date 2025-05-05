import os
from pydantic import BaseSettings

class Settings(BaseSettings):
    AWS_ACCESS_KEY: str
    AWS_SECRET_KEY: str
    AWS_REGION: str = "us-west-2"
    S3_BUCKET_NAME: str = "my-app-bucket"

    class Config:
        env_file = ".env"

settings = Settings()