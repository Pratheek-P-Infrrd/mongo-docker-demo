import os
from dotenv import load_dotenv

load_dotenv()

class ApplicationConfig:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _load_config(self):
        # Load everything from env
        self.DB_NAME = os.getenv("DB_NAME")
        self.BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))  # fallback if missing
        self.DEV_URI = os.getenv("DEV_MONGO_URI")
        self.LOCAL_URI = os.getenv("LOCAL_MONGO_URI")

        # Basic validation
        if not self.DEV_URI or not self.LOCAL_URI:
            raise ValueError("❌ Missing DEV_MONGO_URI or LOCAL_MONGO_URI in .env")
        if not self.DB_NAME:
            raise ValueError("❌ Missing DB_NAME in .env")

app_config = ApplicationConfig()
