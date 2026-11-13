import os
from pathlib import Path
import dj_database_url

BASE_DIR = Path(__file__).resolve().parent

SECRET_KEY = os.getenv("SECRET_KEY", "fallback-secret-key")
DEBUG = os.getenv("DEBUG", "False") == "True"
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "TrackerApp",
    "schedules",
]

DATABASES = {
    "default": dj_database_url.config(
        default=os.getenv("DATABASE_URL"),
        conn_max_age=600,
        ssl_require=True
    )
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
AISSTREAM_API_KEY = os.getenv("AISSTREAM_API_KEY")