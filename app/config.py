import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dev-key-change-me")
    SUPABASE_URL = os.getenv("SUPABASE_URL", "")
    SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
    SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL", "")
    UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.dirname(__file__)), "uploads")
    BULK_OUTPUT_FOLDER = os.path.join(os.path.dirname(os.path.dirname(__file__)), "bulk_outputs")
    MAX_CONTENT_LENGTH = 500 * 1024 * 1024  # 500 MB
