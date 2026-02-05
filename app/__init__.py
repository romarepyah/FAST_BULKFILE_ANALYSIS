"""Flask application factory."""

import logging, os
from flask import Flask
from .config import Config

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    os.makedirs(Config.UPLOAD_FOLDER, exist_ok=True)
    os.makedirs(Config.BULK_OUTPUT_FOLDER, exist_ok=True)

    from .routes import pages, api
    app.register_blueprint(pages)
    app.register_blueprint(api)

    return app
