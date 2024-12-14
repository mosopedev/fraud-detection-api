from flask import Flask
from dotenv import load_dotenv
from app.routes import transaction
import os

def create_app():
    # Load environment variables from `.env`
    load_dotenv()

    app = Flask(__name__)
    app.config["COUCHBASE_CLUSTER"] = os.getenv("COUCHBASE_CLUSTER")
    app.config["COUCHBASE_USER"] = os.getenv("COUCHBASE_USER")
    app.config["COUCHBASE_PASSWORD"] = os.getenv("COUCHBASE_PASSWORD")

    # Register Blueprints
    app.register_blueprint(transaction.bp)

    return app
