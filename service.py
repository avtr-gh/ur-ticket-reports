from flask import Flask, jsonify
from app import get_latest_csv
import logging
import os

app = Flask(__name__)

# Respectar el puerto proporcionado por Cloud Run
PORT = int(os.environ.get("PORT", 8080))

# Configurar logging a stdout (Cloud Logging lo recogerá)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


@app.route("/health", methods=["GET"])
def healthz():
    return ("ok", 200)


@app.route("/latest-report", methods=["GET"])
def latest():
    """Endpoint que devuelve metadata y primer registro del CSV más reciente.

    Retorna JSON con estructura:
    { success: bool, bucket, latest_key, first_record, error, message }
    """
    result = get_latest_csv()
    if not result:
        return jsonify({"success": False, "error": "internal", "message": "No result returned"}), 500

    status_code = 200 if result.get("success") else 400
    return jsonify(result), status_code


if __name__ == "__main__":
    # Ejecutar el servidor de desarrollo solo para pruebas locales
    app.run(host="0.0.0.0", port=PORT)
