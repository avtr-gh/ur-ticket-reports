from flask import Flask, jsonify
from app import process_and_sync
import logging
import os

app = Flask(__name__)

PORT = int(os.environ.get("PORT", 8080))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


@app.route("/health", methods=["GET"])
def healthz():
    return ("ok", 200)


@app.route("/latest-report", methods=["GET"])
def latest():
    result = process_and_sync()
    if not result:
        return jsonify({
            "success": False,
            "error": "internal",
            "message": "No result returned"
        }), 500

    status_code = 200 if result.get("success") else 400
    return jsonify(result), status_code


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
