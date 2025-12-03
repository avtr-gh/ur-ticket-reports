import boto3
import csv
from io import StringIO
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError
from dotenv import load_dotenv
import os
import logging
import sys

# Cargar variables del .env
load_dotenv()

AWS_BUCKET = os.getenv("AWS_S3_BUCKET")

# Configurar logging para ver más información en la consola
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

def get_latest_csv():
    # El SDK tomará las credenciales automáticamente del entorno (.env cargado)
    if not AWS_BUCKET:
        logging.error("Variable de entorno AWS_S3_BUCKET no definida. Revisa tu archivo .env o las variables de entorno.")
        return

    s3 = boto3.client("s3")
    # usar paginator para manejar buckets grandes
    try:
        logging.info("Listando objetos en el bucket: %s", AWS_BUCKET)
        paginator = s3.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=AWS_BUCKET)

        csv_files = []
        for page in page_iterator:
            contents = page.get("Contents", [])
            for obj in contents:
                if obj["Key"].lower().endswith(".csv"):
                    csv_files.append(obj)

        if not csv_files:
            logging.info("No se encontraron archivos CSV.")
            return {"success": False, "error": "no_csv_files", "message": "No se encontraron archivos CSV en el bucket."}

        latest_file = max(csv_files, key=lambda x: x["LastModified"])
        latest_key = latest_file["Key"]

        logging.info("Archivo más reciente: %s", latest_key)

        obj = s3.get_object(Bucket=AWS_BUCKET, Key=latest_key)
        file_content = obj["Body"].read().decode("utf-8")

        reader = csv.reader(StringIO(file_content))
        first_record = next(reader, None)

        logging.info("Primer registro del CSV:")
        logging.info(first_record)

        return {
            "success": True,
            "bucket": AWS_BUCKET,
            "latest_key": latest_key,
            "first_record": first_record,
        }

    except NoCredentialsError:
        logging.exception("No se encontraron credenciales de AWS. Asegúrate de tener AWS_ACCESS_KEY_ID y AWS_SECRET_ACCESS_KEY configuradas.")
        return {"success": False, "error": "no_credentials", "message": "Credenciales AWS no encontradas."}
    except EndpointConnectionError as e:
        logging.exception("Error de conexión al endpoint de AWS: %s", e)
        return {"success": False, "error": "endpoint_error", "message": str(e)}
    except ClientError as e:
        logging.exception("Error accediendo a S3: %s", e)
        return {"success": False, "error": "client_error", "message": str(e)}
    except Exception as e:
        logging.exception("Error inesperado: %s", e)
        return {"success": False, "error": "unexpected_error", "message": str(e)}


def main():
    try:
        result = get_latest_csv()
        if not result or not result.get("success", False):
            # ya se han registrado mensajes de error
            sys.exit(1)
    except Exception:
        sys.exit(1)
if __name__ == "__main__":
    main()
