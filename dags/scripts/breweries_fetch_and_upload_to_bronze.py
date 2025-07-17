from minio import Minio, S3Error
import requests
import json
import io
import logging
from datetime import date
import os
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = "bronze"

logger = logging.getLogger("airflow.task")

def create_minio_client():
    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        return client
    except Exception as e:
        logger.error("Erro ao criar cliente MinIO", exc_info=True)
        raise

def ensure_bucket_exists(client, bucket_name):
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Bucket '{bucket_name}' criado.")
    except S3Error as e:
        logger.error(f"Erro ao verificar/criar bucket '{bucket_name}': {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Erro inesperado ao garantir bucket '{bucket_name}': {e}", exc_info=True)
        raise

def breweries_fetch_and_upload_to_bronze():
    execution_date = date.today().strftime("%Y-%m-%d")
    subdir = f"breweries/{execution_date}"

    client = create_minio_client()
    ensure_bucket_exists(client, MINIO_BUCKET)

    url = "https://api.openbrewerydb.org/v1/breweries"
    per_page = 50
    page = 1

    while True:
        try:
            params = {
                "page": page,
                "per_page": per_page,
                "sort": "name,asc"
            }
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()

            data = response.json()

            if not isinstance(data, list):
                raise ValueError(f"Resposta inválida na página {page}, esperado lista. Obtido: {type(data)}")

            if not data:
                logger.info("Todas as páginas foram processadas.")
                break

            json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
            file_obj = io.BytesIO(json_bytes)
            file_size = len(json_bytes)

            object_name = f"{subdir}/breweries_page_{page}.json"

            try:
                client.put_object(
                    bucket_name=MINIO_BUCKET,
                    object_name=object_name,
                    data=file_obj,
                    length=file_size,
                    content_type="application/json"
                )
            except S3Error as e:
                logger.error(f"Erro ao fazer upload da página {page} para MinIO: {e}", exc_info=True)
                raise

            logger.info(f"Página {page} salva no MinIO como {object_name}")
            page += 1

        except requests.exceptions.Timeout:
            logger.error(f"Timeout na requisição da página {page}", exc_info=True)
            raise
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"Erro HTTP na página {page}: {http_err} - Response: {response.text}", exc_info=True)
            raise
        except requests.exceptions.RequestException as req_err:
            logger.error(f"Erro de requisição na página {page}: {req_err}", exc_info=True)
            raise
        except ValueError as val_err:
            logger.error(f"Erro de validação dos dados na página {page}: {val_err}", exc_info=True)
            raise
        except Exception:
            logger.error(f"Erro inesperado ao processar página {page}", exc_info=True)
            raise
