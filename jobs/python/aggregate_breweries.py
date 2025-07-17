from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit
import os
from dotenv import load_dotenv
import logging
import sys

# -------------------------------
# Configuração de logging
# -------------------------------
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger("aggregate_breweries")

# -------------------------------
# Variáveis de ambiente
# -------------------------------
load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = "silver"
MINIO_PATH = "breweries"

# Validação das variáveis de ambiente
if not all([MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY]):
    logger.error("Variáveis de ambiente MINIO não configuradas corretamente.")
    raise EnvironmentError("Verifique .env: MINIO_ENDPOINT, MINIO_ROOT_USER e MINIO_ROOT_PASSWORD")

try:
    # Criar SparkSession
    spark = SparkSession.builder \
        .appName("AggregateBreweries") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    logger.info("SparkSession criada com sucesso.")

    # Leitura da tabela Hudi no silver
    input_path = f"s3a://{MINIO_BUCKET}/{MINIO_PATH}/"
    logger.info(f"Lendo tabela Hudi do caminho: {input_path}")
    df = spark.read.format("hudi").load(input_path)

    if df.rdd.isEmpty():
        logger.error("DataFrame está vazio após leitura dos dados.")
        raise ValueError("Nenhum dado para processar")

    # Verifica se colunas esperadas existem
    expected_cols = {"brewery_type", "state"}
    missing_cols = expected_cols - set(df.columns)
    if missing_cols:
        logger.error(f"Colunas faltando para agregação: {missing_cols}")
        raise ValueError("Schema inválido: colunas necessárias ausentes")

    # Agrupar quantidade de cervejarias por tipo e estado
    df_agg = df.groupBy("brewery_type", "state") \
                .agg(count("*").alias("quantity")) \
                .orderBy("state", "brewery_type")

    logger.info("Agregação realizada com sucesso.")

    # Define caminho de saída para tabela agregada (gold)
    output_path = f"s3a://{MINIO_BUCKET.replace('silver','gold')}/breweries_aggregated/"

    # Escrever resultado em modo overwrite (substituindo tabela)
    df_agg.write.mode("overwrite").parquet(output_path)

    logger.info(f"Tabela agregada escrita com sucesso em {output_path}")

except Exception as e:
    logger.error("Erro durante o processamento Spark.", exc_info=True)
    raise e

finally:
    spark.stop()
    logger.info("SparkSession finalizada.")
