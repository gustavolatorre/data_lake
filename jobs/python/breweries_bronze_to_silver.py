from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import unicodedata
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from datetime import date
import os
from dotenv import load_dotenv
import logging
import sys

# -------------------------------
# Configuração de logging
# -------------------------------
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger("bronze_to_silver")

# -------------------------------
# Variáveis de Ambiente
# -------------------------------
load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = "silver"
MINIO_PATH = "breweries"

# -------------------------------
# Validação das variáveis de ambiente
# -------------------------------
if not all([MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY]):
    logger.error("Variáveis de ambiente MINIO não configuradas corretamente.")
    raise EnvironmentError("Verifique .env: MINIO_ENDPOINT, MINIO_ROOT_USER e MINIO_ROOT_PASSWORD")

# -------------------------------
# Conexão MinIO
# -------------------------------
try:
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        logger.info(f"Bucket '{MINIO_BUCKET}' criado.")
    else:
        logger.info(f"Bucket '{MINIO_BUCKET}' já existe.")
except Exception:
    logger.error("Erro ao conectar ou criar bucket no MinIO", exc_info=True)
    raise

# -------------------------------
# Spark Session
# -------------------------------
try:
    spark = SparkSession.builder \
        .appName("BreweriesBronzeToSilver") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
except Exception:
    logger.error("Erro ao criar SparkSession", exc_info=True)
    raise

# -------------------------------
# Schema e leitura de dados
# -------------------------------
schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("brewery_type", StringType(), True),
    StructField("address_1", StringType(), True),
    StructField("address_2", StringType(), True),
    StructField("address_3", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state_province", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("phone", StringType(), True),
    StructField("website_url", StringType(), True),
    StructField("state", StringType(), True),
    StructField("street", StringType(), True),
])

today = date.today().strftime("%Y-%m-%d")

# -------------------------------
# UDF de limpeza de acentos
# -------------------------------
def remove_acentos(text):
    if text is None:
        return None
    return ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )

remove_accents_udf = udf(remove_acentos, StringType())

# -------------------------------
# Leitura dos dados do Bronze
# -------------------------------
try:
    df = spark.read.schema(schema).option("multiline", "true").json(f"s3a://bronze/breweries/{today}/")
    logger.info("Dados lidos com sucesso.")
except Exception:
    logger.error("Erro ao ler JSON do bucket Bronze", exc_info=True)
    raise

# -------------------------------
# Verificação de qualidade dos dados
# -------------------------------
# Checar schema
expected_columns = set(field.name for field in schema.fields)
read_columns = set(df.columns)
missing_columns = expected_columns - read_columns

if missing_columns:
    logger.error(f"Colunas faltando no JSON de entrada: {missing_columns}")
    raise ValueError("Schema inválido")

# Checar valores nulos em campos críticos
null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in ["id", "name", "state"]]).collect()[0]
for field in ["id", "name", "state"]:
    if null_counts[field] > 0:
        logger.warning(f"{null_counts[field]} registros com {field} nulo")

if df.count() == 0:
    logger.error("DataFrame está vazio após leitura do JSON.")
    raise ValueError("Nenhum dado para processar")

# -------------------------------
# Transformações
# -------------------------------
df = df.withColumn("state",
    when(col("state") == "K�rnten", "Karnten")
    .when(col("state") == "Nieder�sterreich", "Niederoesterreich")
    .otherwise(col("state"))
)
df = df.withColumn("state", remove_accents_udf(col("state")))
df = df.withColumn("data_ingestao", lit(today))

# -------------------------------
# Escrita como Hudi (Silver)
# -------------------------------
hudi_path = f"s3a://{MINIO_BUCKET}/{MINIO_PATH}/"

try:
    df.write.format("hudi") \
        .option("hoodie.table.name", "breweries_hudi") \
        .option("hoodie.datasource.write.recordkey.field", "id") \
        .option("hoodie.datasource.write.precombine.field", "data_ingestao") \
        .option("hoodie.datasource.write.partitionpath.field", "state") \
        .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.SimpleKeyGenerator") \
        .option("hoodie.datasource.write.operation", "upsert") \
        .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
        .mode("append") \
        .save(hudi_path)
    logger.info("Dados gravados com sucesso em formato Hudi.")
except Exception:
    logger.error("Erro ao gravar dados no formato Hudi", exc_info=True)
    raise
