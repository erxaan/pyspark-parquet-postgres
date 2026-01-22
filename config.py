import os

# postgres
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5434"))
DB_NAME = os.getenv("DB_NAME", "salesdb")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

POSTGRES_CONFIG = {
    "host": DB_HOST,
    "port": DB_PORT,
    "database": DB_NAME,
    "user": DB_USER,
    "password": DB_PASSWORD,
}

# jdbc для spark
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
JDBC_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# minio s3 (из parquet-iceberg-s3)
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_REGION = os.getenv("S3_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "data-bucket")

S3_CONFIG = {
    "endpoint_url": S3_ENDPOINT,
    "aws_access_key_id": S3_ACCESS_KEY,
    "aws_secret_access_key": S3_SECRET_KEY,
    "region_name": S3_REGION,
}

# пути
PARQUET_PREFIX = "parquet/sales"
PARQUET_PATH = f"s3a://{S3_BUCKET}/{PARQUET_PREFIX}/sales_data.parquet"

# таблица
TARGET_TABLE = "sales_spark"
