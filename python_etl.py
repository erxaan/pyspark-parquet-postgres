from __future__ import annotations

import logging
from datetime import datetime
from io import BytesIO

import boto3
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine

from config import (
    PARQUET_PREFIX,
    POSTGRES_CONFIG,
    S3_BUCKET,
    S3_CONFIG,
    TARGET_TABLE,
)

logger = logging.getLogger(__name__)

PARQUET_KEY = f"{PARQUET_PREFIX}/sales_data.parquet"


def get_s3_client():
    """s3 клиент для minio"""
    return boto3.client(
        "s3",
        endpoint_url=S3_CONFIG["endpoint_url"],
        aws_access_key_id=S3_CONFIG["aws_access_key_id"],
        aws_secret_access_key=S3_CONFIG["aws_secret_access_key"],
        region_name=S3_CONFIG["region_name"],
    )


def read_parquet(s3_client, bucket: str, key: str) -> pd.DataFrame:
    """чтение parquet из s3"""
    logger.info("Чтение Parquet: s3://%s/%s", bucket, key)
    
    response = s3_client.get_object(Bucket=bucket, Key=key)
    buffer = BytesIO(response["Body"].read())
    
    df = pq.read_table(buffer).to_pandas()
    
    logger.info("Прочитано %d записей", len(df))
    logger.info("Колонки: %s", list(df.columns))
    
    return df


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """добавление поля processed_at"""
    logger.info("Добавление processed_at...")
    
    df["processed_at"] = datetime.now()
    
    return df


def write_to_postgres(df: pd.DataFrame, table: str) -> None:
    """запись в postgres"""
    logger.info("Запись в PostgreSQL: %s", table)
    
    conn_str = (
        f"postgresql://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}"
        f"@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
    )
    engine = create_engine(conn_str)
    
    df.to_sql(
        table,
        engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=500,
    )
    
    logger.info("Записано успешно")


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )


def main() -> None:
    setup_logging()
    logger.info("Python ETL (pandas): S3 Parquet -> PostgreSQL")
    
    # проверка minio
    try:
        s3_client = get_s3_client()
        s3_client.head_bucket(Bucket=S3_BUCKET)
        logger.info("Бакет '%s' доступен", S3_BUCKET)
    except Exception as exc:
        logger.error("Ошибка подключения к MinIO: %s", exc)
        return
    
    try:
        # чтение parquet
        df = read_parquet(s3_client, S3_BUCKET, PARQUET_KEY)
        
        # трансформация
        df = transform_data(df)
        
        # пример данных
        logger.info("Пример данных:")
        print(df.head(5).to_string())
        
        # запись в postgres
        write_to_postgres(df, f"{TARGET_TABLE}_python")
        
        logger.info("ETL завершен")
        logger.info("Таблица: %s_python", TARGET_TABLE)
        
    except Exception as exc:
        logger.exception("Ошибка: %s", exc)
        raise


if __name__ == "__main__":
    main()
