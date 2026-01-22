from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

from config import (
    JDBC_PROPERTIES,
    JDBC_URL,
    PARQUET_PATH,
    S3_ACCESS_KEY,
    S3_ENDPOINT,
    S3_SECRET_KEY,
    TARGET_TABLE,
)

logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """создание spark session с поддержкой s3"""
    spark = (
        SparkSession.builder
        .appName("ParquetToPostgres-ETL")
        # настройки s3/minio
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_parquet(spark: SparkSession, path: str):
    """чтение parquet из s3"""
    logger.info("Чтение Parquet: %s", path)
    
    df = spark.read.parquet(path)
    
    logger.info("Прочитано %d записей", df.count())
    df.printSchema()
    
    return df


def transform_data(df):
    """добавление поля processed_at"""
    logger.info("Добавление processed_at...")
    
    return df.withColumn("processed_at", current_timestamp())


def write_to_postgres(df, table: str) -> None:
    """запись в postgres через jdbc"""
    logger.info("Запись в PostgreSQL: %s", table)
    
    df.write.jdbc(
        url=JDBC_URL,
        table=table,
        mode="overwrite",
        properties=JDBC_PROPERTIES,
    )
    
    logger.info("Записано успешно")


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )


def main() -> None:
    setup_logging()
    logger.info("PySpark ETL: S3 Parquet -> PostgreSQL")
    
    spark = None
    try:
        # spark session
        spark = create_spark_session()
        logger.info("Spark версия: %s", spark.version)
        
        # чтение parquet
        df = read_parquet(spark, PARQUET_PATH)
        
        # трансформация
        df = transform_data(df)
        
        # пример данных
        logger.info("Пример данных:")
        df.show(5, truncate=False)
        
        # запись в postgres
        write_to_postgres(df, TARGET_TABLE)
        
        logger.info("ETL завершен")
        logger.info("Таблица: %s", TARGET_TABLE)
        
    except Exception as exc:
        logger.exception("Ошибка: %s", exc)
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark остановлен")


if __name__ == "__main__":
    main()
