# PySpark ETL Parquet в PostgreSQL

ETL на Apache Spark чтение Parquet из S3, добавление timestamp, запись в PostgreSQL

## Быстрый старт

```bash
# 1. запустить github.com/erxaan/parquet-iceberg-s3 (данные)
cd ../parquet-iceberg-s3
make all

# 2. запустить этот проект
cd ../pyspark-parquet-postgres
make all
```

Или пошагово:
```bash
make check-minio  # проверить minio
make build        # собрать docker 
make up           # запуск контейнеров
make run          # pyspark etl
make run-python   # python etl
make compare      # сравнение
```

## Команды

```bash
make all         # полный цикл
make build       # собрать образ
make up          # запуск
make down        # остановка
make run         # pyspark etl
make run-python  # python etl
make compare     # сравнение
make clean       # удалить все
```

## Доступ

- MinIO Console: http://localhost:9001 (minioadmin/minioadmin)
- PostgreSQL: localhost:5434 (postgres/postgres, db: salesdb)


## Схема данных

**Входные данные** (Parquet из [parquet-iceberg-s3](https://github.com/erxaan/parquet-iceberg-s3))
| Поле | Тип |
|------|-----|
| id | long |
| product_name | string |
| category | string |
| quantity | int |
| price | double |
| sale_date | date |
| customer_region | string |
| total_amount | double |
| sale_year | int |
| sale_month | int |

**Добавляемое поле**
| Поле | Тип |
|------|-----|
| processed_at | timestamp |

## Результат

После запуска данные в PostgreSQL
- `sales_spark` результат PySpark ETL
- `sales_spark_python` результат Python ETL

```bash
PGPASSWORD=postgres psql -h localhost -p 5434 -U postgres -d salesdb \
  -c "SELECT * FROM sales_spark LIMIT 5;"
```

---

## Сравнение PySpark и Python

### Объем кода

| Метрика | PySpark (`main.py`) | Python (`python_etl.py`) |
|---------|---------------------|--------------------------|
| Всего строк | 119 | 126 |
| Строк кода | 80 | 85 |
| Функции | 5 | 5 |

### Количество операций ETL

| Этап | PySpark | Python (pandas) |
|------|---------|-----------------|
| Подключение | 1 (SparkSession) | 1 (boto3 client) |
| Чтение | 1 (`spark.read.parquet`) | 2 (`get_object` + `read_table`) |
| Трансформация | 1 (`withColumn`) | 1 (присваивание) |
| Запись | 1 (`write.jdbc`) | 1 (`to_sql`) |
| **Итого** | **4 операции** | **5 операций** |

### Сложность

| Аспект | PySpark | Python |
|--------|---------|--------|
| Чтение из S3 | одна строка | скачивание и парсинг |
| Запись в БД | JDBC (встроено) | SQLAlchemy |
| Инфраструктура | Docker образ | Docker образ |

### Вывод

| Критерий | Лучше |
|----------|-------|
| Простота кода | PySpark |
| Инфраструктура | Python (без Docker) |
| Малые данные (меньше 1 ГБ) | Python |
| Большие данные (больше 10 ГБ) | PySpark |
| Кластерная обработка | PySpark |

Для 1000 записей оба подхода работают одинаково,
PySpark код проще и масштабируется на кластер
