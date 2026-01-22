.PHONY: up down logs run run-python compare clean restart status help build all check-minio

help:
	@echo "make all        - полный цикл (check-minio + build + up + run + run-python + compare)"
	@echo "make build      - собрать docker образ"
	@echo "make up         - запуск контейнеров"
	@echo "make down       - остановка"
	@echo "make run        - pyspark etl"
	@echo "make run-python - python etl"
	@echo "make compare    - сравнение"
	@echo "make clean      - удалить все"

check-minio:
	@echo "Проверка MinIO..."
	@nc -z localhost 9000 && echo "MinIO (порт 9000): OK" || (echo "ОШИБКА: MinIO не запущен. Запустите parquet-iceberg-s3" && exit 1)

build:
	docker-compose build

up:
	docker-compose up -d

down:
	docker-compose down

run:
	docker exec spark_etl_app python3 /app/main.py

run-python:
	docker exec spark_etl_app python3 /app/python_etl.py

compare:
	@echo "=== Сравнение результатов ==="
	@echo ""
	@echo "PySpark (sales_spark):"
	@PGPASSWORD=postgres psql -h localhost -p 5434 -U postgres -d salesdb -c \
		"SELECT COUNT(*) as records, MIN(processed_at) as processed FROM sales_spark;" 2>/dev/null || echo "  таблица не найдена"
	@echo ""
	@echo "Python (sales_spark_python):"
	@PGPASSWORD=postgres psql -h localhost -p 5434 -U postgres -d salesdb -c \
		"SELECT COUNT(*) as records, MIN(processed_at) as processed FROM sales_spark_python;" 2>/dev/null || echo "  таблица не найдена"

logs:
	docker-compose logs -f

status:
	docker-compose ps

clean:
	docker-compose down -v
	docker rmi pyspark-parquet-postgres-spark 2>/dev/null || true

restart: down up

all: check-minio build up run run-python compare
