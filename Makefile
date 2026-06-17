PROJECT_NAME=spark-practice
PYTHON := python3
SPARK_MASTER_URL=spark://spark-master:7077
WORKERS := $(if $(filter-out run,$(MAKECMDGOALS)),$(filter-out run,$(MAKECMDGOALS)),3)

.PHONY: install run stop

install:
	@if [ -f requirements.txt ]; then \
		$(PYTHON) -m pip install --upgrade pip; \
		$(PYTHON) -m pip install -r requirements.txt; \
	else \
		echo "requirements.txt not found, skipping pip install."; \
	fi

run:
	docker compose up -d --scale spark-worker=$(WORKERS)
	docker compose exec spark-master spark-submit --master $(SPARK_MASTER_URL) /app/main.py

stop:
	docker compose down

%:
	@:
