PROJECT_NAME=spark-practice
PYTHON := python3

.PHONY: install run stop

install:
	@if [ -f requirements.txt ]; then \
		$(PYTHON) -m pip install --upgrade pip; \
		$(PYTHON) -m pip install -r requirements.txt; \
	else \
		echo "requirements.txt not found, skipping pip install."; \
	fi

run:
	docker compose up -d
	docker compose exec spark spark-submit /app/main.py

stop:
	docker compose down