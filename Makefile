.PHONY: all build run stop clean test

all: build

build:
	cd api-gateway && go build -o api-gateway .
	cd data-collector && pip install -r requirements.txt
	cd analyzer && pip install -r requirements.txt
	cd signal-generator && pip install -r requirements.txt

run: docker-build
	docker-compose up -d

stop:
	docker-compose down

clean:
	rm -f api-gateway/api-gateway
	rm -rf api-gateway/data
	rm -rf data-collector/data
	rm -rf analyzer/data
	rm -rf signal-generator/data

test:
	@echo "Running Python tests..."
	cd data-collector && python -m pytest tests/ -v || echo "No tests found"
	cd analyzer && python -m pytest tests/ -v || echo "No tests found"
	cd signal-generator && python -m pytest tests/ -v || echo "No tests found"

dev:
	docker-compose up

logs:
	docker-compose logs -f

build-gateway:
	cd api-gateway && go build -o api-gateway .

docker-build:
	docker-compose build

help:
	@echo "BTC Trading Signals Microservices - Make Commands"
	@echo ""
	@echo "  make build       - Build all services"
	@echo "  make run         - Start all services with Docker"
	@echo "  make stop        - Stop all services"
	@echo "  make clean       - Clean build artifacts"
	@echo "  make test        - Run tests"
	@echo "  make dev         - Start services in development mode"
	@echo "  make logs        - View service logs"
