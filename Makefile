psql-connect:
	psql --host=localhost --port=5433 --username=postgres

build:
	docker build -t etl-csv -f docker/Dockerfile .

build-up:
	docker-compose -f docker-compose.yaml -f docker-compose-superset.yaml up
