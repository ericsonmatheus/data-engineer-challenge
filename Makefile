up:
	bash create_dirs.sh && sudo docker compose up -d

down:
	sudo docker compose down --remove-orphans --volumes

build:
	sudo docker compose build

stop:
	sudo docker compose -f docker-compose.dev.yaml stop

sh:
	sudo docker compose exec -u 0 airflow-scheduler sh

ps:
	sudo docker ps
