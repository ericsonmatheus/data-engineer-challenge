up:
	bash create_dirs.sh && sudo docker compose up -d

down:
	sudo docker compose down --remove-orphans --volumes

sh:
	sudo docker compose exec airflow-scheduler sh

ps:
	sudo docker ps
