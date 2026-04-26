Command to run Backend and frontend in debug mode:

docker compose -f docker-compose.yml -f docker-compose-override.yml up -d --build app


Command to start the app in normal mode:

docker compose -f docker-compose.yml up -d
