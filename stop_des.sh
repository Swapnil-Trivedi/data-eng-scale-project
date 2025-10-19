# script brings the docker compose services down
#!/bin/bash
# stop_des.sh
# Stops the DES environment and removes containers, networks, volumes, and images created by `docker

set -e
echo "=============================="
echo " Stopping DES Environment..."
echo "=============================="
docker-compose -f ./docker/docker-compose.yaml down -v
# prune unused docker volumes
echo "Pruning unused Docker volumes..."
docker volume prune -f

echo "DES Environment stopped and cleaned up."

## check docker containers
echo """Active containers after stopping DES:"
docker ps --filter "name=des_" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo "All DES containers have been stopped."