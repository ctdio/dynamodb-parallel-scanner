#!/bin/bash

export COMPOSE_PROJECT_NAME="dynamodb-parallel-scanner-${RANDOM}"

echo "Using the following docker-compose config:"
docker-compose -f docker-compose.yaml -f docker-compose.ci.yaml config

docker-compose -f docker-compose.yaml -f docker-compose.ci.yaml run test
EXIT_CODE=${?}

docker-compose -f docker-compose.yaml -f docker-compose.ci.yaml down

exit ${EXIT_CODE}
