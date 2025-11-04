set -e

KAFKA_BOOTSTRAP_SERVERS=kafka:9092

docker build -t producer ./producer
docker run --rm \
    -v ./data-packet-1.json:/app/data/data-packet-1.json:ro \
    --network push-api_app_net \
    -e KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BOOTSTRAP_SERVERS \
    producer $@