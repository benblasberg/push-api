

Start base infra
```
docker compose up -d
```

Optionally scale clients connected to the server
```
docker compose up --scale client=3 -d
```

Then, run the producer image to send messages to kafka

```
./produce-messages.sh
```

with optional args
```
  -r REQUESTS_PER_SECOND, --requests_per_second REQUESTS_PER_SECOND: Number of requests to send per second
  -d DURATION, --duration DURATION: Duration to send messages in seconds
  -t TYPE, --type TYPE: The type of message to send. Options are "data-1" and "data-2"
```
 