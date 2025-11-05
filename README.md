## Push api

This repo contains a grpc server with two streaming apis.

1. `rpc StreamTeams(StreamTeamRequest) returns (stream StreamTeamResponse) {}`
2. `rpc StreamEvents(StreamEventRequest) returns (stream StreamEventResponse) {}`

Api 1 `StreamTeams` sends the array of teams from data packet 1.
Api 2 `StreamEvents` sends the array of events from data packet 2.

The grpc client defined in `./client` connects to either api based on the `STREAM_TYPE` env var it is given.

It will remain connected to the stream api indefinitely unless the server or itself are closed.

The push server listens to kafka topics to receive events. These events are processed and sent to any connected clients.

## Design Decisions

- **One way push from server** - Using a grpc stream endpoint lets clients connect once and stream messages continually without having to poll the api. 
    Clients provide a `token` which the server validates and then begins streaming messages as they come in. The teams
    data seems more like reference data and could be a standard RESTful endpoint with caching to quickly return this data that rarely changes.
- **Kafka for pushing data to the server** - Servers subscribe to a kafka topic to receive either teams or events. Each server receives all messages,
    which allows us to add more servers as more clients try to connect. Since this is a push api, we only care about the latest messages so
    our consumers in the server code start at the last index. If we need to scale to multiple kafka write partitions, we can use the eventId
    as a message key so that updates from the same game are always in order.

### Security

A production system would have a more elaborate way of validating a client token upon starting the stream. This example just checks
The given token is one of two in a hardcoded list.

Additionally, connecting to the server and kafka would use secure connections to encrypt data in transit.


## Running instructions

Prereqs
- Docker & Docker compose
- Unix terminal

Start base infra

```
docker compose up -d
```

Optionally scale clients connected to the server
```
docker compose up --scale client=3 -d
```

To run in both "modes":
For part 1 the defaults will send data packet 1 at 5 req per second for 30 seconds

add `STREAM_TYPE=teams` to the `client` service in `docker-compose.yml` to tell the client to connect to the team stream api

```
./produce-messages.sh
```

For part 2:

add `STREAM_TYPE=events` to the `client` service in `docker-compose.yml` to tell the client to connect to the team stream api

```
./produce-messages.sh -t data-2 -r 10
```

Viewing the logs for the client container should show that they received all messages (150 for part 1 and 300 for part two).
The client keeps track of the number of messages it receives and prints them.