package main

import (
	"context"
	"log"
	"net"

	"github.com/benblasberg/push-api/server/auth"
	"github.com/benblasberg/push-api/server/handlers"
	"github.com/benblasberg/push-api/server/kafka"
	server_kafka "github.com/benblasberg/push-api/server/kafka"
	proto "github.com/benblasberg/push-api/server/protobuf/gen"
	"google.golang.org/grpc"
)

func main() {
	port := ":5000"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()

	ctx := context.Background()

	brokers := []string{"broker:9092", "broker:9093"}

	teamsConsumer := server_kafka.NewConsumer(server_kafka.TEAMS, brokers, &kafka.TeamsConverter{})
	err = teamsConsumer.Start(ctx)
	if err != nil {
		log.Fatalf("Failed to connect to kafka broker: %v", err)
	}
	eventsConsumer := server_kafka.NewConsumer(server_kafka.EVENTS, brokers, &kafka.EventConverter{})
	err = eventsConsumer.Start(ctx)
	if err != nil {
		log.Fatalf("Failed to connect to kafka broker: %v", err)
	}

	authService := auth.NewDummyAuthService([]string{"validtoken1", "validtoken2"})
	proto.RegisterPushServiceServer(s, handlers.NewPushServer(teamsConsumer, eventsConsumer, authService))

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
