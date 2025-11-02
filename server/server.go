package main

import (
	"context"
	"log"
	"net"

	"github.com/benblasberg/push-api/server/handlers"
	server_kafka "github.com/benblasberg/push-api/server/kafka"
	proto "github.com/benblasberg/push-api/server/protobuf/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	port := ":5000"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()

	ctx := context.Background()

	teamsConsumer := server_kafka.NewConsumer(server_kafka.TEAMS, []string{"broker:9092", "broker:9093"})
	err = teamsConsumer.Start(ctx)
	if err != nil {
		log.Fatalf("Failed to connect to kafka broker: %v", err)
	}
	proto.RegisterPushServiceServer(s, handlers.NewTeamHandler(teamsConsumer))

	reflection.Register(s)
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
