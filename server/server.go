package main

import (
	"log"
	"net"

	"github.com/benblasberg/push-api/server/handlers"
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
	proto.RegisterPushServiceServer(s, handlers.TeamHandler{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
