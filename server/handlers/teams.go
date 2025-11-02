package handlers

import (
	"log/slog"

	"github.com/benblasberg/push-api/server/kafka"
	proto "github.com/benblasberg/push-api/server/protobuf/gen"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type TeamHandler struct {
	proto.UnimplementedPushServiceServer
	consumer *kafka.Consumer
}

func NewTeamHandler(consumer *kafka.Consumer) *TeamHandler {
	return &TeamHandler{
		consumer: consumer,
	}
}

func (t TeamHandler) StreamTeams(req *proto.StreamTeamRequest, server proto.PushService_StreamTeamsServer) error {

	// check auth token

	// register with the kafka listener, and read the channel returned by it
	// Can also check against a count of connections and reject when we've hit our configured limit
	id, stream, err := t.consumer.AddConnection()
	defer t.consumer.RemoveConnection(id)
	if err != nil {
		slog.ErrorContext(server.Context(), "Error connecting to kafka consumer")
		return status.Error(codes.Internal, "Internal error occurred")
	}

ReadLoop:
	for {
		select {
		case <-server.Context().Done():
			return status.Error(codes.Unavailable, "Server is shutting down")
		case teams, ok := <-stream:
			if ok {
				server.Send(&proto.StreamTeamResponse{
					Team: teams,
				})
			} else {
				slog.DebugContext(server.Context(), "Consumer channel closed")
				break ReadLoop
			}
		}
	}

	return nil
}
