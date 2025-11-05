package handlers

import (
	"log/slog"

	"github.com/benblasberg/push-api/server/auth"
	"github.com/benblasberg/push-api/server/kafka"
	proto "github.com/benblasberg/push-api/server/protobuf/gen"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PushServer struct {
	proto.UnimplementedPushServiceServer
	teamConsumer  *kafka.Consumer[proto.Team]
	eventConsumer *kafka.Consumer[proto.Event]
	authService   auth.AuthService
}

func NewPushServer(teamConsumer *kafka.Consumer[proto.Team], eventConsumer *kafka.Consumer[proto.Event], authService auth.AuthService) *PushServer {
	return &PushServer{
		teamConsumer:  teamConsumer,
		eventConsumer: eventConsumer,
		authService:   authService,
	}
}

func (t PushServer) StreamTeams(req *proto.StreamTeamRequest, server proto.PushService_StreamTeamsServer) error {
	if !t.authService.AuthenitcateToken(req.GetToken()) {
		return status.Error(codes.Unauthenticated, "Could not authenticate token")
	}

	// register with the kafka listener, and read the channel returned by it
	// Can also check against a count of connections and reject when we've hit our configured limit
	id, stream, err := t.teamConsumer.AddConnection()
	defer t.teamConsumer.RemoveConnection(id)
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

func (t PushServer) StreamEvents(req *proto.StreamEventRequest, server proto.PushService_StreamEventsServer) error {
	if !t.authService.AuthenitcateToken(req.GetToken()) {
		return status.Error(codes.Unauthenticated, "Could not authenticate token")
	}

	// register with the kafka listener, and read the channel returned by it
	// Can also check against a count of connections and reject when we've hit our configured limit
	id, stream, err := t.eventConsumer.AddConnection()
	defer t.eventConsumer.RemoveConnection(id)
	if err != nil {
		slog.ErrorContext(server.Context(), "Error connecting to kafka consumer")
		return status.Error(codes.Internal, "Internal error occurred")
	}

ReadLoop:
	for {
		select {
		case <-server.Context().Done():
			return status.Error(codes.Unavailable, "Server is shutting down")
		case events, ok := <-stream:
			if ok {
				server.Send(&proto.StreamEventResponse{
					Event: events,
				})
			} else {
				slog.DebugContext(server.Context(), "Consumer channel closed")
				break ReadLoop
			}
		}
	}

	return nil
}
