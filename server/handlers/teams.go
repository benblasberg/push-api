package handlers

import (
	proto "github.com/benblasberg/push-api/server/protobuf/gen"
)

type TeamHandler struct {
	proto.UnimplementedPushServiceServer
}

func (t TeamHandler) StreamTeams(req *proto.StreamTeamRequest, server proto.PushService_StreamTeamsServer) error {

	// register with the kafka listener, and read the channel returned by it
	// Can also check against a count of connections and reject when we've hit our configured limit

	return nil
}
