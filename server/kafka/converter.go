package kafka

import (
	"encoding/json"
	"log/slog"

	proto "github.com/benblasberg/push-api/server/protobuf/gen"
	"github.com/segmentio/kafka-go"
)

type Converter[T any] interface {
	Convert(kafka.Message) (*T, error)
}

type TeamsConverter struct{}

func (t *TeamsConverter) Convert(message kafka.Message) ([]*proto.Team, error) {
	slog.Error(string(message.Value))
	var data TeamsIn
	err := json.Unmarshal(message.Value, &data)
	if err != nil {
		return nil, err
	}

	// Convert TeamData to proto.Team
	teams := make([]*proto.Team, len(data.Data))
	for i, teamData := range data.Data {
		teams[i] = &proto.Team{
			TeamId:       teamData.TeamId,
			TeamAbbr:     teamData.TeamAbbr,
			Location:     teamData.Location,
			TeamName:     teamData.TeamName,
			TeamFullName: teamData.TeamFullName,
			League:       teamData.League,
			DivisionId:   teamData.DivisionId,
		}
	}

	return teams, nil
}
