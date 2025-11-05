package kafka

import (
	"encoding/json"

	proto "github.com/benblasberg/push-api/server/protobuf/gen"
	"github.com/segmentio/kafka-go"
)

type Converter[T any] interface {
	Convert(kafka.Message) ([]*T, error)
}

type TeamsConverter struct{}

func (t *TeamsConverter) Convert(message kafka.Message) ([]*proto.Team, error) {
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

type EventConverter struct{}

func (e *EventConverter) Convert(message kafka.Message) ([]*proto.Event, error) {
	var data EventsIn
	err := json.Unmarshal(message.Value, &data)
	if err != nil {
		return nil, err
	}

	events := make([]*proto.Event, len(data.Results))
	for i, eventData := range data.Results {
		events[i] = &proto.Event{
			DbrId:   eventData.DbrId,
			Date:    eventData.Date,
			EventId: eventData.EventId,
			Season:  eventData.Season,
			Week:    eventData.Week,
			Response: &proto.Response{
				Id: eventData.Response.Id,
				Type: &proto.Type{
					Description: eventData.Response.Type.Description,
					Id:          eventData.Response.Type.Id,
				},
				State: &proto.State{
					Id:   eventData.Response.State.Id,
					Name: eventData.Response.State.Name,
				},
				Grade: &proto.Grade{
					Code: eventData.Response.Grade.Code,
					Name: eventData.Response.Grade.Name,
				},
				SingleMatch: eventData.Response.SingleMatch,
				Games:       convertGames(eventData.Response.Games),
				Status: &proto.Status{
					ParlaySuspended: &proto.SuspendedStatus{
						Id:          eventData.Response.Status.ParlaySuspended.Id,
						Value:       eventData.Response.Status.ParlaySuspended.Value,
						Description: eventData.Response.Status.ParlaySuspended.Description,
					},
					ModifiedAt: &proto.ModifiedAt{
						Second: eventData.Response.Status.ModifiedAt.Second,
						Micro:  eventData.Response.Status.ModifiedAt.Micro,
					},
				},
				Duration: &proto.Duration{
					Id:          eventData.Response.Duration.Id,
					Description: eventData.Response.Duration.Description,
					Start:       eventData.Response.Duration.Start,
					End:         eventData.Response.Duration.End,
				},
				Probs: &proto.Probs{
					Standard: &proto.StandardProbs{
						Over:  eventData.Response.Probs.Standard.Over,
						Under: eventData.Response.Probs.Standard.Under,
						Push:  eventData.Response.Probs.Standard.Push,
					},
				},
				Markets: convertMarkets(eventData.Response.Markets),
			},
		}
	}

	return events, nil
}

func convertGames(games []Game) []*proto.Game {
	protoGames := make([]*proto.Game, len(games))
	for i, game := range games {
		protoGames[i] = &proto.Game{
			Id:     game.Id,
			Season: game.Season,
			Week:   game.Week,
			Teams: &proto.Teams{
				Home: &proto.TeamRef{
					Abbr: game.Teams.Home.Abbr,
					Id:   game.Teams.Home.Id,
				},
				Away: &proto.TeamRef{
					Abbr: game.Teams.Away.Abbr,
					Id:   game.Teams.Away.Id,
				},
			},
		}
	}
	return protoGames
}

func convertMarkets(markets []Market) []*proto.Market {
	protoMarkets := make([]*proto.Market, len(markets))
	for i, market := range markets {
		protoMarkets[i] = &proto.Market{
			Status: &proto.MarketStatus{
				MarketSuspended: &proto.SuspendedStatus{
					Id:          market.Status.MarketSuspended.Id,
					Value:       market.Status.MarketSuspended.Value,
					Description: market.Status.MarketSuspended.Description,
				},
			},
			Stat: &proto.Stat{
				Id:   market.Stat.Id,
				Name: market.Stat.Name,
			},
			Game: &proto.GameRef{
				Id: market.Game.Id,
			},
			Team: &proto.TeamRef{
				Abbr: market.Team.Abbr,
				Id:   market.Team.Id,
			},
			Player: &proto.Player{
				Id:   market.Player.Id,
				Name: market.Player.Name,
			},
			Line: &proto.Line{
				Value: market.Line.Value,
				Type:  market.Line.Type,
			},
			Grade: &proto.Grade{
				Code: market.Grade.Code,
				Name: market.Grade.Name,
			},
			Probs: &proto.Probs{
				Standard: &proto.StandardProbs{
					Over:  market.Probs.Standard.Over,
					Under: market.Probs.Standard.Under,
					Push:  market.Probs.Standard.Push,
				},
			},
		}
	}
	return protoMarkets
}
