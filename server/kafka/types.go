package kafka

type TeamsIn struct {
	Data []*TeamData `json:"data"`
}

type TeamData struct {
	TeamId       int32  `json:"teamId"`
	TeamAbbr     string `json:"teamAbbr"`
	Location     string `json:"location"`
	TeamName     string `json:"teamName"`
	TeamFullName string `json:"teamFullName"`
	League       string `json:"league"`
	DivisionId   int32  `json:"divisionId"`
}
