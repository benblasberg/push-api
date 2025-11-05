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

type EventsIn struct {
	Results []*Event `json:"results"`
}

type Event struct {
	DbrId    string   `json:"dbrId"`
	Date     string   `json:"date"`
	EventId  int64    `json:"eventId"`
	Season   int32    `json:"season"`
	Week     int32    `json:"week"`
	Response Response `json:"response"`
}

type Response struct {
	Id          string   `json:"id"`
	Type        Type     `json:"type"`
	State       State    `json:"state"`
	Grade       Grade    `json:"grade"`
	SingleMatch bool     `json:"singleMatch"`
	Games       []Game   `json:"games"`
	Status      Status   `json:"status"`
	Duration    Duration `json:"duration"`
	Probs       Probs    `json:"probs"`
	Markets     []Market `json:"markets"`
}

type Type struct {
	Description string `json:"description"`
	Id          int64  `json:"id"`
}

type State struct {
	Id   int32  `json:"id"`
	Name string `json:"name"`
}

type Grade struct {
	Code string `json:"code"`
	Name string `json:"name"`
}

type Game struct {
	Id     int64 `json:"id"`
	Season int32 `json:"season"`
	Week   int32 `json:"week"`
	Teams  Teams `json:"teams"`
}

type Teams struct {
	Home TeamRef `json:"home"`
	Away TeamRef `json:"away"`
}

type TeamRef struct {
	Abbr string `json:"abbr"`
	Id   int32  `json:"id"`
}

type Status struct {
	ParlaySuspended SuspendedStatus `json:"parlaySuspended"`
	ModifiedAt      ModifiedAt      `json:"modifiedAt"`
}

type SuspendedStatus struct {
	Id          int32  `json:"id"`
	Value       bool   `json:"value"`
	Description string `json:"description"`
}

type ModifiedAt struct {
	Second string `json:"second"`
	Micro  string `json:"micro"`
}

type Duration struct {
	Id          int32  `json:"id"`
	Description string `json:"description"`
	Start       int32  `json:"start"`
	End         int32  `json:"end"`
}

type Probs struct {
	Standard StandardProbs `json:"standard"`
}

type StandardProbs struct {
	Over  float64 `json:"over"`
	Under float64 `json:"under"`
	Push  float64 `json:"push"`
}

type Market struct {
	Status MarketStatus `json:"status"`
	Stat   Stat         `json:"stat"`
	Game   GameRef      `json:"game"`
	Team   TeamRef      `json:"team"`
	Player Player       `json:"player"`
	Line   Line         `json:"line"`
	Grade  Grade        `json:"grade"`
	Probs  Probs        `json:"probs"`
}

type MarketStatus struct {
	MarketSuspended SuspendedStatus `json:"marketSuspended"`
}

type Stat struct {
	Id   int32  `json:"id"`
	Name string `json:"name"`
}

type GameRef struct {
	Id int64 `json:"id"`
}

type Player struct {
	Id   int64  `json:"id"`
	Name string `json:"name"`
}

type Line struct {
	Value float64 `json:"value"`
	Type  string  `json:"type"`
}
