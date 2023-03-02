package eventcounter

const (
	EventCreated EventType = "created"
	EventUpdated EventType = "updated"
	EventDeleted EventType = "deleted"
)

type EventType string

type Message struct {
	UID       string    `json:"uid"`
	EventType EventType `json:"event_type"`
	UserID    string    `json:"user_id"`
}

type MessageConsumed struct {
	Id        string    `json:"id"`
	User      string    `json:"user"`
	EventType EventType `json:"eventType"`
}
