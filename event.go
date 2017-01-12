package remit

type Event struct {
	EventId   string
	EventType string
	Resource  string
	Data      EventData
}

type EventData map[string]interface{}
type EventCallback func(Event) (interface{}, error)
