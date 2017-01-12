package remit

import (
	"github.com/streadway/amqp"
)

type Event struct {
	EventId   string
	EventType string
	Resource  string
	Data      EventData
	message   amqp.Delivery
}

type EventData map[string]interface{}
type EventCallback func(Event) (interface{}, error)
