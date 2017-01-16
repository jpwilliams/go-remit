package remit

import (
	"github.com/streadway/amqp"
)

type Event struct {
	message amqp.Delivery

	EventId   string
	EventType string
	Resource  string
	Data      EventData

	Success chan interface{}
	Failure chan interface{}
}

type EventData map[string]interface{}
type EventCallback func(Event) (interface{}, error)
