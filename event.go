package remit

import (
	"sync"

	"github.com/streadway/amqp"
)

type Event struct {
	message     amqp.Delivery
	waitGroup   *sync.WaitGroup
	gotResult   bool
	workChannel chan *amqp.Channel

	EventId   string
	EventType string
	Resource  string
	Data      EventData
	Error     interface{}

	Success chan interface{}
	Failure chan interface{}
	Next    chan bool
}

type EventData map[string]interface{}
type EventCallback func(Event) (interface{}, error)
