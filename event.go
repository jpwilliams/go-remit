package remit

import (
	"sync"

	"github.com/streadway/amqp"
)

type Event struct {
	EventId   string
	EventType string
	Resource  string
	Data      EventData
	Error     interface{}

	Success chan interface{}
	Failure chan interface{}
	Next    chan bool

	message     amqp.Delivery
	waitGroup   *sync.WaitGroup
	gotResult   bool
	workChannel chan *amqp.Channel
}

type EventData map[string]interface{}
