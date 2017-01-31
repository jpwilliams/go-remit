package remit

import (
	"sync"

	"github.com/streadway/amqp"
)

type Event struct {
	message   amqp.Delivery
	waitGroup *sync.WaitGroup
	gotResult bool

	EventId   string
	EventType string
	Resource  string
	Data      EventData

	Success chan interface{}
	Failure chan interface{}
	Skip    chan bool
}

type EventData map[string]interface{}
type EventCallback func(Event) (interface{}, error)
