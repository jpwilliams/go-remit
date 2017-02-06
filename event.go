package remit

import (
	"sync"

	"github.com/streadway/amqp"
)

type Event struct {
	// Data given in the message.
	// All fields are always passed back with the exception
	// of `Data` and `Error`, which are mutually exclusive.
	EventId   string      // the ULID of the event
	EventType string      // the routing key used to route this message
	Resource  string      // the service that send this message
	Data      EventData   // the data this message contains (as `EventData`)
	Error     interface{} // the error this message contains

	// Channels that can be used to respond to or acknowledge this message.
	Success chan interface{} // send data back if the handling was successful
	Failure chan interface{} // send an error back if the handling failed
	Next    chan bool        // skip to the next piece of middleware/function

	message     amqp.Delivery
	waitGroup   *sync.WaitGroup
	gotResult   bool
	workChannel chan *amqp.Channel
}

// EventData, for ease of use, sets `Data` within an `Event` to be a `map[string]interface{}`.
// This enables us to access basic properties via indexing, but deeper handling
// is recommended if more control is needed.
//
// A common practice is to use something like `gopkg.in/mgo.v2/bson` to marshal the data
// through a struct:
//
// 	type IncomingData struct {
//		GivenId string `bson:"id"`
//		TargetId string `bson:"target"`
//	}
//
// 	data := new(IncomingData)
// 	b, _ := bson.Marshal(event.Data)
// 	bson.Unmarshal(b, &data)
//
type EventData map[string]interface{}
