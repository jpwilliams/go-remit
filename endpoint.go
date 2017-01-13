package remit

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/chuckpreslar/emission"
	"github.com/streadway/amqp"
)

type Endpoint struct {
	RoutingKey  string
	Queue       string
	session     *Session
	emitter     *emission.Emitter
	Data        chan Event
	Ready       chan bool
	DataHandler EndpointDataHandler
}

type EndpointOptions struct {
	RoutingKey  string
	Queue       string
	DataHandler EndpointDataHandler
}

type EndpointDataHandler func(Event) (interface{}, interface{})

func createEndpoint(session *Session, options EndpointOptions) Endpoint {
	endpoint := Endpoint{
		RoutingKey:  options.RoutingKey,
		Queue:       options.Queue,
		session:     session,
		emitter:     emission.NewEmitter(),
		Data:        make(chan Event),
		Ready:       make(chan bool),
		DataHandler: options.DataHandler,
	}

	go endpoint.setup()

	return endpoint
}

func (endpoint Endpoint) setup() {
	queue, err := endpoint.session.workChannel.QueueDeclare(
		endpoint.Queue, // name of the queue
		true,           // durable
		false,          // autoDelete
		false,          // exclusive
		false,          // noWait
		nil,            // arguments
	)

	failOnError(err, "Could not create endpoint queue")
	endpoint.Queue = queue.Name
	fmt.Println("Declared queue", endpoint.Queue)

	err = endpoint.session.workChannel.QueueBind(
		endpoint.Queue,      // name of the queue
		endpoint.RoutingKey, // routing key to use
		"remit",             // exchange
		false,               // noWait
		nil,                 // arguments
	)

	failOnError(err, "Could not bind queue to routing key")
	fmt.Println("Bound", endpoint.Queue, "to routing key", endpoint.RoutingKey)
	fmt.Println("Starting consumption")

	deliveries, err := endpoint.session.consumeChannel.Consume(
		endpoint.Queue, // name of the queue
		"",             // consumer tag
		false,          // noAck
		false,          // exclusive
		false,          // noLocal
		false,          // noWait
		nil,            // arguments
	)

	failOnError(err, "Failed trying to consume")
	fmt.Println("Consuming messages")

	go func() {
		for event := range endpoint.Data {
			retResult, retErr := endpoint.DataHandler(event)

			fmt.Println("Got result...")
			fmt.Println("Got error", retErr)

			var accumulatedResults [2]interface{}
			accumulatedResults[0] = retErr
			accumulatedResults[1] = retResult

			j, err := json.Marshal(accumulatedResults)
			failOnError(err, "Failed making JSON from result")

			// fmt.Println(event.message)

			if event.message.ReplyTo == "" || event.message.CorrelationId == "" {
				event.message.Ack(false)

				return
			}

			queue, err = endpoint.session.workChannel.QueueDeclarePassive(
				event.message.ReplyTo, // the queue to assert
				false, // durable
				true,  // autoDelete
				true,  //exclusive
				false, // noWait
				nil,   // arguments
			)

			failOnError(err, "Reply queue just not there")

			err = endpoint.session.publishChannel.Publish(
				"",         // exchange - use default here to publish directly to queue
				queue.Name, // routing key / queue
				false,      // mandatory
				false,      // immediate
				amqp.Publishing{
					Headers:       amqp.Table{},
					ContentType:   "application/json",
					Body:          j,
					Timestamp:     time.Now(),
					MessageId:     event.message.MessageId,
					AppId:         *endpoint.session.Config.Name,
					CorrelationId: event.message.CorrelationId,
				},
			)

			failOnError(err, "Couldn't send that message")

			event.message.Ack(false)
		}
	}()

	go messageHandler(endpoint, deliveries)

	// Have made this non-blocking (so will ignore if
	// no ready listener is set up).
	// Do we want this? Or should we just return ready
	// whenever the listener is set up?
	select {
	case endpoint.Ready <- true:
	default:
		fmt.Println("No ready listener to hear")
	}
}

func messageHandler(endpoint Endpoint, deliveries <-chan amqp.Delivery) {
	for d := range deliveries {
		parsedData := EventData{}
		err := json.Unmarshal(d.Body, &parsedData)
		failOnError(err, "Failed to parse JSON")

		event := Event{
			EventId:   d.MessageId,
			EventType: d.RoutingKey,
			Resource:  d.AppId,
			Data:      parsedData,
			message:   d,
		}

		endpoint.Data <- event
	}
}
