package remit

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

// Endpoint manages the RPC-style consumption and
// publishing of messages.
type Endpoint struct {
	session     *Session
	channel     *amqp.Channel
	waitGroup   sync.WaitGroup
	consumerTag string

	RoutingKey string
	Queue      string

	Data  chan Event
	Ready chan bool

	DataHandler EndpointDataHandler
}

type EndpointOptions struct {
	RoutingKey string
	Queue      string

	DataHandler EndpointDataHandler
}

type EndpointDataHandler func(Event)

func createEndpoint(session *Session, options EndpointOptions) Endpoint {
	endpoint := Endpoint{
		RoutingKey:  options.RoutingKey,
		Queue:       options.Queue,
		session:     session,
		Data:        make(chan Event),
		DataHandler: options.DataHandler,
		waitGroup:   sync.WaitGroup{},
	}

	go endpoint.start()

	return endpoint
}

func (endpoint Endpoint) start() {
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

	err = endpoint.session.workChannel.QueueBind(
		endpoint.Queue,      // name of the queue
		endpoint.RoutingKey, // routing key to use
		"remit",             // exchange
		false,               // noWait
		nil,                 // arguments
	)

	failOnError(err, "Could not bind queue to routing key")

	endpoint.channel, err = endpoint.session.connection.Channel()
	failOnError(err, "Failed to create channel for consumption")

	endpoint.consumerTag = uuid.New().String()
	deliveries, err := endpoint.channel.Consume(
		endpoint.Queue,       // name of the queue
		endpoint.consumerTag, // consumer tag
		false,                // noAck
		false,                // exclusive
		false,                // noLocal
		false,                // noWait
		nil,                  // arguments
	)

	failOnError(err, "Failed trying to consume")

	go func() {
		for event := range endpoint.Data {
			go handleData(endpoint, event)
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
	}
}

func (endpoint Endpoint) Close() {
	err := endpoint.channel.Cancel(endpoint.consumerTag, false)
	failOnError(err, "Failed to clean up consume channel handler")
	endpoint.waitGroup.Wait()
	err = endpoint.channel.Close()
	failOnError(err, "Failed to close consume channel for endpoint")
	close(endpoint.Data)
	close(endpoint.Ready)
}

func handleData(endpoint Endpoint, event Event) {
	endpoint.session.waitGroup.Add(1)
	defer endpoint.session.waitGroup.Done()
	endpoint.waitGroup.Add(1)
	defer endpoint.waitGroup.Done()

	var retResult interface{}
	var retErr interface{}

	fmt.Println("Received", event.message.MessageId)

	go endpoint.DataHandler(event)

	select {
	case retResult = <-event.Success:
	case retErr = <-event.Failure:
	}

	close(event.Success)
	close(event.Failure)

	if retResult != nil {
		fmt.Println("Completed", event.message.MessageId, "- success")
	} else {
		fmt.Println("Completed", event.message.MessageId, "- failure")
	}

	var accumulatedResults [2]interface{}
	accumulatedResults[0] = retErr
	accumulatedResults[1] = retResult

	j, err := json.Marshal(accumulatedResults)
	failOnError(err, "Failed making JSON from result")

	if event.message.ReplyTo == "" || event.message.CorrelationId == "" {
		event.message.Ack(false)

		return
	}

	queue, err := endpoint.session.workChannel.QueueDeclarePassive(
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
			AppId:         endpoint.session.Config.Name,
			CorrelationId: event.message.CorrelationId,
		},
	)

	failOnError(err, "Couldn't send that message")

	event.message.Ack(false)
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
			Success:   make(chan interface{}, 1),
			Failure:   make(chan interface{}, 1),
			message:   d,
		}

		endpoint.Data <- event
	}
}
