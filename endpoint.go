package remit

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/oklog/ulid"
	"github.com/streadway/amqp"
)

// Endpoint manages the RPC-style consumption and
// publishing of messages.
//
// Most commonly, this is used to set up an endpoint that can be requested
// using `Session.Request` or `Session.LazyRequest`.
//
// For examples of Endpoint usage, see `Session.Endpoint` and `Session.LazyEndpoint`.
type Endpoint struct {
	// given properties
	RoutingKey string
	Queue      string

	// generated properties
	Data  chan Event
	Ready chan bool

	session       *Session
	channel       *amqp.Channel
	waitGroup     *sync.WaitGroup
	mu            *sync.Mutex
	consumerTag   string
	dataListeners []chan Event
	shouldReply   bool
}

// EndpointOptions is a list of options that can be passed when setting up an endpoint.
type EndpointOptions struct {
	RoutingKey string
	Queue      string

	shouldReply bool
}

// EndpointDataHandler is the function spec needed for listening to endpoint data.
type EndpointDataHandler func(Event)

// Close closes the endpoint, stopping message consumption and closing the endpoint's
// receiving channel.
//
// It will cancel consumption, but wait for all unacked messages to be handled
// before closing the channel, meaning no loss should occur.
//
// The endpoint can be reopened using `Endpoint.Open`.
func (endpoint Endpoint) Close() {
	err := endpoint.channel.Cancel(endpoint.consumerTag, false)
	failOnError(err, "Failed to cancel consume channel for endpoint")
	endpoint.waitGroup.Wait()
	err = endpoint.channel.Close()
	failOnError(err, "Failed to close consume channel for endpoint")
	endpoint.channel = nil
	close(endpoint.Data)
	close(endpoint.Ready)
}

// OnData is used to register a data handler for a particular endpoint.
// A data handler here is a function (or set of functions) to run whenever
// a new message is received.
//
// If multiple handlers are provided in sequence, earlier functions will act as
// "middleware", used primarily for the mutation of the `Event` or the like.
//
// An example of this might be:
//
// 	endpoint := remitSession.Endpoint("math.sum")
// 	endpoint.OnData(logMessage, parseArguments, handle)
//
// In the above example, `logMessage`, `parseArguments` and `handle` will be run
// in sequence. If any of the functions push data to either `Event.Success` or
// `Event.Failure`, the chain is broken and the message immediately replied to.
// Otherwise, sending `true` to `Event.Next` should be performed to indicate that
// it's safe to move to the next step.
//
// If `Event.Next` is pushed to on the final handler, the message will be treated
// as successful but the reply will contain no data.
func (endpoint *Endpoint) OnData(handlers ...EndpointDataHandler) {
	if len(handlers) == 0 {
		panic("Failed to create endpoint data handler with no functions")
	}

	dataChan := make(chan Event)
	endpoint.mu.Lock()
	endpoint.dataListeners = append(endpoint.dataListeners, dataChan)
	endpoint.mu.Unlock()

	go func() {
		for event := range dataChan {
			go handleData(*endpoint, handlers, event)
		}
	}()
}

// Open the endpoint to messages, starting consumption and pushing `true` to
// `Endpoint.Ready` upon completion.
//
// The recommendation here is to ensure any and all data handlers are registered
// before opening the endpoint up.
func (endpoint *Endpoint) Open() {
	endpoint.Data = make(chan Event)
	endpoint.Ready = make(chan bool)

	workChannel := endpoint.session.workerPool.get()
	queue, err := workChannel.QueueDeclare(
		endpoint.Queue, // name of the queue
		true,           // durable
		false,          // autoDelete
		false,          // exclusive
		false,          // noWait
		nil,            // arguments
	)
	failOnError(err, "Could not create endpoint queue")
	endpoint.Queue = queue.Name

	err = workChannel.QueueBind(
		endpoint.Queue,      // name of the queue
		endpoint.RoutingKey, // routing key to use
		"remit",             // exchange
		false,               // noWait
		nil,                 // arguments
	)
	failOnError(err, "Could not bind queue to routing key")

	endpoint.session.workerPool.release(workChannel)

	endpoint.channel, err = endpoint.session.connection.Channel()
	failOnError(err, "Failed to create channel for consumption")

	// watch for consume channel closure
	waitForClose := make(chan *amqp.Error, 0)
	endpoint.channel.NotifyClose(waitForClose)

	go func() {
		err := <-waitForClose
		panic(err)
	}()

	endpoint.consumerTag = ulid.MustNew(ulid.Now(), nil).String()
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

	go messageHandler(*endpoint, deliveries)

	// Have made this non-blocking (so will ignore if
	// no ready listener is set up).
	// Do we want this? Or should we just return ready
	// whenever the listener is set up?
	select {
	case endpoint.Ready <- true:
	default:
	}
}

func createEndpoint(session *Session, options EndpointOptions) Endpoint {
	debug("creating endpoint")

	endpoint := Endpoint{
		RoutingKey:  options.RoutingKey,
		Queue:       options.Queue,
		session:     session,
		Data:        make(chan Event),
		Ready:       make(chan bool),
		waitGroup:   &sync.WaitGroup{},
		mu:          &sync.Mutex{},
		shouldReply: options.shouldReply,
	}

	return endpoint
}

func handleData(endpoint Endpoint, handlers []EndpointDataHandler, event Event) {
	endpoint.session.waitGroup.Add(1)
	defer endpoint.session.waitGroup.Done()
	endpoint.waitGroup.Add(1)
	defer endpoint.waitGroup.Done()
	event.waitGroup.Add(1)
	defer event.waitGroup.Done()

	var retResult interface{}
	var retErr interface{}

runner:
	for _, handler := range handlers {
		go handler(event)

		select {
		case retResult = <-event.Success:
			break runner
		case retErr = <-event.Failure:
			break runner
		case <-event.Next:
		}
	}

	if retErr != nil {
		debug("failure" + event.message.MessageId)
	} else {
		debug("success " + event.message.MessageId)
	}

	if !endpoint.shouldReply || event.message.ReplyTo == "" || event.message.CorrelationId == "" {
		event.message.Ack(false)
		return
	}

	var accumulatedResults [2]interface{}
	accumulatedResults[0] = retErr
	accumulatedResults[1] = retResult

	j, err := json.Marshal(accumulatedResults)
	failOnError(err, "Failed making JSON from result")

	// fmt.Println(event.message.DeliveryTag, "queuing")
	// fmt.Println(event.message.DeliveryTag, "checking")
	workChannel := endpoint.session.workerPool.get()
	queue, err := workChannel.QueueDeclarePassive(
		event.message.ReplyTo, // the queue to assert
		false, // durable
		true,  // autoDelete
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		endpoint.session.workerPool.drop(workChannel)
		fmt.Println("Reply consumer no longer present; skipping", err)
		event.message.Ack(false)
		return
	}

	endpoint.session.workerPool.release(workChannel)

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
			MessageId:     ulid.MustNew(ulid.Now(), nil).String(),
			AppId:         endpoint.session.Config.Name,
			CorrelationId: event.message.CorrelationId,
		},
	)

	failOnError(err, "Couldn't send that message")

	event.message.Ack(false)
}

func messageHandler(endpoint Endpoint, deliveries <-chan amqp.Delivery) {
	for d := range deliveries {
		var parsedData EventData
		err := json.Unmarshal(d.Body, &parsedData)
		if err != nil {
			fmt.Println("Failed to parse JSON " + d.MessageId)
			fmt.Println(err)
			d.Nack(false, false)
			continue
		}

		event := Event{
			EventId:   d.MessageId,
			EventType: d.RoutingKey,
			Resource:  d.AppId,
			Data:      parsedData,
			Success:   make(chan interface{}, 1),
			Failure:   make(chan interface{}, 1),
			Next:      make(chan bool, 1),

			message:   d,
			waitGroup: &sync.WaitGroup{},
		}

		event.waitGroup.Add(len(endpoint.dataListeners))

		go func() {
			event.waitGroup.Wait()
			close(event.Success)
			close(event.Failure)
			close(event.Next)
		}()

		for _, listener := range endpoint.dataListeners {
			listener <- event
		}
	}
}
