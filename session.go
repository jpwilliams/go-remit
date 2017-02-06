package remit

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/streadway/amqp"
)

// Config represents a collection of options used to connect to
// the RabbitMQ server, as well as some Remit-specific options such as
// the service name.
type Config struct {
	Name string
	Url  string
}

// ConnectionOptions is the options used to connect to RabbitMQ and
// any connection-wide settings needed for Remit.
type ConnectionOptions struct {
	Url  string
	Name string
}

// Session represents a communication session with RabbitMQ.
//
// Most commonly, a single Remit session is used for an entire service,
// as separate channels are generated for each endpoint.
type Session struct {
	// the config given for this connection
	Config Config

	connection     *amqp.Connection
	publishChannel *amqp.Channel
	requestChannel *amqp.Channel
	awaitingReply  map[string]chan Event
	workerPool     *workerPool
	listenerCount  int

	waitGroup *sync.WaitGroup
	mu        *sync.Mutex
}

// Close closes the Remit session by waiting for all unacknowledged messages to be
// handled before closing the RabbitMQ connection and pushing `true` to the returned
// channel.
//
// Example:
//
// 	remitSession := remit.Connect(...)
// 	...
// 	<-remitSession.Close()
//
func (session *Session) Close() chan bool {
	ch := make(chan bool)
	logClosure()

	go func() {
		session.waitGroup.Wait()
		err := session.connection.Close()
		failOnError(err, "Failed to close connection to RabbitMQ safely")
		log.Println("  [x] Safely closed AMQP connection")
		ch <- true
	}()

	return ch
}

// CloseOnSignal returns a channel that will receive either `true` or `false`
// depending on whether Remit closed its connections safely as a result of
// an interruption signal.
//
// The receipt of a signal causes the session to close via `Session.Close()`.
// A second signal being sent whilst the close is in progress will perform a
// "cold" shutdown, dismissing any unacknowledged messages and returning `false`
// to the channel straight away.
//
// The signals currently supported are `1 SIGHUP`, `2 SIGINT`, `3 SIGQUIT` and `15 SIGTERM`.
//
// Example:
//
// 	remitSession := remit.Connect(...)
// 	...
// 	<-remitSession.CloseOnSignal()
//
func (session *Session) CloseOnSignal() chan bool {
	ch := make(chan bool)

	go func() {
		c := make(chan os.Signal, 2)
		signal.Notify(
			c,               // the channel to use
			syscall.SIGHUP,  // Hangup
			syscall.SIGINT,  // Terminal interrupt
			syscall.SIGQUIT, // Terminal quit
			syscall.SIGTERM, // Termination
		)
		<-c
		logClosure()
		go func() {
			session.waitGroup.Wait()
			err := session.connection.Close()
			failOnError(err, "Failed to close connection to RabbitMQ safely")
			log.Println("  [x] Safely closed AMQP connection")
			ch <- true
		}()
		<-c
		log.Println("  [x] Cold shutdown - killing self regardless of message loss...")
		ch <- false
	}()

	return ch
}

// Emit sets up an emitter that messages can be pushed to.
// For a one-liner emission, see `Session.LazyEmit`.
//
// Especially useful for emitting system events or firing off requests if you
// don't care about the response.
//
// `key` will be used as a routing key and emissions are always published to the
// `"remit"` exchange.
//
// Example:
//
// 	remitSession := remit.Connect(...)
//
// 	emitter := remitSession.Emit("service.connected")
// 	emitter <- "my-service-id"
//
// 	// is synonymous with
// 	remitSession.LazyEmit("service.connected", "my-service-id")
//
func (session *Session) Emit(key string) chan interface{} {
	emit := createEmission(session, EmitOptions{
		RoutingKey: key,
	})

	return emit.Channel
}

// Endpoint creates an endpoint for `key` but does not start consuming.
// For a one-liner endpoint, see `Session.LazyEndpoint`.
//
// Using this, the usual set-up is to also add a data handler and then open
// the endpoint for messages:
//
// 	endpoint := remitSession.Endpoint("math.sum")
// 	endpoint.OnData(sumHandler)
// 	endpoint.Open()
//
// This would be synonymous with `Session.LazyEndpoint`'s:
//
// 	endpoint := remitSession.LazyEndpoint("math.sum", sumHandler)
//
// When this endpoint is created, both the `RoutingKey` and `Queue` will be set to
// the provided `key`. If you'd like to specify them as separate entities, see
// `Session.EndpointWithOptions`.
//
// Example:
//
// 	remitSession := remit.Connect(...)
//
// 	endpoint := remitSession.Endpoint("math.sum")
// 	endpoint.OnData(sumHandler)
// 	endpoint.Open()
//
func (session *Session) Endpoint(key string) Endpoint {
	endpoint := createEndpoint(session, EndpointOptions{
		RoutingKey:  key,
		Queue:       key,
		shouldReply: true,
	})

	return endpoint
}

// EndpointWithOptions allows you to create an endpoint with very particular
// options, described in the `EndpointOptions` type.
//
// Failure to provide either a `Queue` or a `Routing` key results in a panic.
// If one is given and not the other, the value will be duplicated.
//
// Example:
//
// 	remitSession := remit.Connect(...)
//
// 	endpoint := EndpointWithOptions(remit.EndpointOptions{
// 		RoutingKey: "maths",
//		Queue: "math.sum",
// 	})
//
func (session *Session) EndpointWithOptions(options EndpointOptions) Endpoint {
	if options.Queue == "" && options.RoutingKey == "" {
		panic("No queue or routing key given")
	}

	if options.RoutingKey == "" && options.Queue != "" {
		options.RoutingKey = options.Queue
	}

	if options.Queue == "" && options.RoutingKey != "" {
		options.Queue = options.RoutingKey
	}

	endpoint := createEndpoint(session, EndpointOptions{
		RoutingKey:  options.RoutingKey,
		Queue:       options.Queue,
		shouldReply: true,
	})

	return endpoint
}

// LazyEmit immediately publishes a message using `Session.Emit` using the given routing
// key and data.
//
// Example:
//
// 	remitSession := remit.Connect(...)
//
// 	remitSession.LazyEmit("service.connceted", "my-service-id")
//
func (session *Session) LazyEmit(key string, data interface{}) {
	emit := createEmission(session, EmitOptions{
		RoutingKey: key,
	})

	emit.Channel <- data
}

// LazyEndpoint is a lazy, one-liner version of `Session.Endpoint`.
//
// It creates an endpoint via `Session.Endpoint`, adds the ordered data handlers given
// in its arguments via `Endpoint.OnData` and then opens the endpoint via `Endpoint.Open()`.
//
// Like `Endpoint.OnData`, `Session.LazyEndpoint` is a variadic method, meaning it
// handles multiple data handlers, having them act as ordered middleware.
//
// Example:
//
// 	remitSession := remit.Connect(...)
//
// 	endpoint := remitSession.LazyEndpoint("math.sum", sumHandler)
//
func (session *Session) LazyEndpoint(key string, handlers ...EndpointDataHandler) Endpoint {
	if len(handlers) == 0 {
		panic("No handlers given for lazy endpoint instantiation")
	}

	endpoint := session.Endpoint(key)
	endpoint.OnData(handlers...)
	endpoint.Open()

	return endpoint
}

// LazyListener is a lazy, one-liner version of `Listener.
//
// It creates a listener via `Session.Listener`, adds the ordered data handlers
// given in its arguments via `Endpoint.OnData` and then opens the endpoint via
// `Endpoint.Open()`.
//
// Like `Endpoint.OnData`, `Session.LazyListener` is a variadic method, meaning it
// handles multiple data handlers, having them act as ordered middlware.
//
// Example:
//
// 	remitSession := remit.Connect(...)
//
// 	listener := remitSession.LazyListener("user.created", logUserDetails)
//
func (session *Session) LazyListener(key string, handlers ...EndpointDataHandler) Endpoint {
	if len(handlers) == 0 {
		panic("No handlers given for lazy listener instantiation")
	}

	listener := session.Listener(key)
	listener.OnData(handlers...)
	listener.Open()

	return listener
}

// LazyRequest is a lazy, one-liner version of `Session.Request`.
//
// It creates a requset via `Session.Request` and immediately sends the data provided
// via `Request.Send`, returning the channel on which the reply will appear.
//
// Example
//
// 	remitSession := remit.Connect(...)
//
// 	event := <-remitSession.LazyRequest("math.sum", remit.J{"numbers": []int{1, 5, 7}})
//
func (session *Session) LazyRequest(key string, data interface{}) chan Event {
	request := createRequest(session, RequestOptions{
		RoutingKey: key,
	})

	return request.Send(data)
}

// Listener creates a listener for `key` but does not start consuming.
// For a one-liner listener, see `Session.LazyListener`.
//
// In terms of the API, this is essentially the same as a `Request` but it
// will never reply to messages received.
//
// In terms of its place within Remit, listeners are used to "listen" for
// events and react. Listeners with the same `key` and `Remit.Name` value
// will have requests round-robin'd between them so multiple services can handle
// listeners as a single unit.
//
// A usual practice is to have listeners listen for events such as `"user.created"`
// or `"message.deleted"`, though they can also listen directly on existing endpoint
// keys without disturbing any other flows.
//
// Example:
//
// 	remitSession := remit.Connect(...)
//
// 	listener := remitSession.Listener("user.created")
// 	listener.OnData(logUserDetails)
// 	listener.Open()
//
func (session *Session) Listener(key string) Endpoint {
	session.mu.Lock()
	session.listenerCount = session.listenerCount + 1
	queue := key + ":l:" + session.Config.Name + ":" + strconv.Itoa(session.listenerCount)
	session.mu.Unlock()

	listener := createEndpoint(session, EndpointOptions{
		RoutingKey:  key,
		Queue:       queue,
		shouldReply: false,
	})

	return listener
}

// Request creates a request with the routing key of `key` but does not
// immediately send.
// For a one-liner request, see `Session.LazyRequest`.
//
// When requesting data, a channel is returned that you can pull data out of.
// Only a single event will be returned for each request made.
// A synchronous pattern for this would be:
//
// 	request := remitSession.Request("math.sum")
// 	event := <-request.Send(remit.J{"numbers": []int{1, 5, 7}})
//
// This would be synonymous with `Session.LazyRequest`'s:
//
// 	event := <-remitSession.LazyRequest("math.sum", remit.J{"numbers": []int{1, 5, 7}})
//
func (session *Session) Request(key string) Request {
	request := createRequest(session, RequestOptions{
		RoutingKey: key,
	})

	return request
}

func (session *Session) registerReply(correlationId string, returnChannel chan Event) {
	session.awaitingReply[correlationId] = returnChannel
}

func (session *Session) watchForReplies(replies <-chan amqp.Delivery) {
	for reply := range replies {
		returnChannel := session.awaitingReply[reply.CorrelationId]

		if returnChannel == nil {
			continue
		}

		delete(session.awaitingReply, reply.CorrelationId)

		var parsedData []EventData
		err := json.Unmarshal(reply.Body, &parsedData)
		failOnError(err, "Failed to parse JSON for reply")

		event := Event{
			EventId:   reply.MessageId,
			EventType: reply.RoutingKey,
			Resource:  reply.AppId,
			message:   reply,
		}

		if parsedData[0] != nil {
			event.Error = parsedData[0]
		} else {
			event.Data = parsedData[1]
		}

		select {
		case returnChannel <- event:
		default:
		}
	}
}

func logClosure() {
	log.Println("Initiated Remit closure.")
	log.Println("  [x] Warm shutdown - resolving pending tasks before closing...")
	log.Println("      Cancelling again will initiate a cold shutdown and messages may be lost.")
}
