package remit

import (
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/streadway/amqp"
)

// Session represents a communication session with RabbitMQ.
//
// Most commonly, a single Remit session is used for an entire service,
// as separate channels are generated for each endpoint.
type Session struct {
	connection     *amqp.Connection
	publishChannel *amqp.Channel
	requestChannel *amqp.Channel
	awaitingReply  map[string]chan Event
	workerPool     *workerPool

	waitGroup *sync.WaitGroup
	mu        *sync.Mutex

	listenerCount int

	Config Config
}

// Config represents a collection of options used to connect to
// the RabbitMQ server, as well as some Remit-specific options such as
// the service name.
type Config struct {
	Name string
	Url  string
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

func logClosure() {
	log.Println("Initiated Remit closure.")
	log.Println("  [x] Warm shutdown - resolving pending tasks before closing...")
	log.Println("      Cancelling again will initiate a cold shutdown and messages may be lost.")
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

// Emit immediately publishes a messages using the given routing key and data.
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
// 	remitSession.Emit("service.connected", "my-service-id")
//

func (session *Session) Emit(key string) chan interface{} {
	emit := createEmission(session, EmitOptions{
		RoutingKey: key,
	})

	return emit.Channel
}

func (session *Session) LazyEmit(key string, data interface{}) {
	emit := createEmission(session, EmitOptions{
		RoutingKey: key,
	})

	emit.Channel <- data
}

func (session *Session) Request(key string) Request {
	request := createRequest(session, RequestOptions{
		RoutingKey: key,
	})

	return request
}

func (session *Session) LazyRequest(key string, data interface{}) chan Event {
	request := createRequest(session, RequestOptions{
		RoutingKey: key,
	})

	return request.Send(data)
}

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

func (session *Session) LazyListener(key string, handlers ...EndpointDataHandler) Endpoint {
	if len(handlers) == 0 {
		panic("No handlers given for lazy listener instantiation")
	}

	listener := session.Listener(key)
	listener.OnData(handlers...)
	listener.Open()

	return listener
}

func (session *Session) registerReply(correlationId string, returnChannel chan Event) {
	session.awaitingReply[correlationId] = returnChannel
}
