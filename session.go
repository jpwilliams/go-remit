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

func (session *Session) registerReply(correlationId string, returnChannel chan Event) {
	session.awaitingReply[correlationId] = returnChannel
}

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

func (session *Session) Endpoint(key string) Endpoint {
	endpoint := createEndpoint(session, EndpointOptions{
		RoutingKey:  key,
		Queue:       key,
		shouldReply: true,
	})

	return endpoint
}

func (session *Session) EndpointWithOptions(options EndpointOptions) Endpoint {
	if options.Queue == "" && options.RoutingKey == "" {
		panic("No queue or routing key given")
	}

	if options.RoutingKey == "" {
		if options.Queue != "" {
			options.RoutingKey = options.Queue
		} else {
			panic("No routing key (explicit or implicit) given")
		}
	}

	endpoint := createEndpoint(session, EndpointOptions{
		RoutingKey:  options.RoutingKey,
		Queue:       options.Queue,
		shouldReply: true,
	})

	return endpoint
}

func (session *Session) LazyEndpoint(key string, handlers ...EndpointDataHandler) Endpoint {
	if len(handlers) == 0 {
		panic("No handlers given for lazy endpoint instantiation")
	}

	endpoint := session.Endpoint(key)
	endpoint.OnData(handlers...)
	endpoint.Open()

	return endpoint
}

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
