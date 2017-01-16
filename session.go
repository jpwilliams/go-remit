package remit

import (
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/streadway/amqp"
)

type Session struct {
	connection     *amqp.Connection
	workChannel    *amqp.Channel
	publishChannel *amqp.Channel

	waitGroup sync.WaitGroup

	Config         Config
	EndpointGlobal EndpointGlobal
}

func (session *Session) CloseOnSignal() chan bool {
	ch := make(chan bool)

	go func() {
		c := make(chan os.Signal, 2)
		signal.Notify(c)
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

func (session *Session) Endpoint(key string, handler EndpointDataHandler) Endpoint {
	endpoint := createEndpoint(session, EndpointOptions{
		RoutingKey:  key,
		Queue:       key,
		DataHandler: handler,
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

	return Endpoint{
		RoutingKey: options.RoutingKey,
		Queue:      options.Queue,
		session:    session,
	}
}

func (session *Session) Emit(key string) Emit {
	if key == "" {
		panic("No valid routing key given for emission")
	}

	emit := createEmit(session, EmitOptions{
		RoutingKey: key,
	})

	return emit
}
