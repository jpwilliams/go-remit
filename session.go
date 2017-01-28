package remit

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/oklog/ulid"
	"github.com/streadway/amqp"
)

type Session struct {
	connection     *amqp.Connection
	publishChannel *amqp.Channel
	requestChannel *amqp.Channel
	awaitingReply  map[string]RequestDataHandler

	waitGroup *sync.WaitGroup

	Config Config
}

func (session *Session) registerReply(correlationId string, handler RequestDataHandler) {
	session.awaitingReply[correlationId] = handler
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
		RoutingKey: key,
		Queue:      key,
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

func (session *Session) Emit(key string, data interface{}) {
	if key == "" {
		panic("No valid routing key given for emission")
	}

	session.waitGroup.Add(1)
	defer session.waitGroup.Done()

	j, err := json.Marshal(data)
	failOnError(err, "Failed making JSON from emission data")

	message := amqp.Publishing{
		Headers:     amqp.Table{},
		ContentType: "application/json",
		Body:        j,
		Timestamp:   time.Now(),
		MessageId:   ulid.MustNew(ulid.Now(), nil).String(),
		AppId:       session.Config.Name,
	}

	err = session.publishChannel.Publish(
		"remit", // exchange
		key,     // routing key
		false,   // mandatory
		false,   // immediate
		message, // message
	)

	failOnError(err, "Failed to emit message")
}

func (session *Session) Request(key string, data interface{}, handler RequestDataHandler) Request {
	request := createRequest(session, RequestOptions{
		RoutingKey:  key,
		DataHandler: handler,
		Data:        data,
	})

	return request
}
