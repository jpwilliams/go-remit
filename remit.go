package remit

import (
	"log"
	"sync"

	"github.com/streadway/amqp"
	. "github.com/tj/go-debug"
)

var debug = Debug("remit")

// J is a convenient aliaas for a `map[string]interface{}`, useful for dealing with
// JSON is a more native manner.
//
// 	remit.J{
// 		"foo": "bar",
// 		"baz": true,
// 		"qux": remit.J{
// 			"big": false,
// 			"small": true,
// 		},
// 	}
//
type J map[string]interface{}

// Connect connects to a RabbitMQ instance using the `Url` provided in
// `ConnectionOptions` and setting the _service name_ to `Name`.
//
// The `Url` should be valid as defined by the AMQP URI scheme. Currently,
// this is:
//
// 	amqp_URI       = "amqp://" amqp_authority [ "/" vhost ] [ "?" query ]
// 	amqp_authority = [ amqp_userinfo "@" ] host [ ":" port ]
// 	amqp_userinfo  = username [ ":" password ]
// 	username       = *( unreserved / pct-encoded / sub-delims )
// 	password       = *( unreserved / pct-encoded / sub-delims )
// 	vhost          = segment
//
// More info can be found here:
//
// 	https://www.rabbitmq.com/uri-spec.html
//
// Example:
//
//	remitSession := remit.Connect(remit.ConnectionOptions{
//		Name: "my-service",
//		Url: "amqp://localhost"
//	})
//
func Connect(options ConnectionOptions) Session {
	debug("connecting to amq")

	conn, err := amqp.Dial(options.Url)
	failOnError(err, "Failed to connect to RabbitMQ")

	closing := conn.NotifyClose(make(chan *amqp.Error))

	go func() {
		for cl := range closing {
			log.Println("Closed", cl.Reason)
		}
	}()

	setupChannel, err := conn.Channel()
	failOnError(err, "Failed to open work channel")

	err = setupChannel.ExchangeDeclare(
		"remit", // name of the exchange
		"topic", // type
		true,    // durable
		true,    // autoDelete
		false,   // internal
		false,   // noWait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare \"remit\" exchange")
	setupChannel.Close()

	publishChannel, err := conn.Channel()
	failOnError(err, "Failed to open publish channel")

	requestChannel, err := conn.Channel()
	failOnError(err, "Failed to open replies channel")

	session := Session{
		Config: Config{
			Name: options.Name,
			Url:  options.Url,
		},

		connection:     conn,
		publishChannel: publishChannel,
		requestChannel: requestChannel,

		waitGroup:     &sync.WaitGroup{},
		mu:            &sync.Mutex{},
		awaitingReply: make(map[string]chan Event),
		workerPool:    newWorkerPool(1, 5, conn),
	}

	replies, err := requestChannel.Consume(
		"amq.rabbitmq.reply-to", // name of the queue
		"",    // consumer tag
		true,  // noAck
		true,  // exclusive
		false, // noLocal
		false, // noWait
		nil,   // arguments
	)
	failOnError(err, "Failed to consume replies")

	go session.watchForReplies(replies)

	return session
}
