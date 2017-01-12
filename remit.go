package remit

import (
	"flag"
	"log"

	"github.com/chuckpreslar/emission"
	"github.com/streadway/amqp"
)

var url = flag.String("url", "amqp:///", "The AMQP URL to connect to.")
var name = flag.String("name", "", "The name to give this Remit service.")

func init() {
	flag.Parse()
}

func Connect() Session {
	conn, err := amqp.Dial(*url)
	failOnError(err, "Failed to connect to RabbitMQ")

	closing := conn.NotifyClose(make(chan *amqp.Error))

	go func() {
		for cl := range closing {
			log.Println("Closed", cl.Reason)
		}
	}()

	workChannel, err := conn.Channel()
	failOnError(err, "Failed to open work channel")

	err = workChannel.ExchangeDeclare(
		"remit", // name of the exchange
		"topic", // type
		true,    // durable
		true,    // autoDelete
		false,   // internal
		false,   // noWait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare \"remit\" exchange")

	publishChannel, err := conn.Channel()
	failOnError(err, "Failed to open publish channel")

	consumeChannel, err := conn.Channel()
	failOnError(err, "Failed to open consume channel")

	return Session{
		Config: Config{
			Name: name,
			Url:  url,
		},

		connection:     conn,
		workChannel:    workChannel,
		publishChannel: publishChannel,
		consumeChannel: consumeChannel,
		emitter:        emission.NewEmitter(),

		EndpointGlobal: EndpointGlobal{
			emitter: emission.NewEmitter(),
		},
	}
}

func createChannel(connection *amqp.Connection) *amqp.Channel {
	channel, err := connection.Channel()
	failOnError(err, "Failed to create channel")

	closing := channel.NotifyClose(make(chan *amqp.Error))

	go func() {
		for cl := range closing {
			log.Println("Closed", cl.Reason)
		}
	}()

	return channel
}
