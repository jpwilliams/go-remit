package remit

import (
	"log"

	"github.com/chuckpreslar/emission"
	"github.com/streadway/amqp"
)

type ConnectionOptions struct {
	Url  string
	Name string
}

func Connect(options ConnectionOptions) Session {
	conn, err := amqp.Dial(options.Url)
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
			Name: options.Name,
			Url:  options.Url,
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
