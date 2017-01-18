package remit

import (
	"encoding/json"
	"log"
	"sync"

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

	replyList := make(map[string]RequestDataHandler)

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

	go func() {
		for reply := range replies {
			handler := replyList[reply.CorrelationId]

			if handler == nil {
				continue
			}

			delete(replyList, reply.CorrelationId)

			parsedData := EventData{}
			json.Unmarshal(reply.Body, &parsedData)
			failOnError(err, "Failed to parse JSON")

			event := Event{
				EventId:   reply.MessageId,
				EventType: reply.RoutingKey,
				Resource:  reply.AppId,
				Data:      parsedData,
				message:   reply,
			}

			go handler(event)
		}
	}()

	return Session{
		Config: Config{
			Name: options.Name,
			Url:  options.Url,
		},

		connection:     conn,
		publishChannel: publishChannel,
		requestChannel: requestChannel,

		waitGroup:     sync.WaitGroup{},
		awaitingReply: replyList,
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
