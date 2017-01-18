package remit

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

type Request struct {
	session   *Session
	waitGroup *sync.WaitGroup

	RoutingKey string

	DataHandler RequestDataHandler
}

type RequestOptions struct {
	RoutingKey string
	Data       interface{}

	DataHandler RequestDataHandler
}

type RequestDataHandler func(Event)

func createRequest(session *Session, options RequestOptions) Request {
	request := Request{
		RoutingKey:  options.RoutingKey,
		session:     session,
		DataHandler: options.DataHandler,
	}

	go request.send(options.Data)

	return request
}

func (request Request) send(data interface{}) {
	j, err := json.Marshal(data)
	failOnError(err, "Failed making JSON from result")

	messageId := uuid.New().String()
	request.session.registerReply(messageId, request.DataHandler)

	err = request.session.requestChannel.Publish(
		"remit",            // exchange
		request.RoutingKey, // routing key / queue
		false,              // mandatory
		false,              // immediate
		amqp.Publishing{
			Headers:       amqp.Table{},
			ContentType:   "application/json",
			Body:          j,
			Timestamp:     time.Now(),
			MessageId:     messageId,
			AppId:         request.session.Config.Name,
			CorrelationId: messageId,
			ReplyTo:       "amq.rabbitmq.reply-to",
		},
	)
	failOnError(err, "Failed to send request message")
}
