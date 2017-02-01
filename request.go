package remit

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/oklog/ulid"
	"github.com/streadway/amqp"
)

type Request struct {
	returnChannel chan Event
	session       *Session
	waitGroup     *sync.WaitGroup

	RoutingKey string

	DataHandler RequestDataHandler
}

type RequestOptions struct {
	returnChannel chan Event

	RoutingKey string
	Data       interface{}

	DataHandler RequestDataHandler
}

type RequestDataHandler func(Event)

func createRequest(session *Session, options RequestOptions) Request {
	request := Request{
		RoutingKey:    options.RoutingKey,
		session:       session,
		returnChannel: make(chan Event, 1),
	}

	go request.send(options.Data)

	return request
}

func (request Request) send(data interface{}) {
	j, err := json.Marshal(data)
	failOnError(err, "Failed making JSON from result")

	messageId := ulid.MustNew(ulid.Now(), nil).String()
	request.session.registerReply(messageId, request.returnChannel)

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
