package remit

import (
	"encoding/json"
	"time"

	"github.com/oklog/ulid"
	"github.com/streadway/amqp"
)

type Request struct {
	session *Session

	RoutingKey string
}

type RequestOptions struct {
	RoutingKey string
}

func createRequest(session *Session, options RequestOptions) Request {
	request := Request{
		RoutingKey: options.RoutingKey,
		session:    session,
	}

	return request
}

func (request *Request) Send(data interface{}) chan Event {
	j, err := json.Marshal(data)
	failOnError(err, "Failed making JSON from result")

	receiveChannel := make(chan Event, 1)
	messageId := ulid.MustNew(ulid.Now(), nil).String()
	request.session.registerReply(messageId, receiveChannel)

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

	return receiveChannel
}
