package remit

import (
	"encoding/json"
	"time"

	"github.com/oklog/ulid"
	"github.com/streadway/amqp"
)

// Request represents an RPC request for data.
//
// Most commonly, this is used to contact another service to retrieve
// data, utilising a `Session.Endpoint`.
//
// For examples of Request usage, see `Session.Request` and `Session.LazyRequest`.
type Request struct {
	RoutingKey string

	session *Session
}

// RequestOptions is a list of options that can be passed when setting up
// a request.
type RequestOptions struct {
	RoutingKey string
}

// Send sends some data to a previously-set-up `Request` using `Session.Request`.
// It returns a channel on which a single reply `Event` will be passed upon RPC completion.
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

func createRequest(session *Session, options RequestOptions) Request {
	request := Request{
		RoutingKey: options.RoutingKey,
		session:    session,
	}

	return request
}
