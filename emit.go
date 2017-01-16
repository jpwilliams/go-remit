package remit

import (
	// "log"
	"time"

	"github.com/chuckpreslar/emission"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

type Emit struct {
	RoutingKey string
	session    *Session
	emitter    *emission.Emitter
}

type EmitOptions EndpointOptions

func createEmit(session *Session, options EmitOptions) Emit {
	return Emit{
		RoutingKey: options.RoutingKey,
		session:    session,
		emitter:    emission.NewEmitter(),
	}
}

func (request Emit) Sent(handler func()) Emit {
	request.emitter.On("Sent", handler)

	return request
}

func (request Emit) Send(data []byte) Emit {
	message := amqp.Publishing{
		Headers:     amqp.Table{},
		ContentType: "application/json",
		Body:        data,
		Timestamp:   time.Now(),
		MessageId:   uuid.New().String(),
		AppId:       request.session.Config.Name,
	}

	err := request.session.publishChannel.Publish(
		"remit",            // exchange
		request.RoutingKey, // routing key
		false,              // mandatory
		false,              // immediate
		message,            // message
	)

	failOnError(err, "Failed to emit message")

	request.emitter.Emit("Sent")

	return request
}
