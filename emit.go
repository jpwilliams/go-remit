package remit

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/oklog/ulid"
	"github.com/streadway/amqp"
)

type Emit struct {
	session *Session
	Channel chan interface{}

	RoutingKey string
}

type EmitOptions struct {
	RoutingKey string
}

func createEmission(session *Session, options EmitOptions) Emit {
	emit := Emit{
		RoutingKey: options.RoutingKey,
		session:    session,
		Channel:    make(chan interface{}),
	}

	go emit.waitForEmissions()

	return emit
}

func (emit *Emit) waitForEmissions() {
	for data := range emit.Channel {
		debug("got emission")

		emit.send(data)
	}

	fmt.Println("finished")
}

func (emit *Emit) send(data interface{}) {
	emit.session.waitGroup.Add(1)
	defer emit.session.waitGroup.Done()

	message := amqp.Publishing{
		Headers:     amqp.Table{},
		ContentType: "application/json",
		Timestamp:   time.Now(),
		MessageId:   ulid.MustNew(ulid.Now(), nil).String(),
		AppId:       emit.session.Config.Name,
	}

	if data != nil {
		j, err := json.Marshal(data)
		failOnError(err, "Failed making JSON from result")
		message.Body = j
	}

	err := emit.session.publishChannel.Publish(
		"remit",         // exchange
		emit.RoutingKey, // routing key / queue
		false,           // mandatory
		false,           // immediate
		message,         // amqp.Publishing
	)
	failOnError(err, "Failed to send emit message")
}
