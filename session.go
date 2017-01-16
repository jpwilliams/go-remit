package remit

import (
	// "log"

	"github.com/chuckpreslar/emission"
	"github.com/streadway/amqp"
)

type Session struct {
	Config         Config
	connection     *amqp.Connection
	workChannel    *amqp.Channel
	publishChannel *amqp.Channel
	consumeChannel *amqp.Channel
	emitter        *emission.Emitter
	EndpointGlobal EndpointGlobal
	testKey        string
}

func (session *Session) Close() {
	err := session.connection.Close()
	failOnError(err, "Failed to close connection to RabbitMQ safely")
}

func (session *Session) Endpoint(key string, handler EndpointDataHandler) Endpoint {
	endpoint := createEndpoint(session, EndpointOptions{
		RoutingKey:  key,
		Queue:       key,
		DataHandler: handler,
	})

	return endpoint
}

func (session *Session) EndpointWithOptions(options EndpointOptions) Endpoint {
	if options.Queue == "" && options.RoutingKey == "" {
		panic("No queue or routing key given")
	}

	if options.RoutingKey == "" {
		if options.Queue != "" {
			options.RoutingKey = options.Queue
		} else {
			panic("No routing key (explicit or implicit) given")
		}
	}

	return Endpoint{
		RoutingKey: options.RoutingKey,
		Queue:      options.Queue,
		session:    session,
		emitter:    emission.NewEmitter(),
	}
}

func (session *Session) Emit(key string) Emit {
	if key == "" {
		panic("No valid routing key given for emission")
	}

	emit := createEmit(session, EmitOptions{
		RoutingKey: key,
	})

	return emit
}
