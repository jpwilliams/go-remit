package remit

import (
        "log"

        "github.com/streadway/amqp"
        "github.com/chuckpreslar/emission"
)

type Session struct {
        config          Config
        connection      *amqp.Connection
        workChannel     *amqp.Channel
        publishChannel  *amqp.Channel
        consumeChannel  *amqp.Channel
        emitter         *emission.Emitter
        EndpointGlobal  EndpointGlobal
}

func(session *Session) Endpoint(key string) (Endpoint) {
        log.Println("Adding endpoint for", key)

        return Endpoint{
                RoutingKey: key,
                Queue: key,
                session: session,
        }
}

func(session *Session) Emit(key string) (Emit) {
        log.Println("Adding emission for", key)

        return Emit{
                session: session,
        }
}
