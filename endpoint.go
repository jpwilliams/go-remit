package remit

import (
        "log"
        "encoding/json"

        "github.com/google/uuid"
)

type Endpoint struct {
        RoutingKey      string
        Queue           string
        session         *Session
}

func(endpoint *Endpoint) Data(handler func(Event)) {
        log.Println("Adding Data listener")

        parsedData := EventData{}
        err := json.Unmarshal([]byte(`{"foo": "bar"}`), &parsedData)
        failOnError(err, "Failed to parse JSON")

        handler(Event{
                EventId: uuid.New().String(),
                EventType: "a.message",
                Resource: "that.publisher",
                Data: parsedData,
        })
}

func(endpoint *Endpoint) Ready(handler func()) {
        log.Println("Adding Ready listener")

        handler()
}
