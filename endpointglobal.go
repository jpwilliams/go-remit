package remit

import (
        "log"

        "github.com/chuckpreslar/emission"
)

type EndpointGlobal struct {
        emitter *emission.Emitter
}

func(endpoint *EndpointGlobal) Data(handler func(Event)) {
        log.Println("Adding global Data listener")
}
