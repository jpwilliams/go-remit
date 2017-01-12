package remit

import (
	"log"

	"github.com/chuckpreslar/emission"
)

type EndpointGlobal struct {
	emitter *emission.Emitter
}

func (endpoint EndpointGlobal) Data(handler func(Event)) EndpointGlobal {
	log.Println("Adding global Data listener")

	return endpoint
}
