package remit

import (
        "log"
)

type Emit struct {
        session         *Session
}

func(request *Emit) Sent(handler func(Event)) {
        log.Println("Adding Sent listener")
}
