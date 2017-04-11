package remit

import (
	"sync"

	"github.com/streadway/amqp"
)

type workerPool struct {
	mx         *sync.Mutex
	min        int
	max        int
	channels   chan *amqp.Channel
	count      int
	inuse      int
	connection *amqp.Connection
}

func newWorkerPool(min int, max int, connection *amqp.Connection) *workerPool {
	p := &workerPool{
		min:        min,
		max:        max,
		channels:   make(chan *amqp.Channel, max),
		connection: connection,
		mx:         &sync.Mutex{},
	}

	for i := 0; i < p.min; i++ {
		p.new()
	}

	return p
}

func (p *workerPool) new() {
	p.mx.Lock()
	defer p.mx.Unlock()

	p.count++
	channel := p.create()
	p.channels <- channel
}

func (p *workerPool) create() *amqp.Channel {
	channel, err := p.connection.Channel()
	if err != nil {
		panic(err)
	}
	return channel
}

func (p *workerPool) get() *amqp.Channel {
	// only defer an unlock on the first iteration here
	looped := false

waiting:
	p.mx.Lock()

	if !looped {
		defer p.mx.Unlock()
	}

	if p.inuse < p.count {
		p.inuse++
	} else if p.count < p.max {
		channel := p.create()
		p.channels <- channel
		p.count++
		p.inuse++
	} else {
		looped = true
		p.mx.Unlock()
		goto waiting
	}

	if p.count < p.min {
		go p.new()
	}

	return <-p.channels
}

func (p *workerPool) release(channel *amqp.Channel) {
	p.mx.Lock()
	defer p.mx.Unlock()

	p.inuse--

	if (p.count - p.inuse) > p.min {
		channel.Close()
		p.count--
	} else {
		p.channels <- channel
	}
}

func (p *workerPool) drop(channel *amqp.Channel) {
	p.mx.Lock()
	defer p.mx.Unlock()

	p.count--
	p.inuse--
}
