package remit

import (
	"fmt"
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
	fmt.Println("creating work channel")
	channel, _ := p.connection.Channel()
	return channel
}

func (p *workerPool) Get() *amqp.Channel {
waiting:
	p.mx.Lock()
	defer p.mx.Unlock()

	if p.inuse < p.count {
		p.inuse++
	} else if p.count < p.max {
		channel := p.create()
		p.channels <- channel
		p.count++
		p.inuse++
	} else {
		p.mx.Unlock()
		goto waiting
	}

	if p.count < p.min {
		go p.new()
	}

	return <-p.channels
}

func (p *workerPool) Release(channel *amqp.Channel) {
	p.mx.Lock()
	defer p.mx.Unlock()

	if (p.count - p.inuse) > p.min {
		p.Drop(channel)
		p.count--
	} else {
		p.channels <- channel
	}

	p.inuse--
}

func (p *workerPool) Drop(channel *amqp.Channel) {
	p.mx.Lock()
	defer p.mx.Unlock()

	channel.Close()
	p.count--
	p.inuse--
}
