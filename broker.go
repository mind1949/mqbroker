// Package mqbroker is a simple message broadcaster.
package mqbroker

import (
	"sync"

	"log"
)

// Broker is a message publishing middleware.
// Used to publish, subscribe, and unsubscribe messages.
type Broker struct {
	exchange chan Msg

	rwm    sync.RWMutex
	queues map[chan Msg]struct{}

	enqueues chan chan Msg
	dequeues chan chan Msg

	done chan struct{}

	debug bool
}

// Msg is []byte's alias.
type Msg = []byte

// NewBroker creates a new Broker.
func NewBroker() *Broker {
	broker := &Broker{
		exchange: make(chan Msg),
		queues:   make(map[chan Msg]struct{}),

		enqueues: make(chan chan Msg),
		dequeues: make(chan chan Msg),

		done: make(chan struct{}),
	}
	go broker.start()

	return broker
}

// start starts Broker.
func (b *Broker) start() {
	for {
		select {
		case msg := <-b.exchange:
			for queue := range b.queues {
				select {
				case queue <- msg:
				default:
					b.debugf("queue is blocked")
				}
			}
		case queue := <-b.enqueues:
			b.add(queue)
			b.debugf("consumer[%d] + 1", b.queuesNum()-1)
		case queue := <-b.dequeues:
			b.remove(queue)
			b.debugf("consumer[%d] - 1", b.queuesNum()+1)
		case <-b.done:
			for queue := range b.queues {
				b.remove(queue)
			}
			b.debugf("close broker")
			return
		}
	}
}

// Pub publishes msg.
func (b *Broker) Pub(msg Msg) {
	if b.queuesNum() == 0 {
		b.debugf("no consumber")
		return
	}

	select {
	case b.exchange <- msg:
	case <-b.done:
		b.debugf("the borker is closed and no message can be puhlished")
	}
}

// Consume comsumes messages.
// With prefetchCount greater than zero, the Broker will publish that
// many messages to queue before message is consumed.
// consume messages from queue.
// cancel cancels consumptions.
func (b *Broker) Consume(prefetchCount int) (queue <-chan Msg, cancel func()) {
	if prefetchCount < 0 {
		prefetchCount = 0
	}
	q := make(chan Msg, prefetchCount)
	b.enqueues <- q
	cancel = func() {
		select {
		case b.dequeues <- q:
		case <-b.done:
		}
	}

	return q, cancel
}

// Close closes Broker.
func (b *Broker) Close() {
	close(b.done)
}

// Done returns done channel.
func (b *Broker) Done() <-chan struct{} {
	return b.done
}

// Debug starts debug mode.
func (b *Broker) Debug() {
	b.debug = true
}

func (b *Broker) queuesNum() int {
	b.rwm.RLock()
	defer b.rwm.RUnlock()

	return len(b.queues)
}

func (b *Broker) add(queue chan Msg) {
	b.rwm.Lock()
	defer b.rwm.Unlock()

	b.queues[queue] = struct{}{}
}

func (b *Broker) remove(queue chan Msg) {
	b.rwm.Lock()
	defer b.rwm.Unlock()

	delete(b.queues, queue)
	close(queue)
}

func (b *Broker) debugf(format string, a ...interface{}) {
	if !b.debug {
		return
	}
	log.Printf(format, a...)
}
