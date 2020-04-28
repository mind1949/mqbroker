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

// NewBroker ceates a new Broker.
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
				}
			}
		case queue := <-b.enqueues:
			b.add(queue)
			b.debugf("已发起消费[consumer: %d]", b.queuesNum())
		case queue := <-b.dequeues:
			b.remove(queue)
			b.debugf("已取消消费[consumer: %d]", b.queuesNum())
		case <-b.done:
			for queue := range b.queues {
				b.remove(queue)
			}
			b.debugf("停用broker")
			return
		}
	}
}

// Pub publishes msg.
func (b *Broker) Pub(msg Msg) {
	if b.queuesNum() == 0 {
		b.debugf("无消费者")
		return
	}

	select {
	case b.exchange <- msg:
	case <-b.done:
		b.debugf("broker已被关闭, 无法发布消息")
	}
}

// Consume comsumes messages.
// With prefetchCount greater than zero, the Broker will publish that
// many messages to queue before message is consumered.
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
