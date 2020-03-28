package mqbroker

import (
	"sync"

	"log"
)

// Broker 消息发布中间件
// 用于发布、订阅、取消订阅消息
type Broker struct {
	exchange chan Msg

	rwm    sync.RWMutex
	queues map[chan Msg]struct{}

	enqueues chan chan Msg
	dequeues chan chan Msg

	done chan struct{}
}

// Msg 单独给要发布的消息类型取个别名, 方便修改要发布的消息类型
// 例如要改为发布A类型消息, 只要修改为type Msg = A
type Msg = []byte

// NewBroker
func NewBroker() *Broker {
	broker := &Broker{
		exchange: make(chan Msg),
		queues:   make(map[chan Msg]struct{}),

		enqueues: make(chan chan Msg),
		dequeues: make(chan chan Msg),

		done: make(chan struct{}),
	}
	broker.start()

	return broker
}

// start 启动Brocker
func (b *Broker) start() {
	go func() {
		for {
			select {
			case msg := <-b.exchange:
				for queue := range b.queues {
					select {
					case queue <- msg:
						debugf("消息已发布至queue")
					default:
					}
				}
			case queue := <-b.enqueues:
				b.add(queue)
				log.Printf("已发起消费[consumer: %d]", b.queuesNum())
			case queue := <-b.dequeues:
				b.remove(queue)
				log.Printf("已取消消费[consumer: %d]", b.queuesNum())
			case <-b.done:
				log.Println("停用broker")
				return
			}
		}
	}()
}

// Pub 发布消息
func (b *Broker) Pub(msg Msg) {
	if b.queuesNum() == 0 {
		debugf("无消费者等待消息")
		return
	}

	select {
	case b.exchange <- msg:
		debugf("已发布消息至exchange")
	case <-b.done:
		log.Println("broker已停用,无法发送")
		return
	}
}

// Consume 消费消息
// prefetchCount 指定可以预先消费的消息数量
// 从queue中消费消息
// 使用cancel取消消费
func (b *Broker) Consume(prefetchCount int) (queue <-chan Msg, cancel func()) {
	var q chan Msg
	if prefetchCount <= 0 {
		q = make(chan Msg)
	} else {
		q = make(chan Msg, prefetchCount)
	}
	cancel = func() {
		select {
		case b.dequeues <- q:
		case <-b.done:
		}
	}
	b.enqueues <- q

	return q, cancel
}

// Close 发送关闭broker信号
func (b *Broker) Close() {
	close(b.done)
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

func (b *Broker) Done() <-chan struct{} {
	return b.done
}
