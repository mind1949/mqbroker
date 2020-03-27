package main

import (
	"fmt"
	"time"

	"github.com/mind1949/mqbroker"
)

func main() {
	b := mqbroker.NewBroker()

	// 发布消息
	go func() {
		defer b.Close()
		for i := 0; i < 10; i++ {
			msg := []byte(fmt.Sprintf("hello mqbroker: %d", i))
			b.Pub(msg)
			time.Sleep(1 * time.Second)
		}
	}()

	// 消费消息
	queue, cancel := b.Consume(10)
	defer cancel()

	for {
		select {
		case msg := <-queue:
			fmt.Println(string(msg))
		case <-b.Done():
			return
		}
	}
}
