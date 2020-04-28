package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/mind1949/mqbroker"
)

func main() {
	b := mqbroker.NewBroker()

	// 发布消息
	go func() {
		defer b.Close()
		for i := 0; i < 10; i++ {
			msg := []byte(fmt.Sprintf("msg[%d]", i+1))
			log.Printf("broker send %s", msg)
			b.Pub(msg)
			time.Sleep(1 * time.Second)
		}
	}()

	// 消费消息
	consume := func(consumer string) {
		queue, cancel := b.Consume(10)
		defer cancel()
		for msg := range queue {
			log.Printf("%s receives %s", consumer, msg)
		}
	}
	for i := 0; i < 5; i++ {
		name := "consumer" + strconv.Itoa(i+1)
		go consume(name)
	}

	<-b.Done()
}
