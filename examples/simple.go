package main

import (
	"fmt"
	"log"
	"strconv"
	"sync"
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
	var (
		wg    sync.WaitGroup
		count = 5
	)
	wg.Add(count)
	consume := func(consumer string) {
		defer wg.Done()
		queue, cancel := b.Consume(10)
		defer cancel()
		for {
			select {
			case msg := <-queue:
				log.Printf("%s receive %s", consumer, msg)
			case <-b.Done():
				return
			}
		}
	}
	for i := 0; i < count; i++ {
		name := "consumer" + strconv.Itoa(i+1)
		go consume(name)
	}
	wg.Wait()
}
