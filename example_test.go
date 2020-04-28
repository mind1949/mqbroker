package mqbroker

import (
	"fmt"
	"strconv"
	"time"
)

func Example_simple() {
	// This example helps to broadcast message to multiple consumers

	b := NewBroker()

	// broadcast message
	go func() {
		defer b.Close()
		for i := 0; i < 3; i++ {
			msg := []byte(fmt.Sprintf("msg[%d]", i+1))
			fmt.Printf("broker send %s\n", msg)
			b.Pub(msg)
		}
		time.Sleep(1 * time.Second)
	}()

	consume := func(consumer string) {
		queue, cancel := b.Consume(10)
		defer cancel()
		for msg := range queue {
			fmt.Printf("%s receives %s\n", consumer, msg)
		}
	}
	// multiple consumers consume message
	for i := 0; i < 5; i++ {
		name := "consumer" + strconv.Itoa(i+1)
		go consume(name)
	}

	<-b.Done()
	// output like this:
	// broker send msg[1]
	// broker send msg[2]
	// broker send msg[3]
	// consumer1 receives msg[2]
	// consumer1 receives msg[3]
	// consumer5 receives msg[1]
	// consumer2 receives msg[1]
	// consumer2 receives msg[2]
	// consumer2 receives msg[3]
	// consumer4 receives msg[2]
	// consumer4 receives msg[3]
	// consumer3 receives msg[1]
	// consumer3 receives msg[2]
	// consumer3 receives msg[3]
	// consumer5 receives msg[2]
	// consumer5 receives msg[3]
}
