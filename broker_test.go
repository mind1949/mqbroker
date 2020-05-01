package mqbroker

import (
	"testing"
	"time"
)

func TestBorkerPub(t *testing.T) {
	b := NewBroker()
	b.Debug()
	msgs := [][]byte{
		{'a'},
		{'b'},
		{'c'},
		{'d'},
		{'e'},
	}
	queue, cancel := b.Consume(len(msgs))
	defer cancel()

	go func() {
		defer b.Close()
		for _, msg := range msgs {
			b.Pub(msg)
		}
	}()

	for i := 0; i < len(msgs); i++ {
		expect := msgs[i]
		select {
		case got := <-queue:
			if string(expect) != string(got) {
				t.Errorf("expect: %s got: %s\n", expect, got)
			}
		case <-time.After(1 * time.Second):
			t.Error("timeout")
		}
	}
}
