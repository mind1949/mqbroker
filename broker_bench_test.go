package mqbroker

import "testing"

func BenchmarkConsumer1(b *testing.B) {
	benchmarkConsumer(b, 1)
}

func BenchmarkConsumer100(b *testing.B) {
	benchmarkConsumer(b, 100)
}

func BenchmarkConsumer10000(b *testing.B) {
	benchmarkConsumer(b, 10000)
}

func BenchmarkConsumer100000(b *testing.B) {
	benchmarkConsumer(b, 100000)
}

func benchmarkConsumer(b *testing.B, size int) {
	bk := NewBroker()
	defer bk.Close()
	for i := 0; i < size; i++ {
		go func() {
			queue, cancel := bk.Consume(200)
			defer cancel()
			for range queue {
			}
		}()
	}
	for i := 0; i < b.N; i++ {
		bk.Pub([]byte{'a'})
	}
}
