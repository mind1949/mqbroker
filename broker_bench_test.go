package mqbroker

import "testing"

func BenchmarkConsumer1(b *testing.B) {
	benchmarkConsumer(1, b)
}

func BenchmarkConsumer100(b *testing.B) {
	benchmarkConsumer(100, b)
}

func BenchmarkConsumer10000(b *testing.B) {
	benchmarkConsumer(10000, b)
}

func BenchmarkConsumer100000(b *testing.B) {
	benchmarkConsumer(100000, b)
}

func benchmarkConsumer(num int, b *testing.B) {
	bk := NewBroker()
	defer bk.Close()
	for i := 0; i < num; i++ {
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
