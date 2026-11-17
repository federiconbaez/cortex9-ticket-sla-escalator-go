package sla

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkScheduleAndFire100k(b *testing.B) {
	const n = 100_000
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		clock := NewFakeClock(t0())
		sink := NewMemorySink()
		e, err := Open(Options{
			Dir:          dir,
			Clock:        clock,
			Sink:         sink,
			CompactEvery: 50_000,
			Sync:         SyncNone,
		})
		if err != nil {
			b.Fatal(err)
		}
		tickets := make([]Ticket, n)
		for j := 0; j < n; j++ {
			tickets[j] = Ticket{
				ID:        fmt.Sprintf("T-%06d", j),
				Priority:  "P2",
				SLA:       time.Hour,
				CreatedAt: t0(),
			}
		}
		b.StartTimer()
		for j := 0; j < n; j++ {
			if err := e.Schedule(tickets[j]); err != nil {
				b.Fatal(err)
			}
		}
		clock.Advance(time.Hour)
		if err := e.ProcessDue(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if sink.UniqueCount() != n*3 {
			b.Fatalf("applied %d want %d", sink.UniqueCount(), n*3)
		}
		if err := e.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
