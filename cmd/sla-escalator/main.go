package main

import (
	"fmt"
	"os"
	"time"

	"github.com/federiconbaez/cortex9-ticket-sla-escalator-go/sla"
)

func main() {
	dir, err := os.MkdirTemp("", "cx9-sla-demo-")
	if err != nil {
		fatal(err)
	}
	defer os.RemoveAll(dir)

	clock := sla.NewFakeClock(time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC))
	sink := sla.NewMemorySink()
	esc, err := sla.Open(sla.Options{Dir: dir, Clock: clock, Sink: sink})
	if err != nil {
		fatal(err)
	}
	defer esc.Close()

	tickets := []sla.Ticket{
		{ID: "INC-100", Priority: "P1", SLA: 10 * time.Minute},
		{ID: "INC-200", Priority: "P2", SLA: 10 * time.Minute},
		{ID: "INC-300", Priority: "P3", SLA: 10 * time.Minute},
	}
	for _, t := range tickets {
		if err := esc.Schedule(t); err != nil {
			fatal(err)
		}
		fmt.Printf("programado %s (%s, sla=%s)\n", t.ID, t.Priority, t.SLA)
	}

	fmt.Println("resolviendo INC-200 antes de cualquier escalamiento")
	if err := esc.Resolve("INC-200"); err != nil {
		fatal(err)
	}

	steps := []time.Duration{
		5 * time.Minute,
		3 * time.Minute,
		2 * time.Minute,
	}
	elapsed := time.Duration(0)
	for _, d := range steps {
		clock.Advance(d)
		elapsed += d
		before := len(sink.Order)
		if err := esc.ProcessDue(); err != nil {
			fatal(err)
		}
		fmt.Printf("t+%s\n", elapsed)
		for _, id := range sink.Order[before:] {
			ev := sink.Applied[id]
			fmt.Printf("  escalamiento %s (prioridad %s, vencía %s)\n", ev.ID, ev.Priority, ev.Deadline.Format(time.RFC3339))
		}
	}

	st := esc.Stats()
	fmt.Printf("estado: tickets=%d resueltos=%d completados=%d\n", st.Tickets, st.Resolved, st.Completed)
	fmt.Printf("efectos únicos=%d llamadas al sink=%d\n", sink.UniqueCount(), sink.CallCount())
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}
