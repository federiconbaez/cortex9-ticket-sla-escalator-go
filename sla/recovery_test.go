package sla

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRecoveryExactlyOnce(t *testing.T) {
	cases := []struct {
		name         string
		installHooks func(*Escalator)
		sinkCallsMin int
		sinkCallsMax int
	}{
		{
			name: "before_intent",
			installHooks: func(e *Escalator) {
				e.hookBeforeIntent = func(string) error { return errInjected }
			},
			sinkCallsMin: 1,
			sinkCallsMax: 1,
		},
		{
			name: "after_intent_before_sink",
			installHooks: func(e *Escalator) {
				e.hookAfterIntent = func(string) error { return errInjected }
			},
			sinkCallsMin: 1,
			sinkCallsMax: 1,
		},
		{
			name: "after_sink_before_complete",
			installHooks: func(e *Escalator) {
				e.hookAfterSink = func(string) error { return errInjected }
			},
			// Recovery retries the sink; the effect stays unique.
			sinkCallsMin: 2,
			sinkCallsMax: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			clock := NewFakeClock(t0())
			sink := NewMemorySink()

			e, err := Open(Options{Dir: dir, Clock: clock, Sink: sink, CompactEvery: -1})
			if err != nil {
				t.Fatal(err)
			}
			if err := e.Schedule(Ticket{ID: "T1", Priority: "P1", SLA: 10 * time.Second, CreatedAt: t0()}); err != nil {
				t.Fatal(err)
			}
			tc.installHooks(e)
			clock.Advance(5 * time.Second)
			if err := e.ProcessDue(); err != errInjected {
				t.Fatalf("got %v want injected crash", err)
			}
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}

			switch tc.name {
			case "before_intent":
				if sink.UniqueCount() != 0 {
					t.Fatalf("sink fired before intent: %v", sink.AppliedIDs())
				}
			case "after_intent_before_sink":
				if sink.UniqueCount() != 0 {
					t.Fatalf("sink fired before it should: %v", sink.AppliedIDs())
				}
			case "after_sink_before_complete":
				if sink.UniqueCount() != 1 {
					t.Fatalf("sink should have applied once before crash, got %v", sink.AppliedIDs())
				}
			}

			e2, err := Open(Options{Dir: dir, Clock: clock, Sink: sink, CompactEvery: -1})
			if err != nil {
				t.Fatal(err)
			}
			if err := e2.ProcessDue(); err != nil {
				t.Fatal(err)
			}
			if sink.UniqueCount() != 1 || !contains(sink.AppliedIDs(), "T1:L1") {
				t.Fatalf("after recovery L1: %v", sink.AppliedIDs())
			}
			if c := sink.CallCount(); c < tc.sinkCallsMin || c > tc.sinkCallsMax {
				t.Fatalf("calls=%d want %d..%d", c, tc.sinkCallsMin, tc.sinkCallsMax)
			}

			clock.Advance(5 * time.Second)
			if err := e2.ProcessDue(); err != nil {
				t.Fatal(err)
			}
			got := sink.AppliedIDs()
			if len(got) != 3 {
				t.Fatalf("expected three tiers after full window, got %v", got)
			}
			for _, id := range []string{"T1:L1", "T1:L2", "T1:L3"} {
				if !contains(got, id) {
					t.Fatalf("missing %s in %v", id, got)
				}
			}
			if err := e2.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRecoveryTruncatedWALLine(t *testing.T) {
	dir := t.TempDir()
	clock := NewFakeClock(t0())
	sink := NewMemorySink()
	e, err := Open(Options{Dir: dir, Clock: clock, Sink: sink, CompactEvery: -1})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Schedule(Ticket{ID: "T1", Priority: "P1", SLA: 10 * time.Second, CreatedAt: t0()}); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	wal := filepath.Join(dir, walFileName)
	// Append a truncated JSON line as a crash mid-write would leave.
	if err := appendFile(wal, []byte(`{"type":"schedule","ticket_id":"TX"`)); err != nil {
		t.Fatal(err)
	}

	e2, err := Open(Options{Dir: dir, Clock: clock, Sink: sink, CompactEvery: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	clock.Advance(5 * time.Second)
	if err := e2.ProcessDue(); err != nil {
		t.Fatal(err)
	}
	if !contains(sink.AppliedIDs(), "T1:L1") {
		t.Fatalf("did not recover T1: %v", sink.AppliedIDs())
	}
}

func appendFile(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err2 := f.Close(); err == nil {
		err = err2
	}
	return err
}
