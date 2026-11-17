package sla

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func t0() time.Time {
	return time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
}

func openTest(t *testing.T, clock Clock, sink Sink, extra func(*Options)) *Escalator {
	t.Helper()
	opts := Options{
		Dir:          t.TempDir(),
		Clock:        clock,
		Sink:         sink,
		CompactEvery: -1,
	}
	if extra != nil {
		extra(&opts)
	}
	e, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func TestScheduleValidation(t *testing.T) {
	e := openTest(t, NewFakeClock(t0()), NewMemorySink(), nil)
	if err := e.Schedule(Ticket{Priority: "P1", SLA: time.Hour}); err == nil {
		t.Fatal("expected error for empty id")
	}
	if err := e.Schedule(Ticket{ID: "T1", Priority: "P1", SLA: 0}); err == nil {
		t.Fatal("expected error for zero sla")
	}
	if err := e.Schedule(Ticket{ID: "T1", Priority: "PX", SLA: time.Hour}); err == nil {
		t.Fatal("expected error for unknown priority")
	}
	if err := e.Schedule(Ticket{ID: "T1", Priority: "P1", SLA: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if err := e.Schedule(Ticket{ID: "T1", Priority: "P1", SLA: time.Hour}); err == nil {
		t.Fatal("expected duplicate error")
	}
	if err := e.Resolve("missing"); err == nil {
		t.Fatal("expected unknown ticket")
	}
}

func TestTiersFireAtConfiguredFractions(t *testing.T) {
	clock := NewFakeClock(t0())
	sink := NewMemorySink()
	e := openTest(t, clock, sink, nil)
	if err := e.Schedule(Ticket{ID: "T1", Priority: "P1", SLA: 10 * time.Second}); err != nil {
		t.Fatal(err)
	}

	clock.Advance(5*time.Second - time.Nanosecond)
	if err := e.ProcessDue(); err != nil {
		t.Fatal(err)
	}
	if sink.UniqueCount() != 0 {
		t.Fatalf("fired early: %v", sink.AppliedIDs())
	}

	clock.Advance(time.Nanosecond)
	if err := e.ProcessDue(); err != nil {
		t.Fatal(err)
	}
	if got := sink.AppliedIDs(); len(got) != 1 || got[0] != "T1:L1" {
		t.Fatalf("after 50%%: %v", got)
	}

	clock.Advance(3 * time.Second)
	if err := e.ProcessDue(); err != nil {
		t.Fatal(err)
	}
	if got := sink.AppliedIDs(); len(got) != 2 || got[1] != "T1:L2" && !contains(got, "T1:L2") {
		t.Fatalf("after 80%%: %v", got)
	}

	clock.Advance(2 * time.Second)
	if err := e.ProcessDue(); err != nil {
		t.Fatal(err)
	}
	got := sink.AppliedIDs()
	if len(got) != 3 || !contains(got, "T1:L3") {
		t.Fatalf("after breach: %v", got)
	}
	if err := e.ProcessDue(); err != nil {
		t.Fatal(err)
	}
	if sink.CallCount() != 3 {
		t.Fatalf("duplicate fires: calls=%d", sink.CallCount())
	}
}

func TestPriorityP3Ladder(t *testing.T) {
	clock := NewFakeClock(t0())
	sink := NewMemorySink()
	e := openTest(t, clock, sink, nil)
	if err := e.Schedule(Ticket{ID: "T3", Priority: "P3", SLA: 10 * time.Second}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(8 * time.Second)
	if err := e.ProcessDue(); err != nil {
		t.Fatal(err)
	}
	if got := sink.AppliedIDs(); len(got) != 1 || got[0] != "T3:L1" {
		t.Fatalf("P3 L1: %v", got)
	}
	clock.Advance(2 * time.Second)
	if err := e.ProcessDue(); err != nil {
		t.Fatal(err)
	}
	if got := sink.AppliedIDs(); len(got) != 2 || !contains(got, "T3:L3") {
		t.Fatalf("P3 L3: %v", got)
	}
}

func TestResolvedBeforeEscalation(t *testing.T) {
	clock := NewFakeClock(t0())
	sink := NewMemorySink()
	e := openTest(t, clock, sink, nil)
	if err := e.Schedule(Ticket{ID: "T1", Priority: "P1", SLA: 10 * time.Second}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(10 * time.Second)
	if err := e.Resolve("T1"); err != nil {
		t.Fatal(err)
	}
	if err := e.Resolve("T1"); err != nil {
		t.Fatal(err)
	}
	if err := e.ProcessDue(); err != nil {
		t.Fatal(err)
	}
	if sink.UniqueCount() != 0 {
		t.Fatalf("escalated a resolved ticket: %v", sink.AppliedIDs())
	}
}

func TestResolvedBetweenTiers(t *testing.T) {
	clock := NewFakeClock(t0())
	sink := NewMemorySink()
	e := openTest(t, clock, sink, nil)
	if err := e.Schedule(Ticket{ID: "T1", Priority: "P1", SLA: 10 * time.Second}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(5 * time.Second)
	if err := e.ProcessDue(); err != nil {
		t.Fatal(err)
	}
	if err := e.Resolve("T1"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(5 * time.Second)
	if err := e.ProcessDue(); err != nil {
		t.Fatal(err)
	}
	if got := sink.AppliedIDs(); len(got) != 1 || got[0] != "T1:L1" {
		t.Fatalf("expected only L1, got %v", got)
	}
}

func TestDeadlineOrdering(t *testing.T) {
	clock := NewFakeClock(t0())
	sink := NewMemorySink()
	e := openTest(t, clock, sink, nil)
	if err := e.Schedule(Ticket{ID: "late", Priority: "P1", SLA: 10 * time.Second}); err != nil {
		t.Fatal(err)
	}
	if err := e.Schedule(Ticket{ID: "early", Priority: "P1", SLA: 6 * time.Second}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(5 * time.Second) // late L1 at 5s, early L1 at 3s (already due) and not L2 yet (4.8s... 80% of 6s = 4.8s, also due).
	// early L1 at 3s, early L2 at 4.8s, late L1 at 5s.
	if err := e.ProcessDue(); err != nil {
		t.Fatal(err)
	}
	want := []string{"early:L1", "early:L2", "late:L1"}
	if len(sink.Order) != 3 {
		t.Fatalf("order %v", sink.Order)
	}
	for i, id := range want {
		if sink.Order[i] != id {
			t.Fatalf("order %v want %v", sink.Order, want)
		}
	}
}

func TestSameDeadlineScheduleOrder(t *testing.T) {
	clock := NewFakeClock(t0())
	sink := NewMemorySink()
	e := openTest(t, clock, sink, nil)
	for _, id := range []string{"TA", "TB", "TC"} {
		if err := e.Schedule(Ticket{ID: id, Priority: "P1", SLA: 10 * time.Second, CreatedAt: t0()}); err != nil {
			t.Fatal(err)
		}
	}
	clock.Advance(5 * time.Second)
	if err := e.ProcessDue(); err != nil {
		t.Fatal(err)
	}
	want := []string{"TA:L1", "TB:L1", "TC:L1"}
	if len(sink.Order) != 3 {
		t.Fatalf("got %v", sink.Order)
	}
	for i, id := range want {
		if sink.Order[i] != id {
			t.Fatalf("order %v want %v", sink.Order, want)
		}
	}
}

func TestCompactAndReopen(t *testing.T) {
	clock := NewFakeClock(t0())
	sink := NewMemorySink()
	dir := t.TempDir()
	e, err := Open(Options{Dir: dir, Clock: clock, Sink: sink, CompactEvery: 4})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		id := fmt.Sprintf("T%d", i)
		if err := e.Schedule(Ticket{ID: id, Priority: "P2", SLA: 10 * time.Second, CreatedAt: t0()}); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Resolve("T0"); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	clock.Advance(10 * time.Second)
	e2, err := Open(Options{Dir: dir, Clock: clock, Sink: sink, CompactEvery: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if err := e2.ProcessDue(); err != nil {
		t.Fatal(err)
	}
	// 5 open tickets * 3 tiers, T0 resolved
	if sink.UniqueCount() != 15 {
		t.Fatalf("applied %d want 15 (%v)", sink.UniqueCount(), sink.AppliedIDs())
	}
	if contains(sink.AppliedIDs(), "T0:L1") {
		t.Fatal("resolved ticket fired after reopen")
	}
	st := e2.Stats()
	if st.Tickets != 6 || st.Resolved != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestConcurrentScheduleResolveProcess(t *testing.T) {
	clock := NewFakeClock(t0())
	sink := NewMemorySink()
	e := openTest(t, clock, sink, func(o *Options) { o.Sync = SyncNone })

	const n = 50
	var wg sync.WaitGroup
	errCh := make(chan error, n*2)
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			id := fmt.Sprintf("T%02d", i)
			if err := e.Schedule(Ticket{ID: id, Priority: "P2", SLA: 10 * time.Second, CreatedAt: t0()}); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()

	clock.Advance(5 * time.Second)
	wg.Add(n/2 + 1)
	go func() {
		defer wg.Done()
		if err := e.ProcessDue(); err != nil {
			errCh <- err
		}
	}()
	for i := 0; i < n; i += 2 {
		i := i
		go func() {
			defer wg.Done()
			if err := e.Resolve(fmt.Sprintf("T%02d", i)); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	clock.Advance(5 * time.Second)
	if err := e.ProcessDue(); err != nil {
		t.Fatal(err)
	}

	// Odd tickets were not resolved: 25 tickets * 3 tiers = 75.
	// Even tickets may have fired L1 before resolve; L2/L3 must not fire after resolve.
	for i := 1; i < n; i += 2 {
		for _, tier := range []string{"L1", "L2", "L3"} {
			id := EscalationID(fmt.Sprintf("T%02d", i), tier)
			if _, ok := sink.Applied[id]; !ok {
				t.Fatalf("missing %s", id)
			}
		}
	}
	for i := 0; i < n; i += 2 {
		for _, tier := range []string{"L2", "L3"} {
			id := EscalationID(fmt.Sprintf("T%02d", i), tier)
			if _, ok := sink.Applied[id]; ok {
				t.Fatalf("resolved ticket fired %s", id)
			}
		}
	}
}

func TestFakeClockIndependentOfWall(t *testing.T) {
	clock := NewFakeClock(t0())
	if !clock.Now().Equal(t0()) {
		t.Fatalf("now %v", clock.Now())
	}
	clock.Advance(time.Hour)
	if clock.Now().Sub(t0()) != time.Hour {
		t.Fatalf("advance %v", clock.Now())
	}
}

func TestEscalationID(t *testing.T) {
	if got := EscalationID("T1", "L2"); got != "T1:L2" {
		t.Fatalf("got %q", got)
	}
}

func contains(ids []string, want string) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

var errInjected = errors.New("injected crash")
