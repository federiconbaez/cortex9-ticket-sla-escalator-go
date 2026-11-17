package sla

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"
)

// Ticket is a support ticket subject to an SLA window.
type Ticket struct {
	ID        string
	Priority  string
	SLA       time.Duration
	CreatedAt time.Time
}

// SyncMode controls when the write-ahead log is fsynced.
type SyncMode int

const (
	// SyncEach fsyncs every appended record. This is the default.
	SyncEach SyncMode = iota
	// SyncNone flushes on close and compaction only. Used by the 100k benchmark.
	SyncNone
)

// Options configure Open.
type Options struct {
	Dir          string
	Clock        Clock
	Policy       Policy
	Sink         Sink
	CompactEvery int
	PollInterval time.Duration
	Sync         SyncMode
}

// Escalator schedules SLA deadlines, persists them, and fires idempotent escalations.
type Escalator struct {
	mu      sync.Mutex
	clock   Clock
	policy  Policy
	sink    Sink
	journal *journal
	state   *memState
	heap    deadlineHeap
	seq     uint64
	poll    time.Duration

	hookBeforeIntent func(id string) error
	hookAfterIntent  func(id string) error
	hookAfterSink    func(id string) error
}

// Open loads persisted state (snapshot + WAL) and rebuilds the deadline heap.
func Open(opts Options) (*Escalator, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("dir required")
	}
	if opts.Clock == nil {
		opts.Clock = SystemClock{}
	}
	if opts.Policy.Tiers == nil {
		opts.Policy = DefaultPolicy()
	}
	if opts.Sink == nil {
		opts.Sink = NewMemorySink()
	}
	poll := opts.PollInterval
	if poll == 0 {
		poll = 10 * time.Millisecond
	}
	syncEach := opts.Sync != SyncNone
	j, st, err := openJournal(opts.Dir, opts.CompactEvery, syncEach)
	if err != nil {
		return nil, err
	}
	e := &Escalator{
		clock:   opts.Clock,
		policy:  opts.Policy,
		sink:    opts.Sink,
		journal: j,
		state:   st,
		seq:     st.maxSeq(),
		poll:    poll,
	}
	e.rebuildHeap()
	return e, nil
}

// Close flushes and closes the journal.
func (e *Escalator) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.journal == nil {
		return nil
	}
	err := e.journal.close()
	e.journal = nil
	return err
}

// Compact writes a snapshot and truncates the WAL.
func (e *Escalator) Compact() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.journal.compact(e.state)
}

// Schedule persists a ticket and enqueues its tier deadlines.
func (e *Escalator) Schedule(t Ticket) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if t.ID == "" {
		return fmt.Errorf("ticket id required")
	}
	if t.SLA <= 0 {
		return fmt.Errorf("sla must be positive")
	}
	tiers, err := e.policy.TiersFor(t.Priority)
	if err != nil {
		return err
	}
	if _, exists := e.state.Tickets[t.ID]; exists {
		return fmt.Errorf("ticket %q already scheduled", t.ID)
	}
	created := t.CreatedAt
	if created.IsZero() {
		created = e.clock.Now()
	} else {
		created = created.UTC()
	}
	deadlines := make(map[string]time.Time, len(tiers))
	for _, tier := range tiers {
		deadlines[tier.Name] = created.Add(durationFraction(t.SLA, tier.Fraction))
	}
	e.seq++
	rec := walRecord{
		Type:      recSchedule,
		TicketID:  t.ID,
		Priority:  t.Priority,
		SLANanos:  int64(t.SLA),
		CreatedAt: created,
		Deadlines: deadlines,
		Seq:       e.seq,
		At:        e.clock.Now(),
	}
	if err := e.persist(rec); err != nil {
		e.seq--
		return err
	}
	for _, tier := range tiers {
		heap.Push(&e.heap, deadlineEvent{
			deadline: deadlines[tier.Name],
			ticketID: t.ID,
			tier:     tier.Name,
			seq:      e.seq,
		})
	}
	return nil
}

// Resolve marks a ticket done so later tiers do not fire. It is idempotent.
func (e *Escalator) Resolve(ticketID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	t, ok := e.state.Tickets[ticketID]
	if !ok {
		return fmt.Errorf("unknown ticket %q", ticketID)
	}
	if t.Resolved {
		return nil
	}
	return e.persist(walRecord{
		Type:     recResolve,
		TicketID: ticketID,
		At:       e.clock.Now(),
	})
}

// ProcessDue fires every escalation whose deadline is at or before now.
func (e *Escalator) ProcessDue() error {
	for {
		ev, ok := e.popDue()
		if !ok {
			return nil
		}
		if err := e.fire(ev); err != nil {
			e.mu.Lock()
			heap.Push(&e.heap, ev)
			e.mu.Unlock()
			return err
		}
	}
}

// Run processes due escalations until ctx is cancelled.
func (e *Escalator) Run(ctx context.Context) error {
	if err := e.ProcessDue(); err != nil {
		return err
	}
	ticker := time.NewTicker(e.poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := e.ProcessDue(); err != nil {
				return err
			}
		}
	}
}

// Stats is a point-in-time view of in-memory state.
type Stats struct {
	Tickets   int
	Resolved  int
	Intents   int
	Completed int
	Pending   int
}

// Stats returns counters for tickets and escalation records.
func (e *Escalator) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	s := Stats{
		Tickets:   len(e.state.Tickets),
		Intents:   len(e.state.Intents),
		Completed: len(e.state.Completed),
		Pending:   e.heap.Len(),
	}
	for _, t := range e.state.Tickets {
		if t.Resolved {
			s.Resolved++
		}
	}
	return s
}

func (e *Escalator) rebuildHeap() {
	e.heap = e.heap[:0]
	for _, t := range e.state.Tickets {
		if t.Resolved {
			continue
		}
		for tier, dl := range t.Deadlines {
			id := EscalationID(t.ID, tier)
			if _, done := e.state.Completed[id]; done {
				continue
			}
			heap.Push(&e.heap, deadlineEvent{
				deadline: dl,
				ticketID: t.ID,
				tier:     tier,
				seq:      t.Seq,
			})
		}
	}
}

func (e *Escalator) persist(r walRecord) error {
	if err := e.journal.append(r); err != nil {
		return err
	}
	if err := e.state.apply(r); err != nil {
		return err
	}
	return e.journal.maybeCompact(e.state)
}

func (e *Escalator) popDue() (deadlineEvent, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.clock.Now()
	for {
		ev, ok := e.heap.peek()
		if !ok || ev.deadline.After(now) {
			return deadlineEvent{}, false
		}
		ev = heap.Pop(&e.heap).(deadlineEvent)
		t, exists := e.state.Tickets[ev.ticketID]
		if !exists || t.Resolved {
			continue
		}
		id := EscalationID(ev.ticketID, ev.tier)
		if _, done := e.state.Completed[id]; done {
			continue
		}
		return ev, true
	}
}

func (e *Escalator) fire(ev deadlineEvent) error {
	id := EscalationID(ev.ticketID, ev.tier)
	esc, err := e.recordIntent(ev, id)
	if err != nil {
		return err
	}
	if esc == nil {
		return nil
	}
	if err := e.sink.Escalate(*esc); err != nil {
		return err
	}
	if e.hookAfterSink != nil {
		if err := e.hookAfterSink(id); err != nil {
			return err
		}
	}
	return e.recordComplete(id)
}

func (e *Escalator) recordIntent(ev deadlineEvent, id string) (*Escalation, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	t, exists := e.state.Tickets[ev.ticketID]
	if !exists || t.Resolved {
		return nil, nil
	}
	if _, done := e.state.Completed[id]; done {
		return nil, nil
	}
	_, hasIntent := e.state.Intents[id]
	if !hasIntent && e.hookBeforeIntent != nil {
		if err := e.hookBeforeIntent(id); err != nil {
			return nil, err
		}
	}
	if !hasIntent {
		if err := e.persist(walRecord{
			Type:         recIntent,
			EscalationID: id,
			TicketID:     ev.ticketID,
			Tier:         ev.tier,
			Deadline:     ev.deadline,
			At:           e.clock.Now(),
		}); err != nil {
			return nil, err
		}
	}
	if e.hookAfterIntent != nil {
		if err := e.hookAfterIntent(id); err != nil {
			return nil, err
		}
	}
	esc := Escalation{
		ID:       id,
		TicketID: ev.ticketID,
		Tier:     ev.tier,
		Priority: t.Priority,
		Deadline: ev.deadline,
	}
	return &esc, nil
}

func (e *Escalator) recordComplete(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, done := e.state.Completed[id]; done {
		return nil
	}
	return e.persist(walRecord{
		Type:         recComplete,
		EscalationID: id,
		At:           e.clock.Now(),
	})
}

func durationFraction(d time.Duration, frac float64) time.Duration {
	return time.Duration(float64(d) * frac)
}
