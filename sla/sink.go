package sla

import (
	"sort"
	"sync"
	"time"
)

// Escalation is the side-effect payload delivered to a Sink.
type Escalation struct {
	ID       string
	TicketID string
	Tier     string
	Priority string
	Deadline time.Time
}

// Sink receives due escalations. Implementations must be idempotent on Escalation.ID.
type Sink interface {
	Escalate(e Escalation) error
}

// MemorySink is an idempotent in-memory sink, suitable for tests and the demo.
type MemorySink struct {
	mu      sync.Mutex
	Applied map[string]Escalation
	Order   []string
	Calls   int
}

// NewMemorySink returns an empty idempotent sink.
func NewMemorySink() *MemorySink {
	return &MemorySink{Applied: make(map[string]Escalation)}
}

// Escalate records the event once per ID and counts every call.
func (s *MemorySink) Escalate(e Escalation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Calls++
	if _, ok := s.Applied[e.ID]; ok {
		return nil
	}
	s.Applied[e.ID] = e
	s.Order = append(s.Order, e.ID)
	return nil
}

// AppliedIDs returns sorted unique IDs that produced an effect.
func (s *MemorySink) AppliedIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.Applied))
	for id := range s.Applied {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// CallCount returns how many times Escalate was invoked, including retries.
func (s *MemorySink) CallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Calls
}

// UniqueCount returns the number of distinct applied IDs.
func (s *MemorySink) UniqueCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.Applied)
}
