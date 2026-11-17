package sla

import (
	"sync"
	"time"
)

// Clock abstracts time so tests can inject a fake clock.
type Clock interface {
	Now() time.Time
}

// SystemClock uses the process wall clock.
type SystemClock struct{}

// Now returns the current local time.
func (SystemClock) Now() time.Time { return time.Now() }

// FakeClock is a monotonic, manually advanced clock.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock returns a fake clock fixed at now.
func NewFakeClock(now time.Time) *FakeClock {
	return &FakeClock{now: now.UTC()}
}

// Now returns the current fake instant.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Set jumps the clock to t.
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t.UTC()
}
