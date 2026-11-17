package sla

import "fmt"

// Tier is one escalation level, fired at a fraction of the ticket SLA window.
type Tier struct {
	Name     string
	Fraction float64
}

// Policy maps ticket priorities to their escalation tiers.
type Policy struct {
	Tiers map[string][]Tier
}

// DefaultPolicy provides L1 at 50%, L2 at 80% and L3 at breach for P1/P2,
// and a shorter ladder for P3.
func DefaultPolicy() Policy {
	standard := []Tier{
		{Name: "L1", Fraction: 0.50},
		{Name: "L2", Fraction: 0.80},
		{Name: "L3", Fraction: 1.00},
	}
	return Policy{Tiers: map[string][]Tier{
		"P1": append([]Tier(nil), standard...),
		"P2": append([]Tier(nil), standard...),
		"P3": {
			{Name: "L1", Fraction: 0.80},
			{Name: "L3", Fraction: 1.00},
		},
	}}
}

// TiersFor returns the configured ladder for priority.
func (p Policy) TiersFor(priority string) ([]Tier, error) {
	t, ok := p.Tiers[priority]
	if !ok || len(t) == 0 {
		return nil, fmt.Errorf("unknown ticket priority %q", priority)
	}
	return t, nil
}

// EscalationID is the idempotency key for a ticket and tier.
func EscalationID(ticketID, tier string) string {
	return ticketID + ":" + tier
}
