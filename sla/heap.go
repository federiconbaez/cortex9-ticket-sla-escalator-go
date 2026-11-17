package sla

import "time"

type deadlineEvent struct {
	deadline time.Time
	ticketID string
	tier     string
	seq      uint64
}

type deadlineHeap []deadlineEvent

func (h deadlineHeap) Len() int { return len(h) }

func (h deadlineHeap) Less(i, j int) bool {
	if !h[i].deadline.Equal(h[j].deadline) {
		return h[i].deadline.Before(h[j].deadline)
	}
	if h[i].seq != h[j].seq {
		return h[i].seq < h[j].seq
	}
	if h[i].ticketID != h[j].ticketID {
		return h[i].ticketID < h[j].ticketID
	}
	return h[i].tier < h[j].tier
}

func (h deadlineHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *deadlineHeap) Push(x any) {
	*h = append(*h, x.(deadlineEvent))
}

func (h *deadlineHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func (h deadlineHeap) peek() (deadlineEvent, bool) {
	if len(h) == 0 {
		return deadlineEvent{}, false
	}
	return h[0], true
}
