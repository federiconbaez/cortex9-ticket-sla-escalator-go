package sla

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	recSchedule = "schedule"
	recResolve  = "resolve"
	recIntent   = "intent"
	recComplete = "complete"

	defaultCompactEvery = 1024
	walFileName         = "wal.jsonl"
	snapFileName        = "snapshot.json"
)

type walRecord struct {
	Type         string               `json:"type"`
	TicketID     string               `json:"ticket_id,omitempty"`
	Priority     string               `json:"priority,omitempty"`
	SLANanos     int64                `json:"sla_nanos,omitempty"`
	CreatedAt    time.Time            `json:"created_at,omitempty"`
	Deadlines    map[string]time.Time `json:"deadlines,omitempty"`
	Seq          uint64               `json:"seq,omitempty"`
	EscalationID string               `json:"escalation_id,omitempty"`
	Tier         string               `json:"tier,omitempty"`
	Deadline     time.Time            `json:"deadline,omitempty"`
	At           time.Time            `json:"at,omitempty"`
}

type ticketState struct {
	ID        string               `json:"id"`
	Priority  string               `json:"priority"`
	SLANanos  int64                `json:"sla_nanos"`
	CreatedAt time.Time            `json:"created_at"`
	Deadlines map[string]time.Time `json:"deadlines"`
	Seq       uint64               `json:"seq"`
	Resolved  bool                 `json:"resolved"`
}

type memState struct {
	Tickets   map[string]*ticketState `json:"tickets"`
	Intents   map[string]walRecord    `json:"intents"`
	Completed map[string]struct{}     `json:"completed"`
}

func newMemState() *memState {
	return &memState{
		Tickets:   make(map[string]*ticketState),
		Intents:   make(map[string]walRecord),
		Completed: make(map[string]struct{}),
	}
}

func (s *memState) apply(r walRecord) error {
	switch r.Type {
	case recSchedule:
		if _, ok := s.Tickets[r.TicketID]; ok {
			return nil
		}
		deadlines := r.Deadlines
		if deadlines == nil {
			deadlines = map[string]time.Time{}
		}
		s.Tickets[r.TicketID] = &ticketState{
			ID:        r.TicketID,
			Priority:  r.Priority,
			SLANanos:  r.SLANanos,
			CreatedAt: r.CreatedAt,
			Deadlines: deadlines,
			Seq:       r.Seq,
		}
	case recResolve:
		t, ok := s.Tickets[r.TicketID]
		if !ok {
			return nil
		}
		t.Resolved = true
	case recIntent:
		if r.EscalationID == "" {
			return fmt.Errorf("intent without escalation id")
		}
		if _, ok := s.Intents[r.EscalationID]; ok {
			return nil
		}
		s.Intents[r.EscalationID] = r
	case recComplete:
		if r.EscalationID == "" {
			return fmt.Errorf("complete without escalation id")
		}
		s.Completed[r.EscalationID] = struct{}{}
	default:
		return fmt.Errorf("unknown wal record type %q", r.Type)
	}
	return nil
}

func (s *memState) maxSeq() uint64 {
	var max uint64
	for _, t := range s.Tickets {
		if t.Seq > max {
			max = t.Seq
		}
	}
	return max
}

type journal struct {
	dir          string
	walPath      string
	snapPath     string
	f            *os.File
	buf          *bufio.Writer
	records      int
	compactEvery int
	syncEach     bool
}

func openJournal(dir string, compactEvery int, syncEach bool) (*journal, *memState, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, err
	}
	if compactEvery == 0 {
		compactEvery = defaultCompactEvery
	}
	if compactEvery < 0 {
		compactEvery = 0
	}
	j := &journal{
		dir:          dir,
		walPath:      filepath.Join(dir, walFileName),
		snapPath:     filepath.Join(dir, snapFileName),
		compactEvery: compactEvery,
		syncEach:     syncEach,
	}
	st := newMemState()
	if err := j.loadSnapshot(st); err != nil {
		return nil, nil, err
	}
	n, err := j.replayWAL(st)
	if err != nil {
		return nil, nil, err
	}
	j.records = n
	f, err := os.OpenFile(j.walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, err
	}
	j.f = f
	j.buf = bufio.NewWriterSize(f, 64*1024)
	return j, st, nil
}

func (j *journal) loadSnapshot(st *memState) error {
	b, err := os.ReadFile(j.snapPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if len(bytes.TrimSpace(b)) == 0 {
		return nil
	}
	var snap memState
	if err := json.Unmarshal(b, &snap); err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	if snap.Tickets != nil {
		st.Tickets = snap.Tickets
	}
	if snap.Intents != nil {
		st.Intents = snap.Intents
	}
	if snap.Completed != nil {
		st.Completed = snap.Completed
	}
	for _, t := range st.Tickets {
		if t.Deadlines == nil {
			t.Deadlines = map[string]time.Time{}
		}
	}
	return nil
}

func (j *journal) replayWAL(st *memState) (int, error) {
	f, err := os.Open(j.walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	var (
		prev    []byte
		hasPrev bool
		n       int
	)
	for sc.Scan() {
		if hasPrev {
			if err := applyLine(st, prev); err != nil {
				return n, err
			}
			n++
		}
		prev = append(prev[:0], sc.Bytes()...)
		hasPrev = true
	}
	if err := sc.Err(); err != nil {
		return n, err
	}
	if hasPrev {
		if err := applyLine(st, prev); err != nil {
			// A truncated last line after a crash is ignored.
			if !json.Valid(bytes.TrimSpace(prev)) {
				return n, nil
			}
			return n, err
		}
		n++
	}
	return n, nil
}

func applyLine(st *memState, line []byte) error {
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return nil
	}
	var r walRecord
	if err := json.Unmarshal(line, &r); err != nil {
		return err
	}
	return st.apply(r)
}

func (j *journal) append(r walRecord) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if _, err := j.buf.Write(b); err != nil {
		return err
	}
	if err := j.buf.WriteByte('\n'); err != nil {
		return err
	}
	j.records++
	if j.syncEach {
		return j.flushSync()
	}
	return nil
}

func (j *journal) flushSync() error {
	if j.buf != nil {
		if err := j.buf.Flush(); err != nil {
			return err
		}
	}
	if j.f != nil {
		return j.f.Sync()
	}
	return nil
}

func (j *journal) maybeCompact(st *memState) error {
	if j.compactEvery <= 0 || j.records < j.compactEvery {
		return nil
	}
	return j.compact(st)
}

func (j *journal) compact(st *memState) error {
	if err := j.flushSync(); err != nil {
		return err
	}
	tmp := j.snapPath + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	if err := enc.Encode(st); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, j.snapPath); err != nil {
		return err
	}
	if err := syncDir(j.dir); err != nil {
		return err
	}
	if err := j.buf.Flush(); err != nil {
		return err
	}
	if err := j.f.Close(); err != nil {
		return err
	}
	wf, err := os.OpenFile(j.walPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if err := wf.Sync(); err != nil {
		wf.Close()
		return err
	}
	j.f = wf
	j.buf = bufio.NewWriterSize(wf, 64*1024)
	j.records = 0
	return syncDir(j.dir)
}

func (j *journal) close() error {
	if j.f == nil {
		return nil
	}
	err := j.flushSync()
	err2 := j.f.Close()
	j.f = nil
	j.buf = nil
	if err != nil {
		return err
	}
	return err2
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
