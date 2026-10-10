// Copyright 2026 The Wasp Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storer

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethersphere/bee/v2/pkg/log"
)

// budgetClock drives clock in a test. Tests using it must not run in
// parallel, because clock is a package variable.
type budgetClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *budgetClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *budgetClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func useFakeClock(t *testing.T) *budgetClock {
	t.Helper()
	c := &budgetClock{now: time.Unix(1_000_000, 0)}
	old, oldPause := clock, maxEvictionPause
	clock = c.Now
	maxEvictionPause = 15 * time.Minute
	t.Cleanup(func() { clock, maxEvictionPause = old, oldPause })
	return c
}

func newBudgetDB(buf io.Writer) *DB {
	logger := log.Noop
	if buf != nil {
		logger = log.NewLogger("test", log.WithSink(buf), log.WithVerbosity(log.VerbosityInfo))
	}
	return &DB{metrics: newMetrics(), logger: logger, quit: make(chan struct{})}
}

// waitSlice models one poll of a waiting eviction: decide, charge the
// time until the next poll, and report whether it waited.
func waitSlice(db *DB, c *budgetClock, d time.Duration) bool {
	reason := db.evictionPause()
	db.countWait(reason)
	c.advance(d)
	db.countWait(db.evictionPause())
	return reason != pauseNone
}

// Back-to-back /rchash samples, with gaps shorter than a poll, used to hold
// eviction off for as long as they continued, because each new stretch had
// a fresh cap (#659). With the budget, eviction waits for at most 15
// minutes of every 30, then runs.
func TestPauseBudgetBackToBackSamples(t *testing.T) {
	c := useFakeClock(t)
	db := newBudgetDB(nil)

	waited := time.Duration(0)
	ran := false
	for i := range 40 { // 40 samples of 30 s, back to back
		db.sampleStarted(false)
		if waitSlice(db, c, 30*time.Second) {
			waited += 30 * time.Second
		} else {
			ran = true
		}
		db.sampleDone(false)
		_ = i
	}
	if !ran {
		t.Fatal("eviction never ran during 20 minutes of back-to-back samples")
	}
	if waited > maxEvictionPause {
		t.Fatalf("eviction waited %v, want at most %v", waited, maxEvictionPause)
	}
	if got := metricValue(db.metrics.EvictionPauseCapped); got != 1 {
		t.Fatalf("capped counter %v, want 1 (one change to the bound)", got)
	}
}

// Once the budget is used and samples stop, old waiting leaves the window,
// and a later sample pauses eviction again.
func TestPauseBudgetWindowFrees(t *testing.T) {
	c := useFakeClock(t)
	db := newBudgetDB(nil)

	db.sampleStarted(false)
	for range 70 { // 17.5 minutes of polls: the budget is used up
		waitSlice(db, c, 15*time.Second)
	}
	db.sampleDone(false)
	if db.pauseBudget.waited(c.Now()) < maxEvictionPause {
		t.Fatal("budget not used up after a sample longer than the bound")
	}

	c.advance(31 * time.Minute)
	db.sampleStarted(false)
	defer db.sampleDone(false)
	if db.evictionPause() != pauseForOther {
		t.Fatal("after the window passed, a new sample does not pause eviction")
	}
}

// A lottery sample always pauses eviction, even with the budget used up by
// other samples (#659 must not undo #649).
func TestPauseBudgetLotteryExempt(t *testing.T) {
	c := useFakeClock(t)
	db := newBudgetDB(nil)

	db.sampleStarted(false)
	for range 70 {
		waitSlice(db, c, 15*time.Second)
	}
	if db.evictionPause() != pauseNone {
		t.Fatal("budget not used up")
	}

	db.sampleStarted(true)
	if db.evictionPause() != pauseLottery {
		t.Fatal("a lottery sample does not pause eviction once other samples used the budget")
	}
	// Waiting for the lottery is not charged to the budget.
	before := db.pauseBudget.waited(c.Now())
	waitSlice(db, c, time.Minute)
	if after := db.pauseBudget.waited(c.Now()); after > before {
		t.Fatalf("waiting for a lottery sample was charged to the budget: %v to %v", before, after)
	}
	db.sampleDone(true)
	db.sampleDone(false)
}

// A stuck lottery sample is still capped per stretch (no change from #653).
func TestPauseBudgetLotteryStretchCap(t *testing.T) {
	c := useFakeClock(t)
	db := newBudgetDB(nil)

	db.sampleStarted(true)
	defer db.sampleDone(true)
	if db.evictionPause() != pauseLottery {
		t.Fatal("lottery sample does not pause eviction")
	}
	c.advance(maxEvictionPause)
	if db.evictionPause() != pauseNone {
		t.Fatal("a lottery sample past the stretch cap still pauses eviction")
	}
}

// The wait in progress counts: one sample longer than the bound ends its
// own wait at the bound, without another interval closing first.
func TestPauseBudgetCountsWaitInProgress(t *testing.T) {
	c := useFakeClock(t)
	db := newBudgetDB(nil)

	db.sampleStarted(false)
	defer db.sampleDone(false)
	db.countWait(db.evictionPause()) // the wait opens
	c.advance(maxEvictionPause)
	if db.evictionPause() != pauseNone {
		t.Fatal("a single wait at the bound, still in progress, does not end the pause")
	}
}

// The Warning is logged at most once per window, while the counter counts
// each change to the bound.
func TestPauseBudgetWarningOncePerWindow(t *testing.T) {
	c := useFakeClock(t)
	var buf bytes.Buffer
	db := newBudgetDB(&buf)

	reach := func() {
		db.sampleStarted(false)
		db.countWait(db.evictionPause())
		c.advance(maxEvictionPause)
		_ = db.evictionPause() // reaches the bound
		db.countWait(pauseNone)
		db.sampleDone(false)
	}
	reach()
	c.advance(time.Minute) // still inside the window: bound still reached
	db.sampleStarted(false)
	_ = db.evictionPause()
	db.sampleDone(false)

	c.advance(2 * maxEvictionPause) // the window passed
	reach()

	if got := metricValue(db.metrics.EvictionPauseCapped); got != 2 {
		t.Fatalf("capped counter %v, want 2 (two changes to the bound)", got)
	}
	if n := strings.Count(buf.String(), "used up their share"); n != 2 {
		t.Fatalf("%d warnings, want 2 (one per window)", n)
	}
}

// The progress line is written at most once per interval, carries the
// run's kind and counts, and is also written while eviction waits.
func TestProgressLine(t *testing.T) {
	c := useFakeClock(t)
	old := progressLineInterval
	progressLineInterval = time.Minute
	t.Cleanup(func() { progressLineInterval = old })

	var buf bytes.Buffer
	db := newBudgetDB(&buf)

	db.progressStart(progressKindExpired, 3)
	db.progressRound(1000)
	if strings.Contains(buf.String(), "reserve eviction progress") {
		t.Fatal("progress line before the interval")
	}
	c.advance(time.Minute)
	db.progressRound(1000)
	db.progressRound(1000) // same instant: not a second line
	if n := strings.Count(buf.String(), "reserve eviction progress"); n != 1 {
		t.Fatalf("%d lines, want 1", n)
	}
	if !strings.Contains(buf.String(), `"kind"="expired"`) || !strings.Contains(buf.String(), `"batches_left"=3`) {
		t.Fatalf("line lacks the run's kind or batches left: %s", buf.String())
	}

	c.advance(time.Minute)
	db.progressLine(true) // from the wait for a sample
	if !strings.Contains(buf.String(), `"paused"=true`) {
		t.Fatalf("no line from the wait: %s", buf.String())
	}

	db.progressBatchDone()
	db.progressBatchDone()
	if got := metricValue(db.metrics.EvictionExpiredBatchesRemaining); got != 1 {
		t.Fatalf("expired batches remaining %v, want 1", got)
	}
	db.progressEnd()
	if got := metricValue(db.metrics.EvictionExpiredBatchesRemaining); got != 0 {
		t.Fatalf("expired batches remaining after the run %v, want 0", got)
	}
}

// The waiting loop charges each poll by why it waited: a wait that spans a
// lottery sample and a following /rchash charges only the /rchash part.
func TestPauseBudgetSplitPerPoll(t *testing.T) {
	c := useFakeClock(t)
	db := newBudgetDB(nil)

	db.sampleStarted(true)
	db.sampleStarted(false)
	waitSlice(db, c, 5*time.Minute) // both run: the lottery's wait
	db.sampleDone(true)
	waitSlice(db, c, 2*time.Minute) // the /rchash alone
	db.sampleDone(false)

	if got := db.pauseBudget.waited(c.Now()); got != 2*time.Minute {
		t.Fatalf("charged %v, want 2m (only the /rchash part)", got)
	}
}

// The wait for a sample writes the progress line with the run's state, so
// a long pause still shows in the journal (#626).
func TestProgressLineFromTheWait(t *testing.T) {
	c := useFakeClock(t)
	oldPoll, oldLine := samplingPausePollVar, progressLineInterval
	samplingPausePollVar, progressLineInterval = time.Millisecond, time.Minute
	t.Cleanup(func() { samplingPausePollVar, progressLineInterval = oldPoll, oldLine })

	buf := &lockedBuffer{}
	db := newBudgetDB(buf)
	db.progressStart(progressKindUnreserve, 0)
	db.sampleStarted(true)

	done := make(chan error, 1)
	go func() { done <- db.waitWhileSampling(context.Background(), nil) }()
	c.advance(2 * time.Minute) // the line is due while eviction waits
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(buf.String(), `"paused"=true`) {
		if time.Now().After(deadline) {
			t.Fatalf("no progress line from the wait: %s", buf.String())
		}
		time.Sleep(time.Millisecond)
	}
	db.sampleDone(true)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// lockedBuffer is a log sink safe to read while another goroutine logs.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}
