/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cacher

// Tests for the WatchCacheStallResume feature gate: a watcher whose input
// channel fills up is poked and catches up from the watch cache history
// instead of being terminated; it is terminated only when its resume
// position has aged out of the history, and then with an in-stream 410.

import (
	"context"
	"fmt"
	"math/rand"
	goruntime "runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/apis/example"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/cacher/metrics"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	compbasemetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/testutil"

	cachertesting "k8s.io/apiserver/pkg/storage/cacher/testing"
)

var stallResumeMetricsOnce sync.Once

// ensureStallResumeMetrics instantiates the stall/resume metric vectors (they
// are lazily created on registration) so tests can read their values.
func ensureStallResumeMetrics() {
	stallResumeMetricsOnce.Do(func() {
		registry := compbasemetrics.NewKubeRegistry()
		for _, m := range []compbasemetrics.Registerable{
			metrics.WatcherStalls, metrics.WatcherDeferredEvents, metrics.WatcherCatchupRounds,
			metrics.WatcherCatchupEvents, metrics.TerminatedWatchersCounter,
		} {
			_ = registry.Register(m)
		}
	})
}

// stallResumeCounters is a snapshot of the stall/resume counters for pods.
type stallResumeCounters struct {
	stalls, deferred, rounds float64
	expired, expiredInitial  float64
	unresponsive             float64
}

func readStallResumeCounters(t testing.TB) stallResumeCounters {
	t.Helper()
	ensureStallResumeMetrics()
	read := func(m compbasemetrics.CounterMetric) float64 {
		v, err := testutil.GetCounterMetricValue(m)
		if err != nil {
			t.Fatalf("reading counter: %v", err)
		}
		return v
	}
	return stallResumeCounters{
		stalls:         read(metrics.WatcherStalls.WithLabelValues("", "pods")),
		deferred:       read(metrics.WatcherDeferredEvents.WithLabelValues("", "pods")),
		rounds:         read(metrics.WatcherCatchupRounds.WithLabelValues("", "pods")),
		expired:        read(metrics.TerminatedWatchersCounter.WithLabelValues("", "pods", metrics.TerminationReasonResourceExpired)),
		expiredInitial: read(metrics.TerminatedWatchersCounter.WithLabelValues("", "pods", metrics.TerminationReasonResourceExpiredInitial)),
		unresponsive:   read(metrics.TerminatedWatchersCounter.WithLabelValues("", "pods", metrics.TerminationReasonUnresponsive)),
	}
}

// newStallResumeCacher enables the feature gate for the duration of the test
// and builds a cacher over MockStorage.
func newStallResumeCacher(t *testing.T) *Cacher {
	t.Helper()
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.WatchCacheStallResume, true)
	ensureStallResumeMetrics()
	cacher, _, err := newTestCacher(&cachertesting.MockStorage{})
	if err != nil {
		t.Fatalf("Couldn't create cacher: %v", err)
	}
	if cacher.stall == nil {
		t.Fatalf("expected the cacher to run in stall/resume mode")
	}
	t.Cleanup(cacher.Stop)
	return cacher
}

// stallResumePod builds pod-<rv> in namespace "ns" with the given resourceVersion.
func stallResumePod(rv uint64) *examplev1.Pod {
	return &examplev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            fmt.Sprintf("pod-%d", rv),
			Namespace:       "ns",
			ResourceVersion: strconv.FormatUint(rv, 10),
		},
	}
}

// stallResumeAddPods adds pods pod-<from>..pod-<to>, each at its own RV.
func stallResumeAddPods(t testing.TB, c *Cacher, from, to uint64) {
	t.Helper()
	for rv := from; rv <= to; rv++ {
		if err := c.watchCache.Add(stallResumePod(rv)); err != nil {
			t.Fatalf("failed to add a pod: %v", err)
		}
	}
}

// stallResumeWatch opens an all-pods-in-ns watch from the given resourceVersion.
func stallResumeWatch(t testing.TB, c *Cacher, rv uint64) watch.Interface {
	t.Helper()
	pred := storage.Everything
	pred.AllowWatchBookmarks = true
	w, err := c.Watch(context.Background(), "/pods/ns", storage.ListOptions{ResourceVersion: strconv.FormatUint(rv, 10), Predicate: pred})
	if err != nil {
		t.Fatalf("Failed to create watch: %v", err)
	}
	return w
}

// stallResumeCollect reads object events until it has seen the object at
// resourceVersion `until`, the watch closes, or the timeout fires. It fails
// the test on an unexpected error event and returns the object RVs in
// delivery order plus whether the channel was closed.
func stallResumeCollect(t testing.TB, w watch.Interface, until uint64, timeout time.Duration) (rvs []uint64, closed bool) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case ev, ok := <-w.ResultChan():
			if !ok {
				return rvs, true
			}
			if ev.Type == watch.Error {
				t.Fatalf("unexpected error event: %#v", ev.Object)
			}
			if ev.Type == watch.Bookmark {
				continue
			}
			rv, err := storage.APIObjectVersioner{}.ObjectResourceVersion(ev.Object)
			if err != nil {
				t.Fatalf("parsing resource version: %v", err)
			}
			rvs = append(rvs, rv)
			if rv >= until {
				return rvs, false
			}
		case <-deadline:
			t.Fatalf("timed out waiting for events; got %d so far", len(rvs))
		}
	}
}

// assertExactSequence verifies rvs is exactly from, from+1, ..., to.
func assertExactSequence(t testing.TB, rvs []uint64, from, to uint64) {
	t.Helper()
	want := int(to - from + 1)
	if len(rvs) != want {
		t.Fatalf("expected %d object events (%d..%d), got %d: %v", want, from, to, len(rvs), rvs)
	}
	for i, rv := range rvs {
		if rv != from+uint64(i) {
			t.Fatalf("event %d: expected resourceVersion %d, got %d (sequence: %v)", i, from+uint64(i), rv, rvs)
		}
	}
}

// TestStallResumeSurvivesStall is the core property: a client that stops
// reading while the writer produces far more than the watcher's buffers can
// hold is not terminated, loses nothing and sees strict resourceVersion
// order once it resumes. With the gate off (the control that proves the
// schedule really overflows the watcher), it is terminated.
func TestStallResumeSurvivesStall(t *testing.T) {
	// The client is not reading while ~15x the watcher's total buffering
	// (input+result, chanSize 10 each) is written.
	const first, last = 101, 400

	t.Run("WatchCacheStallResume=true", func(t *testing.T) {
		cacher := newStallResumeCacher(t)
		stallResumeAddPods(t, cacher, 100, 100)
		w := stallResumeWatch(t, cacher, 100)
		defer w.Stop()
		before := readStallResumeCounters(t)

		stallResumeAddPods(t, cacher, first, last)

		rvs, closed := stallResumeCollect(t, w, last, 10*time.Second)
		if closed {
			t.Fatalf("watch closed after %d events; expected it to survive the stall", len(rvs))
		}
		assertExactSequence(t, rvs, first, last)
		after := readStallResumeCounters(t)
		if after.stalls-before.stalls < 1 {
			t.Errorf("expected at least one stall episode, got %v", after.stalls-before.stalls)
		}
		if after.deferred-before.deferred < 1 {
			t.Errorf("expected deferred events, got %v", after.deferred-before.deferred)
		}
		if after.rounds-before.rounds < 1 {
			t.Errorf("expected at least one catch-up round, got %v", after.rounds-before.rounds)
		}
		terms := (after.expired + after.expiredInitial + after.unresponsive) - (before.expired + before.expiredInitial + before.unresponsive)
		if terms != 0 {
			t.Errorf("expected no terminations, got %v", terms)
		}
		// Nothing more should arrive.
		select {
		case ev, ok := <-w.ResultChan():
			t.Errorf("unexpected extra event %#v (ok=%v)", ev, ok)
		case <-time.After(200 * time.Millisecond):
		}
	})

	t.Run("WatchCacheStallResume=false", func(t *testing.T) {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.WatchCacheStallResume, false)
		cacher, _, err := newTestCacher(&cachertesting.MockStorage{})
		if err != nil {
			t.Fatalf("Couldn't create cacher: %v", err)
		}
		defer cacher.Stop()
		stallResumeAddPods(t, cacher, 100, 100)
		w := stallResumeWatch(t, cacher, 100)
		defer w.Stop()
		before := readStallResumeCounters(t)

		stallResumeAddPods(t, cacher, first, last)

		// Today's behavior: the watch is terminated (a clean close) well
		// before it could deliver everything.
		delivered := 0
		for range w.ResultChan() {
			delivered++
		}
		if delivered >= last-first {
			t.Errorf("gate off: expected the watcher to be terminated during the stall, but it delivered %d events", delivered)
		}
		if after := readStallResumeCounters(t); after.unresponsive-before.unresponsive < 1 {
			t.Errorf("gate off: expected terminated{reason=unresponsive} to increase")
		}
	})
}

// TestStallResumeOrderingRace is the mandatory -race ordering test: a real
// watchCache and dispatcher race failed enqueues against subsequent
// successful enqueues at one watcher while the client drains at random. An
// event that failed to enqueue must never be delivered after an event with a
// higher resourceVersion, and every event is delivered exactly once.
func TestStallResumeOrderingRace(t *testing.T) {
	cacher := newStallResumeCacher(t)
	stallResumeAddPods(t, cacher, 100, 100)
	w := stallResumeWatch(t, cacher, 100)
	defer w.Stop()
	before := readStallResumeCounters(t)

	const last = 1000
	writerErr := make(chan error, 1)
	go func() {
		defer close(writerErr)
		for rv := uint64(101); rv <= last; rv++ {
			if err := cacher.watchCache.Add(stallResumePod(rv)); err != nil {
				writerErr <- err
				return
			}
			// jitter so failed and successful enqueues interleave
			if rand.Intn(20) == 0 {
				time.Sleep(time.Microsecond * time.Duration(rand.Intn(100)))
			}
		}
	}()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	var rvs []uint64
	deadline := time.After(30 * time.Second)
	for len(rvs) == 0 || rvs[len(rvs)-1] < last {
		select {
		case ev, ok := <-w.ResultChan():
			if !ok {
				t.Fatalf("watch closed unexpectedly after %d events", len(rvs))
			}
			if ev.Type == watch.Error {
				t.Fatalf("unexpected error event: %#v", ev.Object)
			}
			if ev.Type == watch.Bookmark {
				continue
			}
			rv, err := storage.APIObjectVersioner{}.ObjectResourceVersion(ev.Object)
			if err != nil {
				t.Fatal(err)
			}
			rvs = append(rvs, rv)
			// random reader stalls
			if rng.Intn(50) == 0 {
				time.Sleep(time.Millisecond * time.Duration(rng.Intn(3)))
			}
		case <-deadline:
			t.Fatalf("timed out; got %d events", len(rvs))
		}
	}
	if err := <-writerErr; err != nil {
		t.Fatalf("writer failed: %v", err)
	}
	assertExactSequence(t, rvs, 101, last)
	if after := readStallResumeCounters(t); after.stalls-before.stalls < 1 {
		t.Errorf("no enqueue failure occurred; the race the test targets was not exercised")
	}
}

// TestStallResumeCoalesceSchedule is the mandatory -race coalesce test:
// three bursts of enqueue failures — a partial client drain after the
// first, a full drain after the second, the third landing on a fully wedged
// watcher. Both latch branches are witnessed: failures coalescing onto an
// already-pending token (deferred events strictly exceed stall episodes) and
// a fresh token opened after the watcher consumed the previous one (the full
// drain before burst 3 empties the latch, so burst 3 must open a new
// episode: at least two episodes in total). Every event of every burst is
// delivered exactly once, in order.
func TestStallResumeCoalesceSchedule(t *testing.T) {
	cacher := newStallResumeCacher(t)
	stallResumeAddPods(t, cacher, 100, 100)
	w := stallResumeWatch(t, cacher, 100)
	defer w.Stop()
	before := readStallResumeCounters(t)

	// Burst 1: overflow input+result with the client silent.
	stallResumeAddPods(t, cacher, 101, 160)
	// One partial drain: read a handful of events so the watcher's
	// processing goroutine unblocks and starts a catch-up round.
	drained := 0
	deadline := time.After(10 * time.Second)
	var rvs []uint64
	for drained < 5 {
		select {
		case ev, ok := <-w.ResultChan():
			if !ok {
				t.Fatalf("watch closed unexpectedly")
			}
			if ev.Type == watch.Error {
				t.Fatalf("unexpected error event: %#v", ev.Object)
			}
			if ev.Type == watch.Bookmark {
				continue
			}
			rv, err := storage.APIObjectVersioner{}.ObjectResourceVersion(ev.Object)
			if err != nil {
				t.Fatal(err)
			}
			rvs = append(rvs, rv)
			drained++
		case <-deadline:
			t.Fatalf("timed out draining")
		}
	}
	// Burst 2: overflow again while the previous round may still be running.
	stallResumeAddPods(t, cacher, 161, 240)

	rest, closed := stallResumeCollect(t, w, 240, 10*time.Second)
	if closed {
		t.Fatalf("watch closed unexpectedly")
	}
	rvs = append(rvs, rest...)
	assertExactSequence(t, rvs, 101, 240)
	beforeBurst3 := readStallResumeCounters(t)

	// Burst 3: the client is fully caught up and now reads nothing while
	// far more than its buffering is written. The latch is empty before it
	// (the previous rounds consumed their tokens), so this burst must open
	// at least one fresh episode.
	stallResumeAddPods(t, cacher, 241, 400)
	rvs, closed = stallResumeCollect(t, w, 400, 10*time.Second)
	if closed {
		t.Fatalf("watch closed unexpectedly")
	}
	assertExactSequence(t, rvs, 241, 400)

	after := readStallResumeCounters(t)
	stalls := after.stalls - before.stalls
	deferred := after.deferred - before.deferred
	// Fresh-token branch: at least burst 1 and burst 3 opened an episode
	// each (burst 3 followed a fully drained, token-free watcher).
	if stalls < 2 {
		t.Errorf("expected at least 2 stall episodes over the schedule (a fresh token after a consume), got %v", stalls)
	}
	if got := after.stalls - beforeBurst3.stalls; got < 1 {
		t.Errorf("burst 3 opened no fresh episode: %v", got)
	}
	// Coalescing branch: many more failed enqueues than episodes.
	if deferred <= stalls {
		t.Errorf("expected deferred events (%v) to exceed stall episodes (%v): failures coalescing onto a pending token", deferred, stalls)
	}
	if got := after.deferred - beforeBurst3.deferred; got < 100 {
		t.Errorf("burst 3: expected >= 100 deferred events, got %v", got)
	}
}

// TestStallResumeChaosOracle races one writer against many watchers with
// randomized reader stalls and different filters. Oracle: each watcher's
// delivered object events are exactly the written events matching its
// filter from its start resourceVersion, strictly increasing, no gaps, no
// duplicates.
func TestStallResumeChaosOracle(t *testing.T) {
	cacher := newStallResumeCacher(t)

	makePod := func(rv uint64) *example.Pod {
		return &example.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:            fmt.Sprintf("pod-%d", rv%50), // 50 keys => updates
				Namespace:       "ns",
				ResourceVersion: strconv.FormatUint(rv, 10),
			},
			Spec: example.PodSpec{NodeName: fmt.Sprintf("node-%d", rv%5)},
		}
	}
	// Seed all 50 keys so every later event is an update of an existing key,
	// which keeps the "matches filter" oracle a pure function of the RV (a
	// key never changes node: key rv%%50 => node rv%%5, constant per key).
	const seedEnd = uint64(1049)
	for rv := uint64(1000); rv <= seedEnd; rv++ {
		if err := cacher.watchCache.Add(makePod(rv)); err != nil {
			t.Fatal(err)
		}
	}

	type watcherSpec struct {
		node   int // -1: all pods, else spec.nodeName filter
		stalls int // number of random reader stalls
	}
	specs := []watcherSpec{{-1, 0}, {-1, 3}, {-1, 8}, {2, 3}, {4, 6}, {-1, 20}}
	const last = seedEnd + 800

	// wanted computes the exact event sequence a watcher of this spec must
	// see: every written RV matching its node filter, in increasing order.
	wanted := func(spec watcherSpec) []uint64 {
		var want []uint64
		for rv := seedEnd + 1; rv <= last; rv++ {
			if spec.node < 0 || rv%5 == uint64(spec.node) {
				want = append(want, rv)
			}
		}
		return want
	}

	type result struct {
		rvs    []uint64
		closed bool
	}
	results := make([]result, len(specs))
	before := readStallResumeCounters(t)
	var wg sync.WaitGroup
	watchers := make([]watch.Interface, len(specs))
	for i, spec := range specs {
		pred := storage.Everything
		if spec.node >= 0 {
			pred = storage.SelectionPredicate{
				Label: labels.Everything(),
				Field: fields.OneTermEqualSelector("spec.nodeName", fmt.Sprintf("node-%d", spec.node)),
			}
		}
		w, err := cacher.Watch(context.Background(), "/pods/ns", storage.ListOptions{ResourceVersion: strconv.FormatUint(seedEnd, 10), Predicate: pred})
		if err != nil {
			t.Fatalf("Failed to create watch: %v", err)
		}
		watchers[i] = w
		wg.Add(1)
		go func(i int, spec watcherSpec, w watch.Interface) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(i)*7919 + 13))
			deadline := time.After(60 * time.Second)
			wantCount := len(wanted(spec))
			stallEvery := 0
			if spec.stalls > 0 {
				stallEvery = wantCount / spec.stalls
			}
			res := &results[i]
			for len(res.rvs) < wantCount {
				select {
				case ev, ok := <-w.ResultChan():
					if !ok {
						res.closed = true
						return
					}
					if ev.Type == watch.Error {
						t.Errorf("watcher %d: unexpected error event: %#v", i, ev.Object)
						res.closed = true
						return
					}
					if ev.Type == watch.Bookmark {
						continue
					}
					rv, err := storage.APIObjectVersioner{}.ObjectResourceVersion(ev.Object)
					if err != nil {
						t.Errorf("watcher %d: %v", i, err)
						return
					}
					res.rvs = append(res.rvs, rv)
					if stallEvery > 0 && len(res.rvs)%stallEvery == 0 {
						time.Sleep(time.Duration(2+rng.Intn(8)) * time.Millisecond)
					}
				case <-deadline:
					t.Errorf("watcher %d: timed out after %d events", i, len(res.rvs))
					return
				}
			}
		}(i, spec, w)
	}

	for rv := seedEnd + 1; rv <= last; rv++ {
		if err := cacher.watchCache.Update(makePod(rv)); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	for _, w := range watchers {
		w.Stop()
	}
	if after := readStallResumeCounters(t); after.stalls-before.stalls < 1 {
		t.Errorf("the schedule did not stall any watcher; the test would not exercise catch-up (stalls delta %v)", after.stalls-before.stalls)
	}

	for i, spec := range specs {
		res := results[i]
		if res.closed {
			t.Errorf("watcher %d closed unexpectedly", i)
			continue
		}
		want := wanted(spec)
		if len(res.rvs) != len(want) {
			t.Errorf("watcher %d (node=%d): expected %d events, got %d", i, spec.node, len(want), len(res.rvs))
			continue
		}
		for j := range want {
			if res.rvs[j] != want[j] {
				t.Errorf("watcher %d (node=%d): event %d: want RV %d got %d", i, spec.node, j, want[j], res.rvs[j])
				break
			}
		}
	}
}

// TestStallResumeBookmarkPredicate pins the delivery predicate for
// bookmarks in stall/resume mode: a bookmark at or above the position is
// delivered, a bookmark below the position (leapt over by a catch-up round)
// is dropped, and bookmarks never move the position.
func TestStallResumeBookmarkPredicate(t *testing.T) {
	cacher := newStallResumeCacher(t)
	stallResumeAddPods(t, cacher, 100, 100)
	w := stallResumeWatch(t, cacher, 100)
	defer w.Stop()
	cw := w.(*cacheWatcher)

	expectEvent := func(want watch.EventType, wantRV uint64) {
		t.Helper()
		select {
		case ev, ok := <-w.ResultChan():
			if !ok {
				t.Fatalf("watch closed unexpectedly")
			}
			rv, _ := storage.APIObjectVersioner{}.ObjectResourceVersion(ev.Object)
			if ev.Type != want || rv != wantRV {
				t.Fatalf("expected %v@%d, got %v@%d", want, wantRV, ev.Type, rv)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %v@%d", want, wantRV)
		}
	}
	expectNothing := func() {
		t.Helper()
		select {
		case ev, ok := <-w.ResultChan():
			t.Fatalf("expected no event, got %#v (ok=%v)", ev, ok)
		case <-time.After(100 * time.Millisecond):
		}
	}
	bookmark := func(rv uint64) *watchCacheEvent {
		return &watchCacheEvent{Type: watch.Bookmark, ResourceVersion: rv, Object: &examplev1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: strconv.FormatUint(rv, 10)}}}
	}

	// One object event: the position is now 101.
	stallResumeAddPods(t, cacher, 101, 101)
	expectEvent(watch.Added, 101)

	// A bookmark equal to the position is delivered ...
	if !cw.nonblockingAdd(bookmark(101)) {
		t.Fatal("failed to enqueue bookmark")
	}
	expectEvent(watch.Bookmark, 101)
	// ... one below the position is dropped ...
	if !cw.nonblockingAdd(bookmark(100)) {
		t.Fatal("failed to enqueue bookmark")
	}
	expectNothing()
	// ... and a higher one is delivered without moving the position (the
	// dispatcher never enqueues a bookmark ahead of a lower object event,
	// so this order is synthetic; it shows the position is object-driven).
	if !cw.nonblockingAdd(bookmark(150)) {
		t.Fatal("failed to enqueue bookmark")
	}
	expectEvent(watch.Bookmark, 150)
	stallResumeAddPods(t, cacher, 102, 102)
	expectEvent(watch.Added, 102)
}

// TestStallResumeWakesUpWithoutFurtherInput proves the stall latch is a
// blocking wake-up and not just a flag polled when input events arrive: an
// event that reached the watch cache history but not this watcher's input
// channel is delivered by a catch-up round triggered from the latch alone,
// with no further dispatch to the watcher.
func TestStallResumeWakesUpWithoutFurtherInput(t *testing.T) {
	cacher := newStallResumeCacher(t)
	stallResumeAddPods(t, cacher, 100, 100)
	w := stallResumeWatch(t, cacher, 100)
	defer w.Stop()
	cw := w.(*cacheWatcher)

	// Deliver one event normally so the watcher is live at position 101.
	stallResumeAddPods(t, cacher, 101, 101)
	if rvs, _ := stallResumeCollect(t, w, 101, 5*time.Second); len(rvs) != 1 || rvs[0] != 101 {
		t.Fatalf("unexpected initial delivery: %v", rvs)
	}

	// Append an event to the watch cache history WITHOUT dispatching it to
	// any watcher (the eventHandler is what feeds the dispatcher).
	// Unsynchronized swap of the event handler: safe here only because the
	// MockStorage-backed reflector delivers no events concurrently (its
	// watch never fires), so nothing else calls processEvent meanwhile.
	wc := cacher.watchCache
	saved := wc.config.eventHandler
	wc.config.eventHandler = nil
	if err := wc.Add(stallResumePod(102)); err != nil {
		t.Fatal(err)
	}
	wc.config.eventHandler = saved

	// The watcher's input is empty and quiet. Poke it: only the latch's
	// blocking select arm can now make it fetch RV 102 from the history.
	select {
	case ev, ok := <-w.ResultChan():
		t.Fatalf("expected no event before the poke, got %#v (ok=%v)", ev, ok)
	default:
	}
	rounds := readStallResumeCounters(t).rounds
	cw.poke(102)
	rvs, closed := stallResumeCollect(t, w, 102, 5*time.Second)
	if closed || len(rvs) != 1 || rvs[0] != 102 {
		t.Fatalf("expected RV 102 delivered via a latch-triggered round, got %v (closed=%v)", rvs, closed)
	}
	if got := readStallResumeCounters(t).rounds - rounds; got != 1 {
		t.Errorf("expected exactly one catch-up round, got %v", got)
	}
}

// TestStallResumeInitialListWatchers covers watchers whose first interval
// comes from the store (a legacy resourceVersion=0 watch and a WatchList
// SendInitialEvents watch): a stall during the initial listing is followed
// by a catch-up from the history, never a second listing, and the initial
// events, the watch-list end bookmark and the subsequent stream are all
// delivered exactly once and in order.
func TestStallResumeInitialListWatchers(t *testing.T) {
	for _, watchList := range []bool{false, true} {
		t.Run(fmt.Sprintf("watchList=%v", watchList), func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.WatchList, true)
			forceRequestWatchProgressSupport(t)
			cacher := newStallResumeCacher(t)
			// Seed more objects than the watcher's result buffer so the
			// initial listing itself blocks on the (silent) client.
			const seedFrom, seedTo = 100, 149
			stallResumeAddPods(t, cacher, seedFrom, seedTo)

			pred := storage.Everything
			opts := storage.ListOptions{Predicate: pred, ResourceVersion: "0"}
			if watchList {
				trueVal := true
				pred.AllowWatchBookmarks = true
				opts = storage.ListOptions{Predicate: pred, SendInitialEvents: &trueVal, ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			w, err := cacher.Watch(ctx, "/pods/ns", opts)
			if err != nil {
				t.Fatalf("Failed to create watch: %v", err)
			}
			defer w.Stop()

			// The client reads nothing while the writer overflows the
			// watcher's buffers.
			const last = 400
			stallResumeAddPods(t, cacher, seedTo+1, last)

			// Now consume everything: the initial Added events (any order),
			// the end bookmark iff watchList, then seedTo+1..last in RV order.
			seen := map[uint64]int{}
			var ordered []uint64
			bookmarks := 0
			deadline := time.After(30 * time.Second)
		read:
			for {
				select {
				case ev, ok := <-w.ResultChan():
					if !ok {
						t.Fatalf("watch closed unexpectedly after %d events", len(seen))
					}
					switch ev.Type {
					case watch.Error:
						t.Fatalf("unexpected error event: %#v", ev.Object)
					case watch.Bookmark:
						bookmarks++
						continue
					}
					rv, err := storage.APIObjectVersioner{}.ObjectResourceVersion(ev.Object)
					if err != nil {
						t.Fatal(err)
					}
					seen[rv]++
					ordered = append(ordered, rv)
					if rv == last {
						break read
					}
				case <-deadline:
					t.Fatalf("timed out; %d distinct events so far", len(seen))
				}
			}
			for rv := uint64(seedFrom); rv <= last; rv++ {
				if seen[rv] != 1 {
					t.Errorf("resourceVersion %d delivered %d times, want exactly once", rv, seen[rv])
				}
			}
			// After the initial listing everything must be in increasing order.
			if len(ordered) < seedTo-seedFrom+1 {
				t.Fatalf("too few events: %d", len(ordered))
			}
			postInit := ordered[seedTo-seedFrom+1:]
			for i, rv := range postInit {
				if rv != seedTo+1+uint64(i) {
					t.Errorf("post-init event %d: want RV %d, got %d", i, seedTo+1+uint64(i), rv)
					break
				}
			}
			if watchList && bookmarks < 1 {
				t.Errorf("expected the watch-list initial-events-end bookmark")
			}
			if !watchList && bookmarks != 0 {
				t.Errorf("expected no bookmarks for a plain rv=0 watch, got %d", bookmarks)
			}
		})
	}
}

// TestStallResumeDeleteEventsThroughCatchUp verifies that delete events and
// filter transitions served from the history during a catch-up round are
// converted exactly like live deliveries.
func TestStallResumeDeleteEventsThroughCatchUp(t *testing.T) {
	cacher := newStallResumeCacher(t)
	makePod := func(name string, rv uint64, labelled bool) *examplev1.Pod {
		p := &examplev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", ResourceVersion: strconv.FormatUint(rv, 10)}}
		if labelled {
			p.Labels = map[string]string{"watched": "true"}
		}
		return p
	}
	// One matching pod establishes the starting position.
	if err := cacher.watchCache.Add(makePod("seed", 100, true)); err != nil {
		t.Fatal(err)
	}
	pred := storage.SelectionPredicate{
		Label: labels.SelectorFromSet(labels.Set{"watched": "true"}),
		Field: fields.Everything(),
	}
	w, err := cacher.Watch(context.Background(), "/pods/ns", storage.ListOptions{ResourceVersion: "100", Predicate: pred})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	type observed struct {
		typ  watch.EventType
		name string
		rv   uint64
	}
	before := readStallResumeCounters(t)
	var expected []observed
	rv := uint64(101)
	// Fillers inside the filter: ADDED, and enough of them to overflow the
	// silent watcher's buffers so the interesting events below are served by
	// a catch-up round.
	for i := 0; i < 30; i++ {
		name := fmt.Sprintf("filler-%d", i)
		if err := cacher.watchCache.Add(makePod(name, rv, true)); err != nil {
			t.Fatal(err)
		}
		expected = append(expected, observed{watch.Added, name, rv})
		rv++
	}
	// victim: added inside the filter ...
	if err := cacher.watchCache.Add(makePod("victim", rv, true)); err != nil {
		t.Fatal(err)
	}
	expected = append(expected, observed{watch.Added, "victim", rv})
	rv++
	// ... leaves the filter (label removed) => DELETED with the new RV ...
	if err := cacher.watchCache.Update(makePod("victim", rv, false)); err != nil {
		t.Fatal(err)
	}
	expected = append(expected, observed{watch.Deleted, "victim", rv})
	rv++
	// ... re-enters the filter => ADDED ...
	if err := cacher.watchCache.Update(makePod("victim", rv, true)); err != nil {
		t.Fatal(err)
	}
	expected = append(expected, observed{watch.Added, "victim", rv})
	rv++
	// ... and is really deleted => DELETED carrying the deletion RV.
	if err := cacher.watchCache.Delete(makePod("victim", rv, true)); err != nil {
		t.Fatal(err)
	}
	expected = append(expected, observed{watch.Deleted, "victim", rv})
	rv++
	// filler-0 flapping in and out of the filter.
	inFilter := true
	for i := 0; i < 20; i++ {
		labelled := i%2 == 0
		if err := cacher.watchCache.Update(makePod("filler-0", rv, labelled)); err != nil {
			t.Fatal(err)
		}
		switch {
		case inFilter && labelled:
			expected = append(expected, observed{watch.Modified, "filler-0", rv})
		case inFilter && !labelled:
			expected = append(expected, observed{watch.Deleted, "filler-0", rv})
		case !inFilter && labelled:
			expected = append(expected, observed{watch.Added, "filler-0", rv})
		}
		inFilter = labelled
		rv++
	}

	// Now the client reads; it must see exactly the expected sequence.
	var got []observed
	deadline := time.After(10 * time.Second)
	for len(got) < len(expected) {
		select {
		case ev, ok := <-w.ResultChan():
			if !ok {
				t.Fatalf("watch closed after %d events", len(got))
			}
			if ev.Type == watch.Error {
				t.Fatalf("unexpected error event: %#v", ev.Object)
			}
			if ev.Type == watch.Bookmark {
				continue
			}
			acc, err := apimeta.Accessor(ev.Object)
			if err != nil {
				t.Fatal(err)
			}
			rv, _ := strconv.ParseUint(acc.GetResourceVersion(), 10, 64)
			got = append(got, observed{ev.Type, acc.GetName(), rv})
		case <-deadline:
			t.Fatalf("timed out after %d/%d events", len(got), len(expected))
		}
	}
	for i := range expected {
		if got[i] != expected[i] {
			t.Errorf("event %d: expected %+v, got %+v", i, expected[i], got[i])
		}
	}
	select {
	case ev, ok := <-w.ResultChan():
		t.Errorf("unexpected extra event %#v (ok=%v)", ev, ok)
	case <-time.After(200 * time.Millisecond):
	}
	if after := readStallResumeCounters(t); after.stalls-before.stalls < 1 {
		t.Errorf("the schedule did not stall the watcher; the test would not exercise catch-up")
	}
}

// TestStallResumeSingleKeyWatch covers a watch on one named object: its
// catch-up scans the whole history segment and filters it down to its own
// key, delivering that key's events in order and nothing else.
func TestStallResumeSingleKeyWatch(t *testing.T) {
	cacher := newStallResumeCacher(t)
	target := &examplev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "target", Namespace: "ns", ResourceVersion: "100"}}
	if err := cacher.watchCache.Add(target); err != nil {
		t.Fatal(err)
	}
	w, err := cacher.Watch(context.Background(), "/pods/ns/target", storage.ListOptions{ResourceVersion: "100", Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Failed to create watch: %v", err)
	}
	defer w.Stop()

	// Interleave updates of the target with writes to unrelated pods while
	// the client reads nothing; well over the watcher's buffering (every
	// event, not only the target's, lands in this watcher's input channel).
	before := readStallResumeCounters(t)
	var targetRVs []uint64
	rv := uint64(101)
	for i := 0; i < 150; i++ {
		if i%3 == 0 {
			pod := target.DeepCopy()
			pod.ResourceVersion = strconv.FormatUint(rv, 10)
			if err := cacher.watchCache.Update(pod); err != nil {
				t.Fatal(err)
			}
			targetRVs = append(targetRVs, rv)
		} else {
			if err := cacher.watchCache.Add(&examplev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("other-%d", i), Namespace: "ns", ResourceVersion: strconv.FormatUint(rv, 10)}}); err != nil {
				t.Fatal(err)
			}
		}
		rv++
	}

	rvs, closed := stallResumeCollect(t, w, targetRVs[len(targetRVs)-1], 10*time.Second)
	if closed {
		t.Fatalf("watch closed unexpectedly")
	}
	if len(rvs) != len(targetRVs) {
		t.Fatalf("expected %d events for the target key, got %d: %v", len(targetRVs), len(rvs), rvs)
	}
	for i := range rvs {
		if rvs[i] != targetRVs[i] {
			t.Errorf("event %d: want RV %d got %d", i, targetRVs[i], rvs[i])
		}
	}
	if after := readStallResumeCounters(t); after.stalls-before.stalls < 1 {
		t.Errorf("the schedule did not stall the watcher; the test would not exercise catch-up")
	}
}

// pinRingCapacity fixes the watch cache history capacity so events age out
// deterministically once more than `capacity` newer events exist.
func pinRingCapacity(t *testing.T, c *Cacher, capacity int) {
	t.Helper()
	wc := c.watchCache
	wc.Lock()
	defer wc.Unlock()
	wc.history.lowerBoundCapacity = capacity
	wc.history.upperBoundCapacity = capacity
	if wc.history.capacity != capacity {
		wc.history.doCacheResizeLocked(capacity)
	}
}

// expectExpiredThenClose reads the watch until it closes and requires the
// sequence to end with exactly one 410 ResourceExpired ERROR event followed
// by the channel close (any number of object events may precede the error).
func expectExpiredThenClose(t *testing.T, w watch.Interface) {
	t.Helper()
	deadline := time.After(15 * time.Second)
	sawError := false
	for {
		select {
		case ev, ok := <-w.ResultChan():
			if !ok {
				if !sawError {
					t.Fatalf("watch closed without the 410 error event")
				}
				return
			}
			if ev.Type == watch.Error {
				if sawError {
					t.Fatalf("more than one error event")
				}
				sawError = true
				status, ok := ev.Object.(*metav1.Status)
				if !ok || !apierrors.IsResourceExpired(apierrors.FromObject(status)) || status.Code != 410 {
					t.Fatalf("expected a 410 ResourceExpired status, got %#v", ev.Object)
				}
				continue
			}
			if sawError {
				t.Fatalf("received %v event after the error event", ev.Type)
			}
		case <-deadline:
			t.Fatalf("timed out waiting for the 410 and close (sawError=%v)", sawError)
		}
	}
}

// TestStallResumeExpiryReasons verifies the honest termination: a watcher
// whose resume position has aged out of the history receives an in-stream
// 410 (ERROR) followed by a clean close, and the terminated counter records
// resource_expired for a watcher that had reached the live stream and
// resource_expired_initial for one that stalled during its initial listing.
func TestStallResumeExpiryReasons(t *testing.T) {
	t.Run("live watcher ages out", func(t *testing.T) {
		cacher := newStallResumeCacher(t)
		pinRingCapacity(t, cacher, 100)
		stallResumeAddPods(t, cacher, 100, 100)
		w := stallResumeWatch(t, cacher, 100)
		defer w.Stop()
		before := readStallResumeCounters(t)

		// One live delivery so the watcher has reached the live stream.
		stallResumeAddPods(t, cacher, 101, 101)
		if rvs, _ := stallResumeCollect(t, w, 101, 5*time.Second); len(rvs) != 1 {
			t.Fatalf("unexpected initial delivery: %v", rvs)
		}
		// Stall for far more events than the pinned history holds.
		stallResumeAddPods(t, cacher, 102, 401)

		expectExpiredThenClose(t, w)
		after := readStallResumeCounters(t)
		if after.expired-before.expired != 1 {
			t.Errorf("expected terminated{reason=resource_expired} +1, got %v", after.expired-before.expired)
		}
		if after.expiredInitial-before.expiredInitial != 0 {
			t.Errorf("expected no resource_expired_initial, got %v", after.expiredInitial-before.expiredInitial)
		}
	})

	t.Run("initial-list watcher ages out", func(t *testing.T) {
		cacher := newStallResumeCacher(t)
		pinRingCapacity(t, cacher, 100)
		// More initial objects than the result buffer so hydration blocks.
		stallResumeAddPods(t, cacher, 100, 149)
		w, err := cacher.Watch(context.Background(), "/pods/ns", storage.ListOptions{ResourceVersion: "0", Predicate: storage.Everything})
		if err != nil {
			t.Fatalf("Failed to create watch: %v", err)
		}
		defer w.Stop()
		before := readStallResumeCounters(t)

		// While the client is silent, write enough to age the initial
		// position out of the pinned history.
		stallResumeAddPods(t, cacher, 150, 449)

		expectExpiredThenClose(t, w)
		after := readStallResumeCounters(t)
		if after.expiredInitial-before.expiredInitial != 1 {
			t.Errorf("expected terminated{reason=resource_expired_initial} +1, got %v", after.expiredInitial-before.expiredInitial)
		}
		if after.expired-before.expired != 0 {
			t.Errorf("expected no resource_expired, got %v", after.expired-before.expired)
		}
	})
}

// TestStallResumeStopMidCatchUp verifies a client-side stop while a catch-up
// round is blocked on a slow client: the watcher exits promptly (no
// goroutine leak), sends no error event, and tolerates further pokes (the
// stall latch is never closed).
func TestStallResumeStopMidCatchUp(t *testing.T) {
	cacher := newStallResumeCacher(t)

	stallResumeAddPods(t, cacher, 100, 100)
	w := stallResumeWatch(t, cacher, 100)
	cw := w.(*cacheWatcher)
	stallResumeAddPods(t, cacher, 101, 300)
	// Let the asynchronous dispatch finish and the watcher settle blocked on
	// its full result buffer.
	if err := wait.PollUntilContextTimeout(context.Background(), time.Millisecond, 5*time.Second, true, func(context.Context) (bool, error) {
		return len(cacher.incoming) == 0 && len(cw.result) == cap(cw.result), nil
	}); err != nil {
		t.Fatalf("watcher never settled into the wedged state: %v", err)
	}

	// Read a few events so the processing goroutine runs a catch-up round
	// (the interval is far larger than the result buffer), which refills the
	// result buffer and then blocks on the silent client mid-round.
	if rvs, closed := stallResumeCollect(t, w, 105, 5*time.Second); closed || len(rvs) != 5 {
		t.Fatalf("unexpected prefix: %v (closed=%v)", rvs, closed)
	}
	if err := wait.PollUntilContextTimeout(context.Background(), time.Millisecond, 5*time.Second, true, func(context.Context) (bool, error) {
		return len(cw.result) == cap(cw.result), nil
	}); err != nil {
		t.Fatalf("watcher never refilled its result buffer: %v", err)
	}
	w.Stop()
	// A poke against a stopped watcher must not panic: the latch is never
	// closed.
	cw.poke(301)

	// The result channel is closed by the exiting goroutine; drain it.
	deadline := time.After(5 * time.Second)
drain:
	for {
		select {
		case ev, ok := <-w.ResultChan():
			if !ok {
				break drain
			}
			if ev.Type == watch.Error {
				t.Errorf("no error event expected on a client-side stop, got %#v", ev.Object)
			}
		case <-deadline:
			t.Fatalf("timed out waiting for the result channel to close")
		}
	}

	// The result channel is closed by the watcher's processing goroutine on
	// its way out; additionally make sure no watcher processing goroutine is
	// left running (a leak would show up as a goroutine in processInterval /
	// process / catchUp).
	watcherFrames := func() int {
		buf := make([]byte, 1<<20)
		n := goruntime.Stack(buf, true)
		count := 0
		for _, marker := range []string{".processInterval(", ".catchUp(", ".streamInterval("} {
			count += strings.Count(string(buf[:n]), marker)
		}
		return count
	}
	if err := wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 10*time.Second, true, func(context.Context) (bool, error) {
		return watcherFrames() == 0, nil
	}); err != nil {
		t.Errorf("watcher goroutine leaked: %d watcher frames still running", watcherFrames())
	}
}

// The dispatch fork placement (live deliveries still carry the shared
// cachingObject, i.e. the fork happens after setCachingObjects) is pinned by
// TestCachingObjects, which runs in both gate states.

// TestStallResumeScopedWatcherStalePosition covers the resume-position
// staleness hazard for scope-filtered watchers: a watcher registered under a
// name scope receives no object events while the rest of the collection
// churns the ring past its start position, so when a burst of its own
// events finally overflows its input, its position is far outside the
// history. The catch-up must resume from the missed event (which is in the
// ring), not from the stale position, and deliver everything — with or
// without bookmarks. Without the gate the same burst is absorbed by
// dispatch's blocking-add grace, so a 410 here would be a regression, not
// parity.
func TestStallResumeScopedWatcherStalePosition(t *testing.T) {
	for _, bookmarks := range []bool{false, true} {
		t.Run(fmt.Sprintf("bookmarks=%v", bookmarks), func(t *testing.T) {
			cacher := newStallResumeCacher(t)
			pinRingCapacity(t, cacher, 100)
			target := &examplev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "target", Namespace: "ns", ResourceVersion: "100"}}
			if err := cacher.watchCache.Add(target); err != nil {
				t.Fatal(err)
			}
			pred := storage.SelectionPredicate{
				Label:               labels.Everything(),
				Field:               fields.OneTermEqualSelector("metadata.name", "target"),
				AllowWatchBookmarks: bookmarks,
			}
			w, err := cacher.Watch(context.Background(), "/pods/ns/target", storage.ListOptions{ResourceVersion: "100", Predicate: pred})
			if err != nil {
				t.Fatalf("Failed to create watch: %v", err)
			}
			defer w.Stop()

			// Churn 200 unrelated pods: the pinned 100-slot ring now holds
			// RVs 201-300, all invisible to the scoped watcher, whose
			// position is still 100 — aged out of the ring.
			rv := uint64(101)
			for i := 0; i < 200; i++ {
				if err := cacher.watchCache.Add(&examplev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("other-%d", i), Namespace: "ns", ResourceVersion: strconv.FormatUint(rv, 10)}}); err != nil {
					t.Fatal(err)
				}
				rv++
			}

			if bookmarks {
				// Deliver a bookmark at the ring head the way the cacher's
				// periodic bookmark path does (dispatchEvent ->
				// nonblockingAdd) and wait for the client to receive it: a
				// bookmark far above the last object event must not change
				// where the resume starts either.
				bookmarkObj := &examplev1.Pod{}
				if err := (storage.APIObjectVersioner{}).UpdateObject(bookmarkObj, 300); err != nil {
					t.Fatal(err)
				}
				if !w.(*cacheWatcher).nonblockingAdd(&watchCacheEvent{Type: watch.Bookmark, Object: bookmarkObj, ResourceVersion: 300}) {
					t.Fatalf("bookmark did not fit in the watcher's input channel")
				}
				deadline := time.After(10 * time.Second)
				for got := false; !got; {
					select {
					case ev, ok := <-w.ResultChan():
						if !ok {
							t.Fatalf("watch closed while waiting for the bookmark")
						}
						if ev.Type == watch.Error {
							t.Fatalf("unexpected error event: %#v", ev.Object)
						}
						got = ev.Type == watch.Bookmark
					case <-deadline:
						t.Fatalf("timed out waiting for the bookmark")
					}
				}
			}

			// Burst enough target updates to overrun the watcher's buffering
			// while the client reads nothing: the stall's catch-up must
			// resume from the first missed event, not the aged-out start
			// position (100).
			before := readStallResumeCounters(t)
			var targetRVs []uint64
			for i := 0; i < 60; i++ {
				pod := target.DeepCopy()
				pod.ResourceVersion = strconv.FormatUint(rv, 10)
				if err := cacher.watchCache.Update(pod); err != nil {
					t.Fatal(err)
				}
				targetRVs = append(targetRVs, rv)
				rv++
			}
			rvs, closed := stallResumeCollect(t, w, targetRVs[len(targetRVs)-1], 10*time.Second)
			if closed {
				t.Fatalf("watch closed unexpectedly after %d/%d events: %v", len(rvs), len(targetRVs), rvs)
			}
			assertExactSequence(t, rvs, targetRVs[0], targetRVs[len(targetRVs)-1])
			if after := readStallResumeCounters(t); after.stalls-before.stalls < 1 {
				t.Errorf("the schedule did not stall the watcher; the test would not exercise the resume position")
			}
		})
	}
}
