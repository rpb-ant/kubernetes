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

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/apis/example"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	cachertesting "k8s.io/apiserver/pkg/storage/cacher/testing"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
)

// BenchmarkStalledWatchers measures how soon a watcher that stalled is live
// again, and what slow watchers cost the ones beside them. It sees only what
// clients see. Clients re-open a closed watch at once, from the last
// resourceVersion they saw, so a close shows up as a close and not as
// staleness. It needs wall-clock time; run it with:
//
//	go test ./staging/src/k8s.io/apiserver/pkg/storage/cacher/ -run xxx -bench BenchmarkStalledWatchers -benchtime 1x
//
// All metrics but slow-closes and write-max-ms are of the measured clients:
//
//	unrecovered   clients that stalled and were not live again at the end
//	recover-ms    the slowest of the others, from the end of its pause
//	stale-ms      age of the oldest event a client lacked, worst, from 1s after its pause
//	closes        watches closed by the server
//	expired       of those, closed with "too old resource version", so the client would list again
//	write-max-ms  the longest the watch cache took to accept an event
func BenchmarkStalledWatchers(b *testing.B) {
	second := time.Second
	scenarios := []stalledWatchersScenario{
		// One stall, nobody else is slow.
		{name: "alone/500", writesPerSecond: 500, measured: watchClients{count: 5, pause: second}},
		{name: "alone/900", writesPerSecond: 900, measured: watchClients{count: 5, pause: second}},
		{name: "alone/2000", writesPerSecond: 2000, measured: watchClients{count: 5, pause: second}},
		{name: "alone/5000", writesPerSecond: 5000, measured: watchClients{count: 5, pause: second}},
		// Opened while the history was short, as after a restart, so with the smallest channel.
		{name: "alone-short-history/500", writesPerSecond: 500, shortHistory: true, measured: watchClients{count: 5, pause: second}},
		// No pause: the replay of the events it missed takes longer than the channel lasts.
		{name: "joins-behind/2000", writesPerSecond: 2000, measured: watchClients{count: 5, readsPerSecond: 20000, behind: 40000}},
		// Slow neighbours, writes at a rate that alone/500 recovers from.
		{name: "beside-40-unread/500", writesPerSecond: 500, measured: watchClients{count: 5, pause: 430 * time.Millisecond}, slow: watchClients{count: 40, neverReads: true}},
		{name: "beside-1-slow/500", writesPerSecond: 500, measured: watchClients{count: 5, pause: second}, slow: watchClients{count: 1, readsPerSecond: 100}},
		{name: "beside-40-slow/500", writesPerSecond: 500, measured: watchClients{count: 5, pause: second}, slow: watchClients{count: 40, readsPerSecond: 100}},
		// Clients that never pause.
		{name: "steady-beside-40-slow/5000", writesPerSecond: 5000, measured: watchClients{count: 10}, slow: watchClients{count: 40, readsPerSecond: 1000}},
		{name: "steady-beside-1000-paused/900", writesPerSecond: 900, measured: watchClients{count: 10}, slow: watchClients{count: 1000, pause: second}},
		// An indexed resource has ten times the channel.
		{name: "steady-beside-1000-paused-indexed/900", writesPerSecond: 900, indexed: true, measured: watchClients{count: 10}, slow: watchClients{count: 1000, pause: 4 * second}},
	}
	for _, scenario := range scenarios {
		for _, stallResume := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/StallResume=%v", scenario.name, stallResume), func(b *testing.B) {
				featuregatetesting.SetFeatureGateDuringTest(b, utilfeature.DefaultFeatureGate, features.WatchCacheStallResume, stallResume)
				var r stalledWatchersResult
				for b.Loop() {
					r = runStalledWatchersScenario(b, scenario)
				}
				b.ReportMetric(0, "ns/op")
				b.ReportMetric(float64(r.unrecovered), "unrecovered")
				b.ReportMetric(float64(r.recover.Milliseconds()), "recover-ms")
				b.ReportMetric(float64(r.stale.Milliseconds()), "stale-ms")
				b.ReportMetric(float64(r.closes), "closes")
				b.ReportMetric(float64(r.expired), "expired")
				b.ReportMetric(float64(r.slowCloses), "slow-closes")
				b.ReportMetric(float64(r.writeMax.Milliseconds()), "write-max-ms")
			})
		}
	}
}

// BenchmarkWatchStart measures how long it takes to open a watch.
func BenchmarkWatchStart(b *testing.B) {
	for _, stallResume := range []bool{false, true} {
		b.Run(fmt.Sprintf("StallResume=%v", stallResume), func(b *testing.B) {
			featuregatetesting.SetFeatureGateDuringTest(b, utilfeature.DefaultFeatureGate, features.WatchCacheStallResume, stallResume)
			h := newStalledWatchersHarness(b, false)
			h.write()
			for b.Loop() {
				w, err := h.open(h.lastWritten.Load())
				if err != nil {
					b.Fatal(err)
				}
				w.Stop()
			}
		})
	}
}

const (
	stalledWatchersPauseAt  = 4 * time.Second
	stalledWatchersDuration = 12 * time.Second
	// stalledWatchersGrace is how long a client has after its pause before it counts as stale.
	stalledWatchersGrace = time.Second
	// stalledWatchersLive is how stale a live client can be.
	stalledWatchersLive = 50 * time.Millisecond
	// stalledWatchersHistory gives the history its full size, and with it new watchers their full channels.
	stalledWatchersHistory = 60000
	// stalledWatchersMaxEvents bounds the write times that are kept.
	stalledWatchersMaxEvents = 1 << 18
)

type stalledWatchersScenario struct {
	name            string
	writesPerSecond int
	shortHistory    bool
	indexed         bool
	measured, slow  watchClients
}

type watchClients struct {
	count          int
	pause          time.Duration // once, at stalledWatchersPauseAt
	readsPerSecond int           // 0 is as fast as it can
	neverReads     bool
	behind         uint64 // how many events before the latest it opens its first watch
}

type stalledWatchersResult struct {
	unrecovered, closes, expired, slowCloses int
	recover, stale, writeMax                 time.Duration
}

type stalledWatchersHarness struct {
	b           *testing.B
	cacher      *Cacher
	start       time.Time
	lastWritten atomic.Uint64
	writtenAt   [stalledWatchersMaxEvents]atomic.Int64 // by resourceVersion, since start
	writeMax    time.Duration
	stop        chan struct{}
}

func newStalledWatchersHarness(b *testing.B, indexed bool) *stalledWatchersHarness {
	config := newTestCacherConfig(&cachertesting.MockStorage{})
	if indexed {
		config.IndexerFuncs = storage.IndexerFuncs{"spec.nodeName": func(obj runtime.Object) string { return obj.(*example.Pod).Spec.NodeName }}
	}
	c, err := NewCacherFromConfig(config)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(c.Stop)
	if err := c.Wait(context.Background()); err != nil {
		b.Fatal(err)
	}
	h := &stalledWatchersHarness{b: b, cacher: c, start: time.Now(), stop: make(chan struct{})}
	// Events follow the initial list.
	h.lastWritten.Store(c.watchCache.getListResourceVersion())
	return h
}

// write adds one event. Every event has its own resourceVersion, one more than the last.
func (h *stalledWatchersHarness) write() {
	rv := h.lastWritten.Load() + 1
	pod := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod-%d", rv%2000), Namespace: "ns", ResourceVersion: strconv.FormatUint(rv, 10)}}
	began := time.Since(h.start)
	h.writtenAt[rv].Store(int64(began))
	if err := h.cacher.watchCache.Update(pod); err != nil {
		panic(err)
	}
	h.lastWritten.Store(rv)
	h.writeMax = max(h.writeMax, time.Since(h.start)-began)
}

func (h *stalledWatchersHarness) writeAtRate(writesPerSecond int, until time.Duration) {
	began := time.Since(h.start)
	for n := 0; time.Since(h.start) < until; n++ {
		time.Sleep(began + time.Duration(n)*time.Second/time.Duration(writesPerSecond) - time.Since(h.start))
		h.write()
	}
}

func (h *stalledWatchersHarness) open(resourceVersion uint64) (watch.Interface, error) {
	predicate := storage.Everything
	predicate.AllowWatchBookmarks = true
	return h.cacher.Watch(context.Background(), "/pods/", storage.ListOptions{
		ResourceVersion: strconv.FormatUint(resourceVersion, 10),
		Predicate:       predicate,
		Recursive:       true,
	})
}

type watchClient struct {
	watchClients
	h                       *stalledWatchersHarness
	first                   watch.Interface
	lastSeen                atomic.Uint64
	resumed, recovered      atomic.Int64 // since start, 0 is not yet
	closes, expired         atomic.Int64
	readsFrom               time.Duration
	reads                   int
	encodes                 bool
	paused, seesWatchExpire bool
}

func (c *watchClient) openFirst() {
	c.lastSeen.Store(c.h.lastWritten.Load() - min(c.behind, c.h.lastWritten.Load()))
	w, err := c.h.open(c.lastSeen.Load())
	if err != nil {
		c.h.b.Fatal(err)
	}
	c.first = w
}

// stale is the age of the oldest event that the client lacks.
func (c *watchClient) stale(now time.Duration) time.Duration {
	next := c.lastSeen.Load() + 1
	if next > c.h.lastWritten.Load() {
		return 0
	}
	return now - time.Duration(c.h.writtenAt[next].Load())
}

func (c *watchClient) run() {
	w := c.first
	defer func() { w.Stop() }()
	c.readsFrom = time.Since(c.h.start)
	for {
		if c.neverReads {
			// A closed watch is seen without reading from it.
			select {
			case <-c.h.stop:
				return
			case <-w.(*cacheWatcher).done:
			}
		} else if !c.read(w) {
			return
		}
		w.Stop()
		c.closes.Add(1)
		if c.seesWatchExpire || c.neverReads {
			c.lastSeen.Store(c.h.lastWritten.Load())
			if c.seesWatchExpire {
				c.expired.Add(1)
			}
			c.seesWatchExpire = false
		}
		for {
			next, err := c.h.open(c.lastSeen.Load())
			if err == nil {
				w = next
				break
			}
			c.expired.Add(1)
			c.lastSeen.Store(c.h.lastWritten.Load())
		}
	}
}

// read returns true when the server closed the watch, false when the scenario ended.
func (c *watchClient) read(w watch.Interface) bool {
	for {
		var event watch.Event
		var ok bool
		select {
		case <-c.h.stop:
			return false
		case event, ok = <-w.ResultChan():
		}
		if !ok {
			return true
		}
		if c.pause > 0 && !c.paused && time.Since(c.h.start) > stalledWatchersPauseAt {
			c.paused = true
			time.Sleep(c.pause)
			c.resumed.Store(int64(time.Since(c.h.start)))
		}
		if c.readsPerSecond > 0 {
			c.reads++
			if ahead := time.Duration(c.reads)*time.Second/time.Duration(c.readsPerSecond) - (time.Since(c.h.start) - c.readsFrom); ahead > 2*time.Millisecond {
				time.Sleep(ahead)
			}
		}
		switch event.Type {
		case watch.Error:
			c.seesWatchExpire = true
			continue
		case watch.Bookmark:
			continue
		}
		if c.encodes {
			// What a client costs the API server, without the connection.
			if err := cachertesting.Codecs.LegacyCodec(examplev1.SchemeGroupVersion).Encode(event.Object, io.Discard); err != nil {
				panic(err)
			}
		}
		accessor, err := meta.Accessor(event.Object)
		if err != nil {
			panic(err)
		}
		rv, err := strconv.ParseUint(accessor.GetResourceVersion(), 10, 64)
		if err != nil {
			panic(err)
		}
		if last := c.lastSeen.Load(); rv != last+1 {
			c.h.b.Errorf("event %d followed event %d", rv, last)
		}
		c.lastSeen.Store(rv)
	}
}

func runStalledWatchersScenario(b *testing.B, scenario stalledWatchersScenario) stalledWatchersResult {
	h := newStalledWatchersHarness(b, scenario.indexed)
	h.write()
	newClients := func(config watchClients, encodes bool) []*watchClient {
		clients := make([]*watchClient, config.count)
		for i := range clients {
			clients[i] = &watchClient{watchClients: config, h: h, encodes: encodes}
		}
		return clients
	}
	measured, slow := newClients(scenario.measured, true), newClients(scenario.slow, false)
	var running sync.WaitGroup
	openAll := func(clients []*watchClient) {
		var opening sync.WaitGroup
		for _, c := range clients {
			opening.Go(c.openFirst)
		}
		opening.Wait()
		for _, c := range clients {
			running.Go(c.run)
		}
	}
	for !scenario.shortHistory && h.lastWritten.Load() < stalledWatchersHistory {
		h.write()
	}
	// A long-lived cacher has a full budget when a client stalls.
	h.cacher.dispatchTimeoutBudget.returnUnused(maxBudget)
	for len(h.cacher.incoming) > 0 {
		time.Sleep(time.Millisecond)
	}

	h.writeMax = 0
	h.start = time.Now()
	openAll(slow)
	openAll(measured)
	opened := time.Since(h.start)
	running.Go(func() { h.writeAtRate(scenario.writesPerSecond, stalledWatchersDuration) })

	var r stalledWatchersResult
	joinsBehind := scenario.measured.behind > 0
	for now := opened; now < stalledWatchersDuration; now = time.Since(h.start) {
		for _, c := range measured {
			resumed := time.Duration(c.resumed.Load())
			if joinsBehind {
				resumed = opened + time.Duration(float64(scenario.measured.behind)/float64(scenario.measured.readsPerSecond)*float64(time.Second))
			}
			stalls := scenario.measured.pause > 0 || joinsBehind
			if stalls && (resumed == 0 || now < resumed) {
				continue
			}
			stale := c.stale(now)
			if stalls && stale < stalledWatchersLive && c.recovered.Load() == 0 {
				c.recovered.Store(int64(now))
				r.recover = max(r.recover, now-resumed)
			}
			if now > resumed+stalledWatchersGrace {
				r.stale = max(r.stale, stale)
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	for _, c := range measured {
		if c.recovered.Load() == 0 && (scenario.measured.pause > 0 || joinsBehind) {
			r.unrecovered++
		}
		r.closes += int(c.closes.Load())
		r.expired += int(c.expired.Load())
	}
	for _, c := range slow {
		r.slowCloses += int(c.closes.Load())
	}
	close(h.stop)
	running.Wait()
	r.writeMax = h.writeMax
	return r
}
