//go:build watchrig

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
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	goruntime "runtime"
	"runtime/metrics"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/apis/example"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
	"k8s.io/apiserver/pkg/storage"
	cachermetrics "k8s.io/apiserver/pkg/storage/cacher/metrics"
	utilflowcontrol "k8s.io/apiserver/pkg/util/flowcontrol"
	"k8s.io/client-go/tools/cache"
	compbasemetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"

	cachertesting "k8s.io/apiserver/pkg/storage/cacher/testing"
)

const rigGroup, rigResource = "", "pods"

// ---------------------------------------------------------------------------
// klog capture: route all klog output to a per-run file and count the two
// termination-cause signatures the product emits.

type rigLogSink struct {
	mu          sync.Mutex
	file        *os.File
	forcing     int64 // "Forcing ... watcher close due to unresponsiveness" (add() force-close)
	invalidated int64 // "couldn't retrieve watch event to serve" (replay interval invalidated)
	terminating int64 // "Terminating watcher: resume position no longer in the watch cache history" (gate-on 410)
	incomingMax int64 // max N from "N objects queued in incoming channel" (the product's own incoming HWM)
}

// incomingRe extracts the product-side incoming-queue high-water lines:
// "cacher (pods): 87 objects queued in incoming channel."
var incomingRe = regexp.MustCompile(`(\d+) objects queued in incoming channel`)

func (s *rigLogSink) Write(p []byte) (int, error) {
	line := string(p)
	if strings.Contains(line, "watcher close due to unresponsiveness") {
		atomic.AddInt64(&s.forcing, 1)
	}
	if strings.Contains(line, "couldn't retrieve watch event to serve") {
		atomic.AddInt64(&s.invalidated, 1)
	}
	if strings.Contains(line, "resume position no longer in the watch cache history") {
		atomic.AddInt64(&s.terminating, 1)
	}
	if m := incomingRe.FindStringSubmatch(line); m != nil {
		if v, err := strconv.ParseInt(m[1], 10, 64); err == nil {
			for {
				cur := atomic.LoadInt64(&s.incomingMax)
				if v <= cur || atomic.CompareAndSwapInt64(&s.incomingMax, cur, v) {
					break
				}
			}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		return s.file.Write(p)
	}
	return len(p), nil
}

func (s *rigLogSink) setFile(f *os.File) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		s.file.Close()
	}
	s.file = f
	atomic.StoreInt64(&s.forcing, 0)
	atomic.StoreInt64(&s.invalidated, 0)
	atomic.StoreInt64(&s.terminating, 0)
	atomic.StoreInt64(&s.incomingMax, 0)
}

var (
	rigLog     = &rigLogSink{}
	rigLogOnce sync.Once
)

// rigInitKlog points klog at rigLog with verbosity 2 so the V(1) force-close
// line and the interval-invalidation warning are captured. Called once.
func rigInitKlog() {
	rigLogOnce.Do(func() {
		fs := flag.NewFlagSet("rigklog", flag.ContinueOnError)
		klog.InitFlags(fs)
		_ = fs.Set("logtostderr", "false")
		_ = fs.Set("alsologtostderr", "false")
		_ = fs.Set("stderrthreshold", "FATAL")
		_ = fs.Set("v", "2")
		klog.LogToStderr(false)
		klog.SetOutput(rigLog)
	})
}

// ---------------------------------------------------------------------------
// output

type rigOut struct {
	mu     sync.Mutex
	file   *os.File
	enc    *json.Encoder
	encErr error // first Encode failure (e.g. NaN); makes the cell fail loudly instead of dropping lines
}

func newRigOut(path string) (*rigOut, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &rigOut{file: f, enc: json.NewEncoder(f)}, nil
}

func (o *rigOut) emit(v map[string]any) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.enc.Encode(v); err != nil && o.encErr == nil {
		o.encErr = fmt.Errorf("jsonl encode failed for kind=%v: %v", v["kind"], err)
	}
}

func (o *rigOut) close() { o.file.Close() }

// ---------------------------------------------------------------------------
// the run

type rigRun struct {
	cell   rigCell
	rep    int
	outdir string
	out    *rigOut

	cacher *Cacher
	gr     schema.GroupResource

	// timing
	setupT0 time.Time // wall start of setup
	t0      time.Time // run clock start (after seed/pregap)
	runEnd  time.Time

	// writer state (single writer goroutine mutates; atomics for readers)
	rvNext     uint64 // next RV to assign (atomic)
	rvBase     uint64
	writes     int64 // total events written (atomic)
	writeNanos []int64
	writeMu    sync.RWMutex // guards writeNanos growth (readers RLock)
	keys       []string
	keyNode    []int
	nodeWrites []int64 // atomic per node
	created    []bool
	podSample  *example.Pod
	objBytes   int

	// clients
	clients []*rigClient

	// telemetry
	termBase       float64
	termSeries     []termSample
	termMu         sync.Mutex
	incomingHWM    int64
	firstTermNanos int64 // atomic; unix nanos of first observed terminated-counter increment

	// stall/resume metric bases (deltas are reported) and process-cost peaks,
	// updated under termMu by the 1 Hz tick
	stallsBase, deferredBase, roundsBase, catchupEventsBase       float64
	termExpiredBase, termExpiredInitialBase, termUnresponsiveBase float64
	rssMaxMB, heapMaxMB, stalledGaugeMax                          float64
	goroutinesMax                                                 int
	cpuSamples                                                    []float64

	wg     sync.WaitGroup
	cancel context.CancelFunc
	ctx    context.Context
}

type termSample struct {
	AtMs  int64   // ms since t0
	Total float64 // counter value
}

func (r *rigRun) sinceStart(t time.Time) float64 { return t.Sub(r.t0).Seconds() }

// rigTerminatedReasons lists the terminated_watchers_total reason label
// values the rig sums over (the counter gained a reason label).
var rigTerminatedReasons = []string{"unresponsive", "resource_expired", "resource_expired_initial", "stalled_client"}

// terminatedTotal reads apiserver_terminated_watchers_total for pods, summed
// over the reason label.
func rigTerminatedTotal() float64 {
	sum := 0.0
	for _, reason := range rigTerminatedReasons {
		v, err := testutil.GetCounterMetricValue(cachermetrics.TerminatedWatchersCounter.WithLabelValues(rigGroup, rigResource, reason))
		if err != nil {
			return math.NaN()
		}
		sum += v
	}
	return sum
}

func rigInitEventsTotal() float64 {
	v, err := testutil.GetCounterMetricValue(cachermetrics.InitCounter.WithLabelValues(rigGroup, rigResource))
	if err != nil {
		return math.NaN()
	}
	return v
}

// rigStallResumeMetricsOnce instantiates the WatchCacheStallResume metric
// vectors regardless of the gate, so the rig reads well-defined zeros with the
// gate off (they are lazily created on registration).
var rigStallResumeMetricsOnce sync.Once

func rigEnsureStallResumeMetrics() {
	rigStallResumeMetricsOnce.Do(func() {
		reg := compbasemetrics.NewKubeRegistry()
		for _, m := range []compbasemetrics.Registerable{
			cachermetrics.WatcherStalls, cachermetrics.WatcherDeferredEvents, cachermetrics.WatcherCatchupRounds,
			cachermetrics.WatcherCatchupEvents, cachermetrics.TerminatedWatchersCounter,
		} {
			_ = reg.Register(m)
		}
	})
}

func rigCounter(m compbasemetrics.CounterMetric) float64 {
	v, err := testutil.GetCounterMetricValue(m)
	if err != nil {
		return -1
	}
	return v
}

func rigTermReason(reason string) float64 {
	return rigCounter(cachermetrics.TerminatedWatchersCounter.WithLabelValues(rigGroup, rigResource, reason))
}

// rigStallCounters reads the WatchCacheStallResume counters for pods.
func rigStallCounters() (stalls, deferred, rounds, catchupEvents float64) {
	stalls = rigCounter(cachermetrics.WatcherStalls.WithLabelValues(rigGroup, rigResource))
	deferred = rigCounter(cachermetrics.WatcherDeferredEvents.WithLabelValues(rigGroup, rigResource))
	rounds = rigCounter(cachermetrics.WatcherCatchupRounds.WithLabelValues(rigGroup, rigResource))
	if sum, err := testutil.GetHistogramMetricValue(cachermetrics.WatcherCatchupEvents.WithLabelValues(rigGroup, rigResource)); err == nil {
		catchupEvents = sum
	} else {
		catchupEvents = -1
	}
	return
}

// rigStalledGauge reported apiserver_watch_cache_stalled_watchers before the
// gauge (with the sampler and cull) was removed from the gated core; the
// rig keeps the telemetry column and reports "unavailable".
func rigStalledGauge() float64 {
	return -1
}

// rigRSSMB reads the process resident set size from /proc.
func rigRSSMB() float64 {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return -1
	}
	f := strings.Fields(string(b))
	if len(f) < 2 {
		return -1
	}
	pages, err := strconv.ParseInt(f[1], 10, 64)
	if err != nil {
		return -1
	}
	return float64(pages*int64(os.Getpagesize())) / (1 << 20)
}

// ---------------------------------------------------------------------------
// building the cacher

func (r *rigRun) buildCacher() error {
	cfg := r.cell.Cache
	fresh := cfg.FreshDuration
	if fresh == 0 {
		fresh = DefaultEventFreshDuration
	}
	getAttrs := func(obj runtime.Object) (labels.Set, fields.Set, error) {
		pod, ok := obj.(*example.Pod)
		if !ok {
			return storage.DefaultNamespaceScopedAttr(obj)
		}
		l := labels.Set(pod.Labels)
		f := fields.Set{
			"metadata.name":      pod.Name,
			"metadata.namespace": pod.Namespace,
			"spec.nodeName":      pod.Spec.NodeName,
		}
		return l, f, nil
	}
	config := Config{
		Storage:             &cachertesting.MockStorage{},
		Versioner:           storage.APIObjectVersioner{},
		GroupResource:       schema.GroupResource{Group: rigGroup, Resource: rigResource},
		EventsHistoryWindow: fresh,
		ResourcePrefix:      "/pods/",
		KeyFunc:             func(obj runtime.Object) (string, error) { return storage.NamespaceKeyFunc("/pods/", obj) },
		GetAttrsFunc:        getAttrs,
		NewFunc:             func() runtime.Object { return &example.Pod{} },
		NewListFunc:         func() runtime.Object { return &example.PodList{} },
		Codec:               codecs.LegacyCodec(examplev1.SchemeGroupVersion),
		Clock:               clock.RealClock{},
	}
	if cfg.Indexed {
		nodeNameFn := func(obj runtime.Object) string {
			if pod, ok := obj.(*example.Pod); ok {
				return pod.Spec.NodeName
			}
			return ""
		}
		config.IndexerFuncs = map[string]storage.IndexerFunc{"spec.nodeName": nodeNameFn}
		config.Indexers = &cache.Indexers{
			storage.FieldIndex("spec.nodeName"): func(obj interface{}) ([]string, error) {
				pod, ok := obj.(*example.Pod)
				if !ok {
					return nil, fmt.Errorf("not a pod")
				}
				return []string{pod.Spec.NodeName}, nil
			},
		}
	}
	c, err := NewCacherFromConfig(config)
	if err != nil {
		return err
	}
	r.cacher = c
	r.gr = config.GroupResource
	waitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Wait(waitCtx); err != nil {
		return fmt.Errorf("cacher not ready: %w", err)
	}
	if cfg.PinCapacity > 0 {
		wc := c.watchCache
		wc.Lock()
		if cfg.PinCapacity < wc.history.capacity {
			wc.Unlock()
			return fmt.Errorf("PinCapacity %d below initial capacity %d (shrink not supported by rig)", cfg.PinCapacity, wc.history.capacity)
		}
		wc.history.lowerBoundCapacity = cfg.PinCapacity
		wc.history.upperBoundCapacity = cfg.PinCapacity
		if cfg.PinCapacity != wc.history.capacity {
			wc.history.doCacheResizeLocked(cfg.PinCapacity)
		}
		wc.Unlock()
	}
	return nil
}

// ---------------------------------------------------------------------------
// object generation and the writer

func (r *rigRun) pod(keyIdx int, rv uint64) *example.Pod {
	name := r.keys[keyIdx]
	node := fmt.Sprintf("node-%d", r.keyNode[keyIdx])
	p := &example.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "ns",
			Name:            name,
			ResourceVersion: strconv.FormatUint(rv, 10),
			Labels:          map[string]string{"app": "rig"},
		},
		Spec: example.PodSpec{NodeName: node},
	}
	if pb := r.cell.Writers.PadBytes; pb > 0 {
		p.Annotations = map[string]string{"rig/pad": strings.Repeat("x", pb)}
	}
	return p
}

// writeOne assigns the next RV, injects one event through the reflector
// entry point (watchCache.Add for a new key, Update afterwards) and stamps
// the write time for latency accounting.
func (r *rigRun) writeOne(keyIdx int) {
	rv := atomic.AddUint64(&r.rvNext, 1) - 1
	p := r.pod(keyIdx, rv)
	// stamp BEFORE the write: the write can block on the cacher's incoming
	// chan (dispatch backpressure), which is part of the delivery latency we
	// want to see; stamping first also guarantees a consumer never receives
	// an event whose timestamp is not yet recorded.
	r.writeMu.Lock()
	r.writeNanos = append(r.writeNanos, time.Now().UnixNano())
	r.writeMu.Unlock()
	var err error
	if r.created[keyIdx] {
		err = r.cacher.watchCache.Update(p)
	} else {
		r.created[keyIdx] = true
		err = r.cacher.watchCache.Add(p)
	}
	if err != nil {
		panic(fmt.Sprintf("rig: watchCache write failed: %v", err))
	}
	atomic.AddInt64(&r.nodeWrites[r.keyNode[keyIdx]], 1)
	atomic.AddInt64(&r.writes, 1)
}

func (r *rigRun) writeNanosAt(rv uint64) (int64, bool) {
	if rv < r.rvBase {
		return 0, false
	}
	idx := int(rv - r.rvBase)
	r.writeMu.RLock()
	defer r.writeMu.RUnlock()
	if idx >= len(r.writeNanos) {
		return 0, false
	}
	return r.writeNanos[idx], true
}

// setupKeys prepares the key space (names, node assignment).
func (r *rigRun) setupKeys() {
	wc := r.cell.Writers
	nkeys := wc.Keys
	if nkeys == 0 {
		nkeys = r.cell.Cache.Seed
	}
	if nkeys == 0 {
		nkeys = 1000
	}
	nodes := wc.Nodes
	if nodes <= 0 {
		nodes = 1
	}
	r.keys = make([]string, nkeys)
	r.keyNode = make([]int, nkeys)
	r.created = make([]bool, nkeys)
	r.nodeWrites = make([]int64, nodes)
	for i := 0; i < nkeys; i++ {
		r.keys[i] = fmt.Sprintf("pod-%06d", i)
		r.keyNode[i] = i % nodes
	}
	r.rvBase = 10000
	r.rvNext = r.rvBase
	r.writeNanos = make([]int64, 0, 1<<20)
	// approximate delivered-object byte size (json) for relist pricing
	if b, err := json.Marshal(r.pod(0, 1)); err == nil {
		r.objBytes = len(b)
	}
}

// seedAndPregap writes Cache.Seed distinct pods and then PreGap update
// events, all before the run clock starts.
func (r *rigRun) seedAndPregap() {
	for i := 0; i < r.cell.Cache.Seed && i < len(r.keys); i++ {
		r.writeOne(i)
	}
	pre := r.cell.Writers.PreGap
	for i := 0; i < pre; i++ {
		r.writeOne(r.pickKey(int64(i)))
	}
}

// pickKey chooses the key for steady-state update number seq.
func (r *rigRun) pickKey(seq int64) int {
	wc := r.cell.Writers
	nkeys := len(r.keys)
	if wc.HotNodes > 0 && wc.Nodes > 0 {
		// only keys on hot nodes
		hotKeys := 0
		for i := 0; i < nkeys; i++ {
			if r.keyNode[i] < wc.HotNodes {
				hotKeys++
			}
		}
		if hotKeys > 0 {
			// keys with keyNode < HotNodes are exactly i where i%Nodes < HotNodes
			// (assignment is i%Nodes); pick the (seq%hotKeys)-th such key.
			target := int(seq % int64(hotKeys))
			// index of the target-th hot key: k = (target/HotNodes)*Nodes + target%HotNodes
			k := (target/wc.HotNodes)*wc.Nodes + target%wc.HotNodes
			if k < nkeys {
				return k
			}
		}
	}
	return int(seq % int64(nkeys))
}

// runWriter runs the open-loop steady + burst schedule until stop.
func (r *rigRun) runWriter(stop <-chan struct{}) {
	defer r.wg.Done()
	wc := r.cell.Writers
	rate := wc.Rate
	bursts := append([]rigBurst(nil), wc.Bursts...)
	sort.Slice(bursts, func(i, j int) bool { return bursts[i].At < bursts[j].At })
	var seq int64
	// target(t) = steady + completed portion of bursts
	target := func(el time.Duration) float64 {
		n := rate * el.Seconds()
		for _, b := range bursts {
			if el < b.At {
				continue
			}
			done := b.Rate * (el - b.At).Seconds()
			if done > float64(b.Count) {
				done = float64(b.Count)
			}
			n += done
		}
		return n
	}
	stopAt := r.cell.Duration - wc.StopBeforeEnd
	for {
		select {
		case <-stop:
			return
		default:
		}
		el := time.Since(r.t0)
		if el >= stopAt {
			return
		}
		want := int64(target(el))
		if seq < want {
			// catch up (open-loop): write until caught up, re-checking stop
			for seq < want {
				select {
				case <-stop:
					return
				default:
				}
				r.writeOne(r.pickKey(seq))
				seq++
			}
			continue
		}
		// sleep until next event is due (bounded so bursts start on time)
		var sleep time.Duration
		if rate > 0 {
			sleep = time.Duration((float64(seq+1) - rate*el.Seconds()) / rate * float64(time.Second))
		} else {
			sleep = 5 * time.Millisecond
		}
		if sleep < 200*time.Microsecond {
			sleep = 200 * time.Microsecond
		}
		if sleep > 5*time.Millisecond {
			sleep = 5 * time.Millisecond
		}
		time.Sleep(sleep)
	}
}

// ---------------------------------------------------------------------------
// the watch client

// rigSignal is the flow-control initialization signal; process() fires it
// exactly when replay ends and the input consumer starts, which is how the
// rig classifies a termination as replaying-vs-live without a product seam.
type rigSignal struct {
	ch      chan struct{}
	once    sync.Once
	firedAt int64 // atomic unix nanos; 0 = not fired
}

func newRigSignal() *rigSignal { return &rigSignal{ch: make(chan struct{})} }
func (s *rigSignal) Signal() {
	s.once.Do(func() {
		atomic.StoreInt64(&s.firedAt, time.Now().UnixNano())
		close(s.ch)
	})
}
func (s *rigSignal) Wait() { <-s.ch }
func (s *rigSignal) fired() bool {
	select {
	case <-s.ch:
		return true
	default:
		return false
	}
}

// rigWatchState is per-watch-open state. It is allocated fresh for every
// Watch call and captured by the done-stamping goroutine, so a late stamp
// from a previous watch can never leak into the next one.
type rigWatchState struct {
	cw          *cacheWatcher
	sig         *rigSignal
	openAt      int64         // unix nanos of the open
	doneAt      int64         // atomic: unix nanos when cw.done closed (any stop path)
	stamped     chan struct{} // closed once doneAt is stamped
	replayBound uint64        // cache RV read just before Watch: events with RV <= this are replay
}

type rigClient struct {
	r     *rigRun
	group rigWatcherGroup
	gidx  int // index within group
	name  string
	node  int // watched node index (nodename kind)

	// current watch (guarded by mu for the sampler)
	mu  sync.Mutex
	cur *rigWatchState

	// first-watch replay measurement (open -> process() start); -1 = not measured
	firstReplayWallMs float64
	firstReplayEvents int64

	// accounting (client goroutine writes; sampler reads via atomics)
	lastRV        uint64 // last delivered RV (atomic)
	maxRV         uint64
	baseRV        uint64 // RV the first watch was opened from
	nodeBase      int64  // node write count at first open (nodename kind)
	delivered     int64  // atomic
	replayRecv    int64  // events received with RV <= the open-time cache RV (ring/store replay)
	reconnects    int64  // atomic
	expired410    int64  // atomic
	relists       int64
	relistObjs    int64
	relistNanos   int64
	relistGap     int64 // RV distance skipped over by relists (recovered via LIST, not stream)
	terms         int64 // atomic: server-side stream closes without error
	termReplay    int64
	termLive      int64
	termAmbiguous int64
	errEvents     int64
	openCount     int64
	firstCloseMs  int64 // ms after run start when the client first OBSERVED a close, -1 none
	// latency samples (ms), measurement window only
	latMu sync.Mutex
	lat   []float64
	// state
	stallIdx int
}

// openWatch issues a Watch from rv and registers the internal watcher for
// occupancy sampling. Returns the interface plus whether it is an error/
// immediate-close watcher (no cacheWatcher inside).
func (c *rigClient) openWatch(ctx context.Context, rv uint64) (watch.Interface, error) {
	pred := storage.Everything
	switch c.group.Kind {
	case "nodename":
		pred = storage.SelectionPredicate{
			Label:       labels.Everything(),
			Field:       fields.OneTermEqualSelector("spec.nodeName", fmt.Sprintf("node-%d", c.node)),
			IndexFields: []string{"spec.nodeName"},
			GetAttrs: func(obj runtime.Object) (labels.Set, fields.Set, error) {
				pod, ok := obj.(*example.Pod)
				if !ok {
					return nil, nil, fmt.Errorf("not a pod")
				}
				return labels.Set(pod.Labels), fields.Set{
					"metadata.name":      pod.Name,
					"metadata.namespace": pod.Namespace,
					"spec.nodeName":      pod.Spec.NodeName,
				}, nil
			},
		}
	}
	sig := newRigSignal()
	wctx := utilflowcontrol.WithInitializationSignal(ctx, sig)
	ws := &rigWatchState{sig: sig, stamped: make(chan struct{})}
	c.r.cacher.watchCache.RLock()
	ws.replayBound = c.r.cacher.watchCache.resourceVersion
	c.r.cacher.watchCache.RUnlock()
	w, err := c.r.cacher.Watch(wctx, "/pods", storage.ListOptions{
		ResourceVersion: strconv.FormatUint(rv, 10),
		Predicate:       pred,
		Recursive:       true,
	})
	if err != nil {
		return nil, err
	}
	ws.openAt = time.Now().UnixNano()
	if cw, ok := w.(*cacheWatcher); ok {
		ws.cw = cw
		// stamp the server-side stop moment: cacheWatcher.done closes on every
		// stop path (force-close, interval invalidation, ctx done, client
		// Stop) strictly before close(c.result); combined with the signal
		// fire time (start of process()) this classifies a close as
		// during-replay vs live without any product seam. The state is
		// per-open, so a late stamp never leaks into a later watch.
		go func(ws *rigWatchState) {
			<-ws.cw.done
			atomic.StoreInt64(&ws.doneAt, time.Now().UnixNano())
			close(ws.stamped)
		}(ws)
	} else {
		close(ws.stamped) // no cacheWatcher (error watcher): nothing to stamp
	}
	c.mu.Lock()
	c.cur = ws
	c.mu.Unlock()
	atomic.AddInt64(&c.openCount, 1)
	return w, nil
}

// relist prices the 410 recovery: a full LIST from the cache.
func (c *rigClient) relist(ctx context.Context) (uint64, error) {
	list := &example.PodList{}
	start := time.Now()
	err := c.r.cacher.GetList(ctx, "/pods", storage.ListOptions{
		ResourceVersion: "0",
		Predicate:       storage.Everything,
		Recursive:       true,
	}, list)
	if err != nil {
		return 0, err
	}
	c.relists++
	c.relistObjs += int64(len(list.Items))
	c.relistNanos += time.Since(start).Nanoseconds()
	rv, err := storage.APIObjectVersioner{}.ParseResourceVersion(list.ResourceVersion)
	if err != nil {
		return 0, err
	}
	return rv, nil
}

func (c *rigClient) inStall(el time.Duration) (bool, time.Duration) {
	for _, s := range c.group.Stalls {
		if el >= s.At && el < s.At+s.Dur {
			return true, s.At + s.Dur - el
		}
	}
	return false, 0
}

func (c *rigClient) recordLatency(rv uint64, recvNanos int64) {
	// only for live delivery inside the measurement window
	wn, ok := c.r.writeNanosAt(rv)
	if !ok {
		return
	}
	ms := float64(recvNanos-wn) / 1e6
	c.latMu.Lock()
	c.lat = append(c.lat, ms)
	c.latMu.Unlock()
}

// run is the client goroutine: connect, consume with drain cap and stall
// schedule, classify closes, reconnect per policy, price 410s.
func (c *rigClient) run(ctx context.Context) {
	defer c.r.wg.Done()
	r := c.r
	// wait for StartAt
	if d := c.group.StartAt; d > 0 {
		select {
		case <-time.After(time.Until(r.t0.Add(d))):
		case <-ctx.Done():
			return
		}
	}
	// initial start RV
	cur := atomic.LoadUint64(&r.rvNext) - 1
	startRV := cur
	if c.group.StartGap > 0 {
		if uint64(c.group.StartGap) < cur-r.rvBase {
			startRV = cur - uint64(c.group.StartGap)
		} else {
			startRV = r.rvBase
		}
	}
	if c.group.StartExpired {
		startRV = 1 // below the initial list RV of 100 -> ResourceExpired
	}
	c.baseRV = startRV
	atomic.StoreUint64(&c.lastRV, startRV)
	if c.group.Kind == "nodename" {
		c.nodeBase = atomic.LoadInt64(&r.nodeWrites[c.node])
	}
	backoff := c.group.BackoffMin
	drainInterval := time.Duration(0)
	if c.group.Drain > 0 {
		drainInterval = time.Duration(float64(time.Second) / c.group.Drain)
	}
	var nextRead time.Time

	for {
		if ctx.Err() != nil {
			return
		}
		w, err := c.openWatch(ctx, atomic.LoadUint64(&c.lastRV))
		if err != nil {
			return
		}
		closedByServer := false
		errStream := false // stream ended because it carried an Error event (410 etc), not a force-close
	consume:
		for {
			// stall gate: do not read at all during a stall window
			if in, left := c.inStall(time.Since(r.t0)); in {
				select {
				case <-time.After(left):
				case <-ctx.Done():
					w.Stop()
					return
				}
			}
			// drain pacing
			if drainInterval > 0 {
				now := time.Now()
				if nextRead.After(now) {
					select {
					case <-time.After(nextRead.Sub(now)):
					case <-ctx.Done():
						w.Stop()
						return
					}
				}
				if nextRead.Before(time.Now().Add(-2 * drainInterval)) {
					nextRead = time.Now()
				}
				nextRead = nextRead.Add(drainInterval)
			}
			select {
			case ev, ok := <-w.ResultChan():
				if !ok {
					if ctx.Err() != nil {
						// run over: the product closes result on ctx.Done; not a termination
						return
					}
					closedByServer = true
					break consume
				}
				if ev.Type == watch.Error {
					if isExpired(ev) {
						atomic.AddInt64(&c.expired410, 1)
						newRV, lerr := c.relist(ctx)
						if lerr != nil {
							atomic.AddInt64(&c.errEvents, 1)
							w.Stop()
							return
						}
						gap := int64(newRV) - int64(atomic.LoadUint64(&c.lastRV))
						if gap > 0 {
							c.relistGap += gap
						}
						atomic.StoreUint64(&c.lastRV, newRV)
						c.maxRV = newRV
					} else {
						atomic.AddInt64(&c.errEvents, 1)
					}
					// the errWatcher stream is done; open a new watch (not a termination)
					closedByServer = true
					errStream = true
					break consume
				}
				if ev.Type == watch.Bookmark {
					continue
				}
				rv, perr := rigObjectRV(ev.Object)
				if perr != nil {
					continue
				}
				now := time.Now().UnixNano()
				c.mu.Lock()
				ws := c.cur
				c.mu.Unlock()
				live := ws == nil || rv > ws.replayBound
				if live && c.firstReplayWallMs < 0 && atomic.LoadInt64(&c.openCount) == 1 && ws != nil && ws.sig != nil {
					// first watch delivered its first live event: replay wall = open -> process() start,
					// valid only if process() started before any stop (a force-close during replay
					// also fires the signal, but only after done closes)
					fa := atomic.LoadInt64(&ws.sig.firedAt)
					da := atomic.LoadInt64(&ws.doneAt)
					if fa > ws.openAt && (da == 0 || fa < da) {
						c.firstReplayWallMs = float64(fa-ws.openAt) / 1e6
						c.firstReplayEvents = c.replayRecv
					}
				}
				if rv > c.maxRV {
					c.maxRV = rv
					atomic.AddInt64(&c.delivered, 1)
					if !live {
						c.replayRecv++
					}
				}
				atomic.StoreUint64(&c.lastRV, rv)
				if live && rv >= r.rvBase {
					c.recordLatency(rv, now)
				}
			case <-ctx.Done():
				w.Stop()
				return
			}
		}
		w.Stop()
		if closedByServer && !errStream {
			// Classify replaying-vs-live at the SERVER close moment. Note that
			// processInterval still calls process() (firing the init signal)
			// after a force-close during replay, so the naive "did the signal
			// fire" test is always true by the time the client sees the
			// close; compare timestamps instead: killed during replay iff the
			// signal fired after done closed (or never). done closes strictly
			// before close(result) on every product path, so waiting on the
			// stamp here cannot block for long (bounded anyway).
			c.mu.Lock()
			ws := c.cur
			c.mu.Unlock()
			atomic.AddInt64(&c.terms, 1)
			select {
			case <-ws.stamped:
			case <-time.After(2 * time.Second):
			}
			firedAt := int64(0)
			if ws.sig != nil {
				firedAt = atomic.LoadInt64(&ws.sig.firedAt)
			}
			doneAt := atomic.LoadInt64(&ws.doneAt)
			const ambiguity = int64(5 * time.Millisecond)
			switch {
			case doneAt == 0:
				// stamp missing (should not happen: done closes before result)
				c.termAmbiguous++
			case firedAt == 0 || firedAt >= doneAt:
				c.termReplay++
			case doneAt-firedAt < ambiguity:
				// killed within milliseconds of finishing replay: cannot tell
				c.termAmbiguous++
			default:
				c.termLive++
			}
			if atomic.LoadInt64(&c.firstCloseMs) < 0 {
				atomic.StoreInt64(&c.firstCloseMs, time.Since(r.t0).Milliseconds())
			}
		}
		if errStream {
			// re-list already priced; continue watching from the list RV
			continue
		}
		switch c.group.Reconnect {
		case "none":
			return
		case "backoff":
			atomic.AddInt64(&c.reconnects, 1)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
			backoff *= 2
			if c.group.BackoffMax > 0 && backoff > c.group.BackoffMax {
				backoff = c.group.BackoffMax
			}
		default: // immediate (client-go semantics)
			atomic.AddInt64(&c.reconnects, 1)
		}
	}
}

func isExpired(ev watch.Event) bool {
	if st, ok := ev.Object.(*metav1.Status); ok {
		return st.Code == 410 || st.Reason == metav1.StatusReasonExpired
	}
	return false
}

func rigObjectRV(obj runtime.Object) (uint64, error) {
	acc, err := apimeta.Accessor(obj)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(acc.GetResourceVersion(), 10, 64)
}

// ---------------------------------------------------------------------------
// telemetry

type rigMetrics struct {
	last     time.Time
	cpuUser  float64
	cpuTotal float64
	alloc    uint64
}

func readRuntime() (cpuUser, cpuTotal float64, totalAlloc uint64, goroutines int) {
	// process CPU via getrusage (runtime/metrics CPU classes are only refreshed
	// at GC boundaries, too coarse for 1s ticks)
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err == nil {
		cpuUser = float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6
		cpuTotal = cpuUser + float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
	}
	samples := []metrics.Sample{
		{Name: "/gc/heap/allocs:bytes"},
		{Name: "/sched/goroutines:goroutines"},
	}
	metrics.Read(samples)
	if samples[0].Value.Kind() == metrics.KindUint64 {
		totalAlloc = samples[0].Value.Uint64()
	}
	if samples[1].Value.Kind() == metrics.KindUint64 {
		goroutines = int(samples[1].Value.Uint64())
	}
	return
}

// fastPoll samples the terminated-watchers counter and incoming-channel
// depth at 20ms resolution: the counter series pins the exact moment the
// server force-closed (independent of when a stalled client notices),
// and the depth HWM catches sub-second dispatch backpressure.
func (r *rigRun) fastPoll(ctx context.Context) {
	defer r.wg.Done()
	last := rigTerminatedTotal()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			cur := rigTerminatedTotal()
			if cur > last {
				r.termMu.Lock()
				r.termSeries = append(r.termSeries, termSample{AtMs: now.Sub(r.t0).Milliseconds(), Total: cur - r.termBase})
				r.termMu.Unlock()
				if atomic.LoadInt64(&r.firstTermNanos) == 0 {
					atomic.StoreInt64(&r.firstTermNanos, now.UnixNano())
				}
				last = cur
			}
			if l := int64(len(r.cacher.incoming)); l > atomic.LoadInt64(&r.incomingHWM) {
				atomic.StoreInt64(&r.incomingHWM, l)
			}
		}
	}
}

type groupSample struct {
	inputMax, resultMax   int
	inputSum, resultSum   int
	lagMax                uint64
	lags                  []uint64
	delivered, reconnects int64
	expired, terms        int64
	relists               int64
	openW                 int
}

// tick emits one JSONL line per second with global and per-group telemetry.
func (r *rigRun) tick(ctx context.Context) {
	defer r.wg.Done()
	prevWrites := int64(0)
	prevDelivered := map[string]int64{}
	prevReconnects := map[string]int64{}
	prevCPUUser, prevCPUTotal, prevAlloc, _ := readRuntime()
	prevTerm := rigTerminatedTotal()
	prevT := time.Now()
	sec := 0
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			sec++
			dt := now.Sub(prevT).Seconds()
			prevT = now
			writes := atomic.LoadInt64(&r.writes)
			writerRV := atomic.LoadUint64(&r.rvNext) - 1
			wc := r.cacher.watchCache
			wc.RLock()
			cacheRV := wc.resourceVersion
			ringCap := wc.history.capacity
			ringSize := wc.history.endIndex - wc.history.startIndex
			wc.RUnlock()
			incomingLen := len(r.cacher.incoming)
			termNow := rigTerminatedTotal()
			cpuUser, cpuTotal, alloc, goroutines := readRuntime()
			stalls, deferred, rounds, catchupEvents := rigStallCounters()
			rss := rigRSSMB()
			var ms goruntime.MemStats
			goruntime.ReadMemStats(&ms)
			heapMB := float64(ms.HeapInuse) / (1 << 20)
			r.termMu.Lock()
			if rss > r.rssMaxMB {
				r.rssMaxMB = rss
			}
			if heapMB > r.heapMaxMB {
				r.heapMaxMB = heapMB
			}
			if goroutines > r.goroutinesMax {
				r.goroutinesMax = goroutines
			}
			cpuCores := (cpuTotal - prevCPUTotal) / dt
			r.cpuSamples = append(r.cpuSamples, cpuCores)
			// rigStalledGauge reports -1 (unavailable) since the gauge was
			// removed from the gated core; keep the max consistent with
			// the per-tick column instead of decaying to a spurious 0.
			if g := rigStalledGauge(); g > r.stalledGaugeMax || r.stalledGaugeMax == 0 {
				r.stalledGaugeMax = g
			}
			r.termMu.Unlock()
			line := map[string]any{
				"kind":              "tick",
				"scenario":          r.cell.Scenario,
				"cell":              r.cell.Name,
				"rep":               r.rep,
				"t":                 sec,
				"cache_rv":          cacheRV,
				"writer_rv":         writerRV,
				"cache_stale_evs":   int64(writerRV) - int64(cacheRV),
				"writes_ps":         float64(writes-prevWrites) / dt,
				"writes_total":      writes,
				"incoming_len":      incomingLen,
				"incoming_hwm":      atomic.LoadInt64(&r.incomingHWM),
				"incoming_hwm_klog": atomic.LoadInt64(&rigLog.incomingMax),
				"ring_capacity":     ringCap,
				"ring_size":         ringSize,
				"term_total":        termNow - r.termBase,
				"term_delta":        termNow - prevTerm,
				"forcing_lines":     atomic.LoadInt64(&rigLog.forcing),
				"invalid_lines":     atomic.LoadInt64(&rigLog.invalidated),
				"terminating_lines": atomic.LoadInt64(&rigLog.terminating),
				"stalls_total":      stalls - r.stallsBase,
				"deferred_total":    deferred - r.deferredBase,
				"catchup_rounds":    rounds - r.roundsBase,
				"catchup_events":    catchupEvents - r.catchupEventsBase,
				"stalled_gauge":     rigStalledGauge(),
				"cpu_user_cores":    (cpuUser - prevCPUUser) / dt,
				"cpu_total_cores":   cpuCores,
				"alloc_mb_ps":       float64(alloc-prevAlloc) / dt / (1 << 20),
				"rss_mb":            rss,
				"heap_inuse_mb":     heapMB,
				"goroutines":        goroutines,
			}
			for k, v := range r.cell.Params {
				line["p_"+k] = v
			}
			prevWrites, prevTerm = writes, termNow
			prevCPUUser, prevCPUTotal, prevAlloc = cpuUser, cpuTotal, alloc

			// per group
			for gi := range r.cell.Watchers {
				g := r.cell.Watchers[gi]
				var s groupSample
				for _, c := range r.clients {
					if c.group.Name != g.Name {
						continue
					}
					c.mu.Lock()
					var cw *cacheWatcher
					if c.cur != nil {
						cw = c.cur.cw
					}
					c.mu.Unlock()
					if cw != nil {
						il, rl := len(cw.input), len(cw.result)
						s.inputSum += il
						s.resultSum += rl
						if il > s.inputMax {
							s.inputMax = il
						}
						if rl > s.resultMax {
							s.resultMax = rl
						}
						s.openW++
					}
					lrv := atomic.LoadUint64(&c.lastRV)
					var lag uint64
					if writerRV > lrv {
						lag = writerRV - lrv
					}
					if lag > s.lagMax {
						s.lagMax = lag
					}
					s.lags = append(s.lags, lag)
					s.delivered += atomic.LoadInt64(&c.delivered)
					s.reconnects += atomic.LoadInt64(&c.reconnects)
					s.expired += atomic.LoadInt64(&c.expired410)
					s.terms += atomic.LoadInt64(&c.terms)
				}
				sort.Slice(s.lags, func(i, j int) bool { return s.lags[i] < s.lags[j] })
				p99lag := uint64(0)
				if n := len(s.lags); n > 0 {
					p99lag = s.lags[(n*99)/100]
				}
				key := "g:" + g.Name
				dd := s.delivered - prevDelivered[key]
				dr := s.reconnects - prevReconnects[key]
				prevDelivered[key] = s.delivered
				prevReconnects[key] = s.reconnects
				line[key+":input_max"] = s.inputMax
				line[key+":input_sum"] = s.inputSum
				line[key+":result_max"] = s.resultMax
				line[key+":lag_max"] = s.lagMax
				line[key+":lag_p99"] = p99lag
				line[key+":delivered_ps"] = float64(dd) / dt
				line[key+":reconnects_delta"] = dr
				line[key+":reconnects"] = s.reconnects
				line[key+":expired410"] = s.expired
				line[key+":terms"] = s.terms
				line[key+":open_watchers"] = s.openW
			}
			r.out.emit(line)
		}
	}
}

// ---------------------------------------------------------------------------
// cell execution

// rigCellResult carries the headline numbers of one cell run.
type rigCellResult struct {
	Scenario, Cell string
	Rep            int
	Params         map[string]float64
	DurationS      float64
	Writes         int64
	AchievedRate   float64
	TermCounter    float64 // apiserver_terminated_watchers_total delta
	ForcingLines   int64
	InvalidLines   int64
	IncomingHWM    int64
	FirstTermMsRel float64 // ms from stall/event of interest to first server-side termination (-1 if none)
	Groups         map[string]*rigGroupResult
	Extra          map[string]float64
}

type rigGroupResult struct {
	Watchers      int
	ChanSize      int
	Terms         int64
	TermReplay    int64
	TermLive      int64
	Reconnects    int64
	Expired410    int64
	Relists       int64
	RelistObjs    int64
	RelistMs      float64
	Delivered     int64
	Expected      int64
	Undelivered   int64 // end-of-run lag in events (not loss)
	Holes         int64 // proven in-stream loss (dense-RV watchers only)
	TermAmbiguous int64
	MaxLagAtEnd   uint64
	LatP50Ms      float64
	LatP99Ms      float64
	LatMaxMs      float64
	FirstTermMsMd float64 // median of per-client first-close time (ms after run start), -1 if none
	ReplayRecv    int64
	ReplayWallMs  float64 // first watch: open -> process() start (max over group)
	ReplayEvents  int64   // events replayed in that first watch
}

// percentile returns -1 for an empty sample (encoding/json cannot encode NaN, and
// an empty sample is meaningful: the group received nothing to measure).
func percentile(vals []float64, p float64) float64 {
	if len(vals) == 0 {
		return -1
	}
	s := append([]float64(nil), vals...)
	sort.Float64s(s)
	idx := int(math.Ceil(p/100*float64(len(s)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(s) {
		idx = len(s) - 1
	}
	return s[idx]
}

// runCell executes one cell rep end to end and returns its summary. It is
// self-contained: builds a fresh cacher, runs the schedule, tears down.
func runCell(cell rigCell, rep int, outdir string) (*rigCellResult, error) {
	rigInitKlog()
	cachermetrics.Register()
	rigEnsureStallResumeMetrics()
	if err := os.MkdirAll(outdir, 0o755); err != nil {
		return nil, err
	}
	tag := fmt.Sprintf("%s.rep%d", cell.Name, rep)
	lf, err := os.Create(filepath.Join(outdir, tag+".klog.txt"))
	if err != nil {
		return nil, err
	}
	rigLog.setFile(lf)
	out, err := newRigOut(filepath.Join(outdir, tag+".jsonl"))
	if err != nil {
		return nil, err
	}
	defer out.close()

	r := &rigRun{cell: cell, rep: rep, outdir: outdir, out: out}
	r.setupT0 = time.Now()
	if err := r.buildCacher(); err != nil {
		return nil, err
	}
	defer r.cacher.Stop()
	r.setupKeys()
	r.seedAndPregap()

	// chanSize probe: what a fresh watcher of each kind gets right now
	probeChanSize := func(kind string, node int) int {
		g := rigWatcherGroup{Kind: kind}
		c := &rigClient{r: r, group: g, node: node}
		w, err := c.openWatch(context.Background(), atomic.LoadUint64(&r.rvNext)-1)
		if err != nil {
			return -1
		}
		defer w.Stop()
		if cw, ok := w.(*cacheWatcher); ok {
			return cap(cw.result)
		}
		return -1
	}
	chanAll := probeChanSize("all", 0)
	chanNode := -1
	if cell.Cache.Indexed {
		chanNode = probeChanSize("nodename", 0)
	}

	wcap := r.cacher.watchCache.history.capacity
	meta := map[string]any{
		"kind":       "meta",
		"scenario":   cell.Scenario,
		"cell":       cell.Name,
		"rep":        rep,
		"gomaxprocs": goruntime.GOMAXPROCS(0),
		"gomaxprocs_warn": func() string {
			if goruntime.GOMAXPROCS(0) < 4 {
				return "low GOMAXPROCS: scheduler starvation of client goroutines can masquerade as force-closes"
			}
			return ""
		}(),
		"ncpu":          goruntime.NumCPU(),
		"go":            goruntime.Version(),
		"duration_s":    cell.Duration.Seconds(),
		"ring_capacity": wcap,
		"chan_all":      chanAll,
		"chan_nodename": chanNode,
		"seed":          cell.Cache.Seed,
		"pregap":        cell.Writers.PreGap,
		"rate":          cell.Writers.Rate,
		"obj_bytes":     r.objBytes,
		"predict":       cell.Predict,
		"start_wall":    time.Now().Format(time.RFC3339),
		"stall_resume":  r.cacher.stall != nil,
	}
	for k, v := range cell.Params {
		meta["p_"+k] = v
	}
	out.emit(meta)

	r.termBase = rigTerminatedTotal()
	if math.IsNaN(r.termBase) {
		r.termBase = 0
	}
	initBase := rigInitEventsTotal()
	r.stallsBase, r.deferredBase, r.roundsBase, r.catchupEventsBase = rigStallCounters()
	r.termExpiredBase = rigTermReason("resource_expired")
	r.termExpiredInitialBase = rigTermReason("resource_expired_initial")
	r.termUnresponsiveBase = rigTermReason("unresponsive")

	ctx, cancel := context.WithTimeout(context.Background(), cell.Duration)
	r.ctx, r.cancel = ctx, cancel
	r.t0 = time.Now()

	// clients
	for _, g := range cell.Watchers {
		for i := 0; i < g.Count; i++ {
			c := &rigClient{
				r:                 r,
				group:             g,
				gidx:              i,
				name:              fmt.Sprintf("%s-%d", g.Name, i),
				node:              (g.NodeBase + i) % max(1, cell.Writers.Nodes),
				firstCloseMs:      -1,
				firstReplayWallMs: -1,
			}
			r.clients = append(r.clients, c)
		}
	}
	stopWriter := make(chan struct{})
	r.wg.Add(1)
	go r.runWriter(stopWriter)
	for _, c := range r.clients {
		r.wg.Add(1)
		go c.run(ctx)
	}
	r.wg.Add(2)
	go r.fastPoll(ctx)
	go r.tick(ctx)

	<-ctx.Done()
	close(stopWriter)
	r.runEnd = time.Now()
	r.wg.Wait()
	cancel()

	// summary
	dur := r.runEnd.Sub(r.t0).Seconds()
	writes := atomic.LoadInt64(&r.writes)
	finalRV := atomic.LoadUint64(&r.rvNext) - 1
	activeS := (cell.Duration - cell.Writers.StopBeforeEnd).Seconds()
	if activeS <= 0 || activeS > dur {
		activeS = dur
	}
	res := &rigCellResult{
		Scenario:     cell.Scenario,
		Cell:         cell.Name,
		Rep:          rep,
		Params:       cell.Params,
		DurationS:    dur,
		Writes:       writes,
		AchievedRate: float64(writes-int64(cell.Cache.Seed)-int64(cell.Writers.PreGap)) / activeS,
		TermCounter:  rigTerminatedTotal() - r.termBase,
		ForcingLines: atomic.LoadInt64(&rigLog.forcing),
		InvalidLines: atomic.LoadInt64(&rigLog.invalidated),
		IncomingHWM:  atomic.LoadInt64(&r.incomingHWM),
		Groups:       map[string]*rigGroupResult{},
		Extra:        map[string]float64{},
	}
	res.Extra["init_events"] = rigInitEventsTotal() - initBase
	res.Extra["ring_capacity"] = float64(wcap)
	res.Extra["chan_all"] = float64(chanAll)
	res.Extra["chan_nodename"] = float64(chanNode)
	res.Extra["final_rv"] = float64(finalRV)
	// stall/resume + termination-reason + process-cost telemetry
	stalls, deferred, rounds, catchupEvents := rigStallCounters()
	res.Extra["stalls_total"] = stalls - r.stallsBase
	res.Extra["deferred_total"] = deferred - r.deferredBase
	res.Extra["catchup_rounds"] = rounds - r.roundsBase
	res.Extra["catchup_events"] = catchupEvents - r.catchupEventsBase
	res.Extra["term_expired"] = rigTermReason("resource_expired") - r.termExpiredBase
	res.Extra["term_expired_initial"] = rigTermReason("resource_expired_initial") - r.termExpiredInitialBase
	res.Extra["term_unresponsive"] = rigTermReason("unresponsive") - r.termUnresponsiveBase
	res.Extra["terminating_lines"] = float64(atomic.LoadInt64(&rigLog.terminating))
	res.Extra["incoming_hwm_klog"] = float64(atomic.LoadInt64(&rigLog.incomingMax))
	r.termMu.Lock()
	res.Extra["rss_mb_max"] = r.rssMaxMB
	res.Extra["heap_inuse_mb_max"] = r.heapMaxMB
	res.Extra["stalled_gauge_max"] = r.stalledGaugeMax
	res.Extra["goroutines_max"] = float64(r.goroutinesMax)
	res.Extra["cpu_total_cores_avg"] = -1
	if len(r.cpuSamples) > 0 {
		sum := 0.0
		for _, c := range r.cpuSamples {
			sum += c
		}
		res.Extra["cpu_total_cores_avg"] = sum / float64(len(r.cpuSamples))
	}
	r.termMu.Unlock()
	res.Extra["stall_resume_gate"] = 0
	if r.cacher.stall != nil {
		res.Extra["stall_resume_gate"] = 1
	}
	if ft := atomic.LoadInt64(&r.firstTermNanos); ft > 0 {
		res.FirstTermMsRel = float64(ft-r.t0.UnixNano()) / 1e6
	} else {
		res.FirstTermMsRel = -1
	}

	// per group summaries + per watcher lines
	for _, g := range cell.Watchers {
		gr := &rigGroupResult{Watchers: g.Count, FirstTermMsMd: -1}
		var firstTerms []float64
		var lats []float64
		var lagMax uint64
		for _, c := range r.clients {
			if c.group.Name != g.Name {
				continue
			}
			delivered := atomic.LoadInt64(&c.delivered)
			// expected: events matching this client between its first open and end
			var expected int64
			if g.Kind == "nodename" {
				expected = atomic.LoadInt64(&r.nodeWrites[c.node]) - c.nodeBase
			} else {
				expected = int64(finalRV) - int64(c.baseRV)
			}
			// undelivered_end (formerly "missed"): matching events not yet
			// delivered when the run ended = end-of-run LAG for a healthy or
			// diverging client, not proven loss. True loss shows up as
			// "holes": for an all-kind (dense-RV) watcher, RVs at or below the
			// client's high-water mark that were never delivered and were not
			// legitimately skipped by a re-LIST.
			undelivered := expected - c.relistGap - delivered
			lastRV := atomic.LoadUint64(&c.lastRV)
			var lag uint64
			if finalRV > lastRV {
				lag = finalRV - lastRV
			}
			if lag > lagMax {
				lagMax = lag
			}
			holes := int64(-1) // n/a for nodename (sparse RVs per watcher)
			if g.Kind != "nodename" {
				holes = (int64(lastRV) - int64(c.baseRV)) - c.relistGap - delivered
			}
			c.latMu.Lock()
			lats = append(lats, c.lat...)
			latP99 := percentile(c.lat, 99)
			c.latMu.Unlock()
			ft := float64(atomic.LoadInt64(&c.firstCloseMs))
			if ft >= 0 {
				firstTerms = append(firstTerms, ft)
			}
			chanSize := 0
			c.mu.Lock()
			if c.cur != nil && c.cur.cw != nil {
				chanSize = cap(c.cur.cw.result)
			}
			c.mu.Unlock()
			if chanSize > 0 {
				gr.ChanSize = chanSize
			}
			gr.Terms += atomic.LoadInt64(&c.terms)
			gr.TermReplay += c.termReplay
			gr.TermLive += c.termLive
			gr.Reconnects += atomic.LoadInt64(&c.reconnects)
			gr.Expired410 += atomic.LoadInt64(&c.expired410)
			gr.Relists += c.relists
			gr.RelistObjs += c.relistObjs
			gr.RelistMs += float64(c.relistNanos) / 1e6
			gr.Delivered += delivered
			gr.Expected += expected
			gr.Undelivered += undelivered
			if holes > 0 {
				gr.Holes += holes
			}
			gr.TermAmbiguous += c.termAmbiguous
			gr.ReplayRecv += c.replayRecv
			out.emit(map[string]any{
				"kind":                  "watcher",
				"scenario":              cell.Scenario,
				"cell":                  cell.Name,
				"rep":                   rep,
				"group":                 g.Name,
				"idx":                   c.gidx,
				"node":                  c.node,
				"chan_size":             chanSize,
				"base_rv":               c.baseRV,
				"last_rv":               lastRV,
				"delivered":             delivered,
				"replay_recv":           c.replayRecv,
				"expected":              expected,
				"undelivered_end":       undelivered,
				"holes":                 holes,
				"lag_end":               lag,
				"terms":                 atomic.LoadInt64(&c.terms),
				"terms_replay":          c.termReplay,
				"terms_live":            c.termLive,
				"terms_ambiguous":       c.termAmbiguous,
				"reconnects":            atomic.LoadInt64(&c.reconnects),
				"expired410":            atomic.LoadInt64(&c.expired410),
				"relists":               c.relists,
				"relist_objs":           c.relistObjs,
				"relist_ms":             float64(c.relistNanos) / 1e6,
				"relist_gap":            c.relistGap,
				"opens":                 atomic.LoadInt64(&c.openCount),
				"err_events":            atomic.LoadInt64(&c.errEvents),
				"client_first_close_ms": ft,
				"lat_p99_ms":            latP99,
				"first_replay_ms":       c.firstReplayWallMs,
				"first_replay_evs":      c.firstReplayEvents,
			})
			if c.firstReplayWallMs > gr.ReplayWallMs {
				gr.ReplayWallMs = c.firstReplayWallMs
				gr.ReplayEvents = c.firstReplayEvents
			}
		}
		gr.MaxLagAtEnd = lagMax
		gr.LatP50Ms = percentile(lats, 50)
		gr.LatP99Ms = percentile(lats, 99)
		gr.LatMaxMs = percentile(lats, 100)
		if len(firstTerms) > 0 {
			gr.FirstTermMsMd = percentile(firstTerms, 50)
		}
		res.Groups[g.Name] = gr
	}
	// termination time series
	r.termMu.Lock()
	ts := append([]termSample(nil), r.termSeries...)
	r.termMu.Unlock()
	out.emit(map[string]any{"kind": "term_series", "scenario": cell.Scenario, "cell": cell.Name, "rep": rep, "samples": ts})

	summary := map[string]any{
		"kind":               "cell",
		"scenario":           res.Scenario,
		"cell":               res.Cell,
		"rep":                res.Rep,
		"duration_s":         res.DurationS,
		"writes":             res.Writes,
		"achieved_rate":      res.AchievedRate,
		"target_rate":        cell.Writers.Rate,
		"term_counter_delta": res.TermCounter,
		"forcing_lines":      res.ForcingLines,
		"invalid_lines":      res.InvalidLines,
		"incoming_hwm":       res.IncomingHWM,
		"first_term_ms":      res.FirstTermMsRel,
		"init_events":        res.Extra["init_events"],
		"ring_capacity":      wcap,
		"chan_all":           chanAll,
		"chan_nodename":      chanNode,
		"final_rv":           finalRV,
	}
	for k, v := range cell.Params {
		summary["p_"+k] = v
	}
	for k, v := range res.Extra {
		if _, exists := summary[k]; !exists {
			summary[k] = v
		}
	}
	for name, g := range res.Groups {
		summary["g:"+name+":watchers"] = g.Watchers
		summary["g:"+name+":chan_size"] = g.ChanSize
		summary["g:"+name+":terms"] = g.Terms
		summary["g:"+name+":terms_replay"] = g.TermReplay
		summary["g:"+name+":terms_live"] = g.TermLive
		summary["g:"+name+":terms_ambiguous"] = g.TermAmbiguous
		summary["g:"+name+":reconnects"] = g.Reconnects
		summary["g:"+name+":expired410"] = g.Expired410
		summary["g:"+name+":relists"] = g.Relists
		summary["g:"+name+":relist_objs"] = g.RelistObjs
		summary["g:"+name+":relist_ms"] = g.RelistMs
		summary["g:"+name+":delivered"] = g.Delivered
		summary["g:"+name+":expected"] = g.Expected
		summary["g:"+name+":undelivered_end"] = g.Undelivered
		summary["g:"+name+":holes"] = g.Holes
		summary["g:"+name+":replay_recv"] = g.ReplayRecv
		summary["g:"+name+":lag_end"] = g.MaxLagAtEnd
		summary["g:"+name+":lat_p50_ms"] = g.LatP50Ms
		summary["g:"+name+":lat_p99_ms"] = g.LatP99Ms
		summary["g:"+name+":lat_max_ms"] = g.LatMaxMs
		summary["g:"+name+":client_first_close_ms_md"] = g.FirstTermMsMd
		summary["g:"+name+":replay_wall_ms"] = g.ReplayWallMs
		summary["g:"+name+":replay_events"] = g.ReplayEvents
	}
	out.emit(summary)
	appendTSV(filepath.Join(outdir, "summary.tsv"), summary)
	rigLog.setFile(nil)
	if out.encErr != nil {
		return res, out.encErr
	}
	return res, nil
}

// appendTSV appends a row (creating a header on first use) so tables can be
// assembled without re-parsing JSONL. Column order is fixed per outdir by
// the first row's key order (sorted).
func appendTSV(path string, row map[string]any) {
	keys := make([]string, 0, len(row))
	for k := range row {
		if k == "kind" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	// stable core columns first
	core := []string{"scenario", "cell", "rep"}
	rest := []string{}
	for _, k := range keys {
		isCore := false
		for _, c := range core {
			if k == c {
				isCore = true
			}
		}
		if !isCore {
			rest = append(rest, k)
		}
	}
	cols := append(core, rest...)
	var b strings.Builder
	if _, err := os.Stat(path); os.IsNotExist(err) {
		b.WriteString(strings.Join(cols, "\t"))
		b.WriteString("\n")
	}
	for i, k := range cols {
		if i > 0 {
			b.WriteString("\t")
		}
		v, ok := row[k]
		if !ok {
			b.WriteString("")
			continue
		}
		switch x := v.(type) {
		case float64:
			b.WriteString(strconv.FormatFloat(x, 'f', -1, 64))
		default:
			b.WriteString(fmt.Sprint(x))
		}
	}
	b.WriteString("\n")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	f.WriteString(b.String())
}
