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
	goruntime "runtime"
	"strconv"
	"testing"
	"time"
	"weak"

	"github.com/stretchr/testify/require"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/apis/example"
	"k8s.io/apiserver/pkg/storage"
	cachertesting "k8s.io/apiserver/pkg/storage/cacher/testing"
	testingclock "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
)

// A watch-list (watch with sendInitialEvents=true) streams its initial events
// from a snapshot of the whole store, at the pace the client reads. Until the
// last of them is sent, the watcher keeps that snapshot reachable, and with it
// every object as of the watch's resourceVersion. Only the end of the request
// bounds how long that lasts. The history window and compaction do not. Being
// closed as unresponsive does only if it happens before the watcher's bookmark
// is queued, about a second in.
func TestWatchListPinsSnapshotPastCompaction(t *testing.T) {
	forceRequestWatchProgressSupport(t)

	ctx := context.Background()
	clock := testingclock.NewFakeClock(time.Now())
	cacher, _, err := newTestCacherWithoutSyncing(&cachertesting.MockStorage{}, clock)
	require.NoError(t, err)
	defer cacher.Stop()
	require.NoError(t, cacher.Wait(ctx))

	const numPods = 1000
	rv := uint64(1000)
	// rewriteAllPods stores a new object for every pod. The weak pointers
	// let the test ask the garbage collector which of them are still reachable.
	rewriteAllPods := func() []weak.Pointer[example.Pod] {
		pods := make([]weak.Pointer[example.Pod], numPods)
		for i := range pods {
			rv++
			pod := &example.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: fmt.Sprintf("pod-%04d", i), ResourceVersion: strconv.FormatUint(rv, 10)}}
			pods[i] = weak.Make(pod)
			require.NoError(t, cacher.watchCache.Update(pod))
		}
		require.Eventually(t, func() bool { return len(cacher.incoming) == 0 }, wait.ForeverTestTimeout, time.Millisecond)
		return pods
	}
	reachable := func(pods []weak.Pointer[example.Pod]) (n int) {
		goruntime.GC()
		for _, pod := range pods {
			if pod.Value() != nil {
				n++
			}
		}
		return n
	}

	initialPods := rewriteAllPods()
	watchRV := strconv.FormatUint(rv, 10)

	// A cluster-wide watch-list served from the watch cache. The client reads
	// one event and then stops reading.
	pred := storage.Everything
	pred.AllowWatchBookmarks = true
	w, err := cacher.Watch(ctx, "/pods/", storage.ListOptions{
		ResourceVersion:      "0",
		ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
		SendInitialEvents:    ptr.To(true),
		Recursive:            true,
		Predicate:            pred,
	})
	require.NoError(t, err)
	<-w.ResultChan()

	// The dispatcher's one-second bookmark tick queues the bookmark this
	// watcher waits for. From then on, closing it as unresponsive is
	// graceful: it is dropped from dispatch but keeps streaming.
	require.Eventually(t, func() bool {
		clock.Step(2 * time.Second)
		return w.(*cacheWatcher).wasBookmarkAfterRvReceived()
	}, wait.ForeverTestTimeout, 10*time.Millisecond)

	// Half an hour passes. Every pod is rewritten three times, the watch's
	// resourceVersion leaves the history window, and etcd compacts.
	for range 3 {
		clock.Step(10 * time.Minute)
		rewriteAllPods()
	}
	require.NoError(t, cacher.Compact(strconv.FormatUint(rv, 10)))

	err = cacher.GetList(ctx, "/pods/", storage.ListOptions{
		ResourceVersion:      watchRV,
		ResourceVersionMatch: metav1.ResourceVersionMatchExact,
		Recursive:            true,
		Predicate:            storage.Everything,
	}, &example.PodList{})
	require.True(t, apierrors.IsResourceExpired(err), "LIST at the watch's resourceVersion: %v", err)
	t.Logf("LIST at resourceVersion=%s: %v", watchRV, err)

	pinned := reachable(initialPods)
	t.Logf("pods as of resourceVersion=%s still reachable while the watch-list is open: %d of %d", watchRV, pinned, numPods)
	require.Equal(t, numPods, pinned)

	// What the watch handler does when the request times out or the client
	// disconnects. Draining waits for the sending goroutine to exit.
	w.Stop()
	for range w.ResultChan() {
	}
	released := reachable(initialPods)
	t.Logf("after the watch-list is stopped: %d of %d", released, numPods)
	require.Zero(t, released)
}
