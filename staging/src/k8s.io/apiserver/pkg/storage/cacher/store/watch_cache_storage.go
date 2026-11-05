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

package store

import (
	"fmt"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/cache"
)

func NewWatchCacheStorage(indexers *cache.Indexers) *WatchCacheStorage {
	storage := &WatchCacheStorage{
		store:     newBtreeStore(btreeDegree),
		indexer:   newIndexer(ElementIndexers(indexers)),
		snapshots: newSnapshotter(utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot)),
	}
	storage.latestSnapshot.Store(storage.store.Clone())
	return storage
}

type WatchCacheStorage struct {
	// latestSnapshot is an immutable clone of store, republished under lock
	// after every write, so reads of the latest state don't need lock.
	latestSnapshot atomic.Pointer[btreeStore]

	// Access to store, indexer, and snapshots is synchronized using lock.
	lock      sync.RWMutex
	store     btreeStore
	indexer   indexer
	snapshots snapshotter
}

func (w *WatchCacheStorage) SnapshottingEnabled() bool {
	w.lock.RLock()
	defer w.lock.RUnlock()
	return w.snapshots.Enabled()
}

func (w *WatchCacheStorage) CanServeExactRV(rv uint64) bool {
	oldest, ok := w.oldestExactRV()
	return ok && rv >= oldest
}

func (w *WatchCacheStorage) oldestExactRV() (uint64, bool) {
	w.lock.RLock()
	defer w.lock.RUnlock()
	return w.snapshots.OldestResourceVersion()
}

func (w *WatchCacheStorage) Compact(rev uint64) {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.snapshots.Compact(rev)
}

func (w *WatchCacheStorage) MarkConsistent(consistent bool) {
	if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
		w.lock.Lock()
		defer w.lock.Unlock()
		w.snapshots.SetEnabled(consistent)
	}
}

func (w *WatchCacheStorage) LatestSnapshot() Snapshot {
	return w.latestSnapshot.Load()
}

// UpdateStore executes a mutation (Add, Update, Delete) on the underlying store.
// It returns the element that was previously stored under the same key, if any.
func (w *WatchCacheStorage) UpdateStore(eventType watch.EventType, elem *Element, resourceVersion uint64) (prev *Element, err error) {
	if elem == nil {
		return nil, fmt.Errorf("elem cannot be nil")
	}
	w.lock.Lock()
	defer w.lock.Unlock()
	switch eventType {
	case watch.Added, watch.Modified:
		prev = w.store.addOrUpdateElem(elem, resourceVersion)
		err = w.indexer.updateElem(elem.Key, prev, elem, resourceVersion)
	case watch.Deleted:
		prev = w.store.deleteElem(elem, resourceVersion)
		err = w.indexer.updateElem(elem.Key, prev, nil, resourceVersion)
	default:
		err = fmt.Errorf("unexpected event type: %v", eventType)
	}
	if err != nil {
		return nil, err
	}
	latest := w.store.Clone()
	w.latestSnapshot.Store(latest)
	w.snapshots.Add(latest)
	return prev, nil
}

func (w *WatchCacheStorage) UpdateResourceVersion(resourceVersion uint64) {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.store.resourceVersion = resourceVersion
	w.indexer.resourceVersion = resourceVersion
	w.snapshots.UpdateResourceVersion(resourceVersion)
	w.latestSnapshot.Store(w.store.Clone())
}

// CompactSnapshotsLocked prunes snapshots older than the oldest history version.
func (w *WatchCacheStorage) CompactSnapshotsLocked(oldestRV uint64) {
	w.Compact(oldestRV)
}

// Replace replaces the elements in the underlying store and resets snapshots.
func (w *WatchCacheStorage) Replace(toReplace []*Element, version uint64) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.store.Replace(toReplace, version)
	if err := w.indexer.Replace(toReplace, version); err != nil {
		return err
	}
	latest := w.store.Clone()
	w.latestSnapshot.Store(latest)
	w.snapshots.Replace(latest)
	return nil
}

// GetExactSnapshotLocked retrieves a snapshot less than or equal to the given resource version.
func (w *WatchCacheStorage) GetExactSnapshotLocked(resourceVersion uint64) (Snapshot, error) {
	w.lock.RLock()
	defer w.lock.RUnlock()
	snap, err := w.snapshots.GetSnapshot(resourceVersion)
	if err != nil {
		return nil, err
	}
	return snap, nil
}

// GetByIndexSnapshot retrieves elements by index and wraps them in a Snapshot.
func (w *WatchCacheStorage) GetByIndexSnapshot(indexName, value string) (Snapshot, error) {
	// Cloning requires exclusive access, but takes O(1).
	w.lock.Lock()
	defer w.lock.Unlock()
	snap, err := w.indexer.CloneByIndex(indexName, value)
	if err != nil {
		return nil, err
	}
	return snap, nil
}
