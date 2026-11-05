/*
Copyright 2022 The Kubernetes Authors.

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

	"k8s.io/client-go/tools/cache"
)

// newIndexer returns a indexer similar to storeIndex from client-go/tools/cache.
// TODO: Unify the indexer code with client-go/cache package.
// Major differences is type of values stored and their mutability:
// * Indexer in client-go stores object keys, that are not mutable.
// * Indexer in cacher stores whole objects, which is mutable.
// Indexer in client-go uses keys as it is used in conjunction with map[key]value
// allowing for fast value retrieval, while btree used in cacher would provide additional overhead.
// Difference in mutability of stored values is used for optimizing some operations in client-go Indexer.
func newIndexer(indexers cache.Indexers) indexer {
	return indexer{
		indices:  map[string]map[string]*btreeStore{},
		indexers: indexers,
	}
}

type indexer struct {
	indices         map[string]map[string]*btreeStore
	indexers        cache.Indexers
	resourceVersion uint64
}

// CloneByIndex should not be called concurrently, same as btreeStore.Clone.
func (i *indexer) CloneByIndex(indexName, indexValue string) (*btreeStore, error) {
	if i.indexers[indexName] == nil {
		return nil, fmt.Errorf("index with name %s does not exist", indexName)
	}
	bucket := i.indices[indexName][indexValue]
	if bucket == nil {
		empty := newBtreeStore(btreeDegree)
		bucket = &empty
	}
	bucket.resourceVersion = i.resourceVersion
	return bucket.Clone(), nil
}

func (i *indexer) Replace(objs []*Element, resourceVersion uint64) error {
	i.indices = map[string]map[string]*btreeStore{}
	i.resourceVersion = resourceVersion
	for _, storeElem := range objs {
		err := i.updateElem(storeElem.Key, nil, storeElem, resourceVersion)
		if err != nil {
			return err
		}
	}
	return nil
}

func (i *indexer) updateElem(key string, oldObj, newObj *Element, resourceVersion uint64) (err error) {
	i.resourceVersion = resourceVersion
	var oldIndexValues, indexValues []string
	for name, indexFunc := range i.indexers {
		if oldObj != nil {
			oldIndexValues, err = indexFunc(oldObj)
		} else {
			oldIndexValues = oldIndexValues[:0]
		}
		if err != nil {
			return fmt.Errorf("unable to calculate an index entry for key %q on index %q: %w", key, name, err)
		}
		if newObj != nil {
			indexValues, err = indexFunc(newObj)
		} else {
			indexValues = indexValues[:0]
		}
		if err != nil {
			return fmt.Errorf("unable to calculate an index entry for key %q on index %q: %w", key, name, err)
		}
		index := i.indices[name]
		if index == nil {
			index = map[string]*btreeStore{}
			i.indices[name] = index
		}
		if len(indexValues) == 1 && len(oldIndexValues) == 1 && indexValues[0] == oldIndexValues[0] {
			// We optimize for the most common case where indexFunc returns a single value which has not been changed
			i.add(indexValues[0], newObj, index)
			continue
		}
		for _, value := range oldIndexValues {
			i.delete(value, oldObj, index)
		}
		for _, value := range indexValues {
			i.add(value, newObj, index)
		}
	}
	return nil
}

func (i *indexer) add(value string, obj *Element, index map[string]*btreeStore) {
	bucket := index[value]
	if bucket == nil {
		store := newBtreeStore(btreeDegree)
		bucket = &store
		index[value] = bucket
	}
	bucket.addOrUpdateElem(obj, i.resourceVersion)
}

func (i *indexer) delete(value string, obj *Element, index map[string]*btreeStore) {
	bucket := index[value]
	if bucket == nil {
		return
	}
	bucket.deleteElem(obj, i.resourceVersion)
	// If we don's delete the bucket when zero, indices with high cardinality
	// short lived resources can cause memory to increase over time from
	// unused empty buckets. See `kubernetes/kubernetes/issues/84959`.
	if bucket.tree.Len() == 0 {
		delete(index, value)
	}
}
