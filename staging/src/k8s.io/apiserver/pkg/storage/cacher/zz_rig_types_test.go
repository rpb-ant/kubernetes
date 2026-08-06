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

// Package-internal white-box experimentation rig for the watch cache. It is
// compiled only with `-tags watchrig` and driven as a test binary:
//
//	go test -tags watchrig -c -o cacher.rig ./pkg/storage/cacher
//	./cacher.rig -test.run TestRig -rig.scenario=<name> -rig.out=<dir>
//
// A scenario is a named entry in rigScenarios that expands into cells (one
// parameter point each). Each cell run produces JSONL telemetry plus one
// summary line and one TSV row. Adding a scenario means adding a table
// entry; the engine in zz_rig_engine_test.go does not need to change.
package cacher

import "time"

// rigCacheConfig describes how the Cacher under test is built.
type rigCacheConfig struct {
	// Indexed builds the cacher with the pods-style spec.nodeName trigger
	// index, which changes the watcher channel-size caps (1000 for
	// non-triggered watchers, 10 for nodeName-triggered ones) versus the
	// unindexed cap of 100.
	Indexed bool
	// FreshDuration is Config.EventsHistoryWindow (>= 75s).
	FreshDuration time.Duration
	// PinCapacity pins the ring buffer capacity (disables dynamic resize) so
	// that chanSize and ring depth are deterministic. 0 = leave dynamic.
	PinCapacity int
	// Seed pre-creates this many distinct pods before any watcher starts.
	Seed int
}

// rigBurst overlays extra writes on the steady rate.
type rigBurst struct {
	At    time.Duration
	Count int
	Rate  float64
}

// rigWriterConfig drives the injected write load (the rig stands in for the
// reflector: it calls watchCache.Add/Update directly).
type rigWriterConfig struct {
	// Rate is the steady-state write rate, events/s (open-loop paced).
	Rate float64
	// PadBytes pads each pod's payload (annotation) to control object size.
	PadBytes int
	// Bursts overlay {At, Count, Rate} on the steady rate.
	Bursts []rigBurst
	// Keys is the number of distinct pod names cycled by updates
	// (0 = Cache.Seed, or 1000 if that is also 0).
	Keys int
	// Nodes spreads pods over this many distinct spec.nodeName values.
	Nodes int
	// HotNodes, if >0, restricts steady-state updates to pods on nodes
	// [0, HotNodes) - concentrated churn.
	HotNodes int
	// PreGap events are written as fast as possible during setup, after
	// Seed and before the run clock starts, to fuel replay-debt scenarios.
	PreGap int
	// StopBeforeEnd stops the writer this long before the run ends so
	// healthy watchers can drain and missed-event accounting is exact.
	StopBeforeEnd time.Duration
}

// rigStall is a window during which a watcher's consumer does not read.
type rigStall struct{ At, Dur time.Duration }

// rigWatcherGroup is a population of identical watch clients.
type rigWatcherGroup struct {
	Name  string
	Count int
	// Kind: "all" (recursive namespace-wide watch, no selector) or
	// "nodename" (spec.nodeName=<node> field selector using the trigger
	// index; requires an Indexed cache).
	Kind string
	// NodeBase: nodename watcher i watches node (NodeBase+i) mod Nodes.
	NodeBase int
	// Drain caps the consumer read rate, events/s (0 = unbounded).
	Drain float64
	// Stalls: windows during which the consumer does not read at all.
	Stalls []rigStall
	// StartGap opens the first watch this many events behind the current
	// cache RV (replay debt).
	StartGap int
	// StartExpired opens the first watch from an RV below the ring's
	// oldest event, forcing the ResourceExpired (410) path.
	StartExpired bool
	// Reconnect policy after the server closes the stream:
	// "immediate" (client-go semantics: re-Watch from the last delivered
	// RV with zero delay), "backoff" (BackoffMin doubling to BackoffMax),
	// or "none".
	Reconnect  string
	BackoffMin time.Duration
	BackoffMax time.Duration
	// StartAt delays opening the watch relative to the run clock.
	StartAt time.Duration
}

// rigCell is one fully-specified experimental cell.
type rigCell struct {
	Scenario string
	Name     string
	Duration time.Duration
	Cache    rigCacheConfig
	Writers  rigWriterConfig
	Watchers []rigWatcherGroup
	// Params echoes the cell's headline parameters into every JSONL line
	// and the summary TSV, so tables can be built without parsing Name.
	Params map[string]float64
	// Predict is the one-line prediction copied from the experiment DESIGN.
	Predict string
}

// rigScenario is a named family of cells.
type rigScenario struct {
	Name        string
	Description string
	Cells       func() []rigCell
}

// rigScenarios is the scenario registry; entries live in
// zz_rig_scenarios_test.go.
var rigScenarios = map[string]rigScenario{}

func rigRegister(s rigScenario) { rigScenarios[s.Name] = s }
