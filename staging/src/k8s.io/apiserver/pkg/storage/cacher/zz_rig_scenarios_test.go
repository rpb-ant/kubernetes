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
	"fmt"
	"time"
)

// Ring/channel arithmetic used by the scenarios (see MENTAL-MODEL.md §1):
//
//	chanSize = clamp(ceil(ringCapacity/freshSeconds), 10, cap)
//	cap = 1000 for indexed watchers without the nodeName trigger,
//	      10   for nodeName-triggered watchers, 100 for unindexed types.
//
// With capacity pinned to 102400 and fresh=75s: ceil(102400/75)=1366 -> 1000
// (indexed, no trigger) / 100 (unindexed) / 10 (triggered).
// With capacity pinned to 50000: ceil(50000/75)=667 (the cr15 rig figure).

const (
	rigCapDeep = 102400 // deepest capacity the default policy can reach
	rigCapCR15 = 50000  // reproduces the cr15/nami rig's chanSize=667
)

func init() {
	rigRegister(rigScenario{
		Name:        "S0-selftest",
		Description: "positive control: a watcher that stops reading must be force-closed and the rig must see it",
		Cells:       s0Cells,
	})
	rigRegister(rigScenario{
		Name:        "S1-hiccup-sweep",
		Description: "1 live watcher, write rate W x stall duration d: termination boundary vs 2*chanSize/W",
		Cells:       s1Cells,
	})
	rigRegister(rigScenario{
		Name:        "S2-burst-sweep",
		Description: "20 fast watchers, steady 50/s + one burst of B events at 5000/s: terminations, max lag",
		Cells:       s2Cells,
	})
	rigRegister(rigScenario{
		Name:        "S3-replay-loop",
		Description: "reconnecting watcher with replay debt G, drain R, under W: the ejection loop",
		Cells:       s3Cells,
	})
	rigRegister(rigScenario{
		Name:        "S4-slow-watcher-tax",
		Description: "K permanently-slow watchers among 200: dispatcher grace-budget tax on incoming depth/latency",
		Cells:       s4Cells,
	})
	rigRegister(rigScenario{
		Name:        "S5-nodename-narrow",
		Description: "500 nodeName-triggered (chanSize 10) watchers: spread vs concentrated churn, hot-node hiccups",
		Cells:       s5Cells,
	})
	rigRegister(rigScenario{
		Name:        "S8-zombie-herd",
		Description: "M watchers that never read (wedged clients) plus 20 live controls under W=850/s: the cost of keeping zombies in the fan-out",
		Cells:       s8Cells,
	})
}

// ---------------------------------------------------------------------------
// S0: positive control

func s0Cells() []rigCell {
	return []rigCell{{
		Scenario: "S0-selftest",
		Name:     "S0-selftest",
		Duration: 12 * time.Second,
		Cache:    rigCacheConfig{Indexed: false, PinCapacity: 20000, Seed: 1000},
		Writers:  rigWriterConfig{Rate: 1000, Keys: 1000, Nodes: 1, StopBeforeEnd: 2 * time.Second},
		Watchers: []rigWatcherGroup{
			{Name: "stalled", Count: 1, Kind: "all", Reconnect: "immediate",
				Stalls: []rigStall{{At: 1 * time.Second, Dur: 6 * time.Second}}},
			{Name: "healthy", Count: 1, Kind: "all", Reconnect: "immediate"},
		},
		Params:  map[string]float64{"W": 1000, "stall_s": 6, "stall_at_s": 1},
		Predict: "unindexed chanSize=100: stalled watcher force-closed ~2*100/1000=0.2s after stall start; healthy watcher never",
	}}
}

// ---------------------------------------------------------------------------
// S1: hiccup boundary

func s1Cells() []rigCell {
	var cells []rigCell
	rates := []float64{50, 200, 850, 3000}
	stalls := []float64{0.5, 1, 2, 4, 8}
	for _, w := range rates {
		for _, st := range stalls {
			// chanSize=1000 (indexed, no trigger, capacity pinned deep)
			tolerated := (2*1000.0+1)/w + 0.1
			verdict := "survives"
			if st > tolerated {
				verdict = "TERMINATED"
			}
			dur := time.Duration((3 + st + 6) * float64(time.Second))
			cells = append(cells, rigCell{
				Scenario: "S1-hiccup-sweep",
				Name:     fmt.Sprintf("S1-W%g-stall%g", w, st),
				Duration: dur,
				Cache:    rigCacheConfig{Indexed: true, PinCapacity: rigCapDeep, Seed: 5000},
				Writers:  rigWriterConfig{Rate: w, Keys: 5000, Nodes: 10, StopBeforeEnd: 3 * time.Second},
				Watchers: []rigWatcherGroup{
					{Name: "hiccup", Count: 1, Kind: "all", Reconnect: "immediate",
						Stalls: []rigStall{{At: 3 * time.Second, Dur: time.Duration(st * float64(time.Second))}}},
					{Name: "healthy", Count: 1, Kind: "all", Reconnect: "immediate"},
				},
				Params:  map[string]float64{"W": w, "stall_s": st, "tolerated_s": tolerated, "stall_at_s": 3},
				Predict: fmt.Sprintf("chanSize=1000: tolerated stall (2*1000+1)/%g+0.1 = %.2fs => %s", w, tolerated, verdict),
			})
		}
	}
	// A-vs-A noise cell: the boundary-adjacent point re-run under a distinct name.
	cells = append(cells, func() rigCell {
		c := cells[12] // W=850, stall=2
		c.Name = "S1-W850-stall2-avsa"
		c.Params = map[string]float64{"W": 850, "stall_s": 2, "tolerated_s": c.Params["tolerated_s"], "stall_at_s": 3}
		return c
	}())
	return cells
}

// ---------------------------------------------------------------------------
// S2: burst absorption

func s2Cells() []rigCell {
	mk := func(name string, b int, slow int) rigCell {
		groups := []rigWatcherGroup{
			{Name: "fast", Count: 20, Kind: "all", Reconnect: "immediate"},
		}
		if slow > 0 {
			groups = append(groups, rigWatcherGroup{Name: "slow100", Count: slow, Kind: "all", Drain: 100, Reconnect: "immediate"})
		}
		return rigCell{
			Scenario: "S2-burst-sweep",
			Name:     name,
			Duration: 20 * time.Second,
			Cache:    rigCacheConfig{Indexed: true, PinCapacity: rigCapDeep, Seed: 5000},
			Writers: rigWriterConfig{Rate: 50, Keys: 5000, Nodes: 10, StopBeforeEnd: 4 * time.Second,
				Bursts: []rigBurst{{At: 5 * time.Second, Count: b, Rate: 5000}}},
			Watchers: groups,
			Params:   map[string]float64{"W": 50, "burst": float64(b), "burst_rate": 5000, "slow": float64(slow)},
			Predict:  "fast in-process consumers (drain >> 5000/s): 0 terminations, max lag small; drain=100/s watchers under a burst > 2000 events die ~2000/(5000-100)=0.41s into the burst",
		}
	}
	return []rigCell{
		mk("S2-B500", 500, 0),
		mk("S2-B2000", 2000, 0),
		mk("S2-B10000", 10000, 0),
		mk("S2-B2000-avsa", 2000, 0),
		mk("S2-B10000-mixed", 10000, 5),
	}
}

// ---------------------------------------------------------------------------
// S3: replay-loop (the r16 mechanism)

func s3Cells() []rigCell {
	var cells []rigCell
	type pt struct {
		w, r, g float64
	}
	var pts []pt
	for _, r := range []float64{0, 2000, 500, 200} {
		for _, g := range []float64{1000, 10000, 50000} {
			pts = append(pts, pt{850, r, g})
		}
	}
	for _, r := range []float64{500, 200} {
		for _, g := range []float64{10000, 50000} {
			pts = append(pts, pt{400, r, g})
		}
	}
	// gap close to the ring depth (102400): the loop should reach the 410
	// (ResourceExpired) exit and price a full re-LIST within the run.
	for _, r := range []float64{500, 200} {
		pts = append(pts, pt{850, r, 90000})
	}
	for _, p := range pts {
		rname := fmt.Sprintf("%g", p.r)
		if p.r == 0 {
			rname = "inf"
		}
		// chanSize = 1000 (capacity pinned to 102400, indexed, no trigger).
		// First replay attempt survives iff G < 1000 + R*(1000/W); the loop
		// converges iff R > W.
		attempt := 1000.0 / p.w
		converge := "diverges (R<W): reconnect loop, gap grows ~(W-R)/s"
		if p.r == 0 || p.r > p.w {
			converge = "converges (R>W) with reconnect churn"
		}
		cells = append(cells, rigCell{
			Scenario: "S3-replay-loop",
			Name:     fmt.Sprintf("S3-W%g-R%s-G%g", p.w, rname, p.g),
			Duration: 45 * time.Second,
			Cache:    rigCacheConfig{Indexed: true, PinCapacity: rigCapDeep, Seed: 5000},
			Writers:  rigWriterConfig{Rate: p.w, Keys: 5000, Nodes: 10, PreGap: int(p.g), StopBeforeEnd: 4 * time.Second},
			Watchers: []rigWatcherGroup{
				{Name: "replayer", Count: 1, Kind: "all", Drain: p.r, StartGap: int(p.g), Reconnect: "immediate"},
				{Name: "healthy", Count: 1, Kind: "all", Reconnect: "immediate"},
			},
			Params:  map[string]float64{"W": p.w, "R": p.r, "G": p.g, "chan": 1000, "attempt_s": attempt},
			Predict: fmt.Sprintf("first replay dies iff G > 1000+R*%.2f; %s", attempt, converge),
		})
	}
	return cells
}

// ---------------------------------------------------------------------------
// S4: slow-watcher dispatcher tax

func s4Cells() []rigCell {
	mk := func(k int, w float64) rigCell {
		groups := []rigWatcherGroup{
			{Name: "healthy", Count: 200 - k, Kind: "all", Reconnect: "immediate"},
		}
		for i := 0; i < k; i++ {
			// stagger the slow watchers across the first 40s so their kills
			// (each ~2000/(W-10) after start) recur through the run
			groups = append(groups, rigWatcherGroup{
				Name:      fmt.Sprintf("slow%02d", i),
				Count:     1,
				Kind:      "all",
				Drain:     10,
				Reconnect: "immediate",
				StartAt:   time.Duration(float64(i) / float64(max(1, k)) * 40 * float64(time.Second)),
			})
		}
		return rigCell{
			Scenario: "S4-slow-watcher-tax",
			Name:     fmt.Sprintf("S4-K%d-W%g", k, w),
			Duration: 50 * time.Second,
			Cache:    rigCacheConfig{Indexed: true, PinCapacity: rigCapDeep, Seed: 5000},
			Writers:  rigWriterConfig{Rate: w, Keys: 5000, Nodes: 10, StopBeforeEnd: 4 * time.Second},
			Watchers: groups,
			Params:   map[string]float64{"K": float64(k), "W": w},
			Predict:  "each slow watcher blocks the single dispatcher for at most the shared budget (100ms cap, 50ms/s refill) once per kill: incoming HWM ~ W*0.1, healthy p99 latency +<=100ms transient, achieved write rate ~W",
		}
	}
	return []rigCell{
		mk(0, 200), mk(1, 200), mk(5, 200), mk(20, 200), mk(20, 2000),
	}
}

// ---------------------------------------------------------------------------
// S5: nodeName-narrow watchers (kubelet case)

func s5Cells() []rigCell {
	const nodes, perNode = 500, 20
	base := func(name string, hot int, stall float64) rigCell {
		groups := []rigWatcherGroup{
			{Name: "nodew", Count: nodes, Kind: "nodename", NodeBase: 0, Reconnect: "immediate"},
		}
		if stall > 0 {
			// watchers on the (hot) nodes 0..4 hiccup for `stall` seconds at t=5s
			groups = []rigWatcherGroup{
				{Name: "hotstall", Count: 5, Kind: "nodename", NodeBase: 0, Reconnect: "immediate",
					Stalls: []rigStall{{At: 5 * time.Second, Dur: time.Duration(stall * float64(time.Second))}}},
				{Name: "nodew", Count: nodes - 5, Kind: "nodename", NodeBase: 5, Reconnect: "immediate"},
			}
		}
		perHot := 200.0
		if hot > 0 {
			perHot = 200.0 / float64(hot)
		} else {
			perHot = 200.0 / nodes
		}
		tol := (2*10.0 + 1) / perHot
		return rigCell{
			Scenario: "S5-nodename-narrow",
			Name:     name,
			Duration: 18 * time.Second,
			Cache:    rigCacheConfig{Indexed: true, PinCapacity: rigCapDeep, Seed: nodes * perNode},
			Writers:  rigWriterConfig{Rate: 200, Keys: nodes * perNode, Nodes: nodes, HotNodes: hot, StopBeforeEnd: 3 * time.Second},
			Watchers: groups,
			Params:   map[string]float64{"W": 200, "hot_nodes": float64(hot), "per_node_rate": perHot, "stall_s": stall, "tolerated_s": tol, "stall_at_s": 5},
			Predict:  fmt.Sprintf("chanSize=10: a nodeName watcher tolerates ~21 events of stall = %.2fs at its node's rate %.1f/s; drain-unbounded watchers never terminate", tol, perHot),
		}
	}
	return []rigCell{
		base("S5-spread", 0, 0),
		base("S5-hot5", 5, 0),
		base("S5-hot5-stall0.25", 5, 0.25),
		base("S5-hot5-stall0.5", 5, 0.5),
		base("S5-hot5-stall1", 5, 1),
		base("S5-hot5-stall2", 5, 2),
		base("S5-spread-stall2", 0, 2),
	}
}

// ---------------------------------------------------------------------------
// S8: zombie herd (the wedged-but-connected client population)

func s8Cells() []rigCell {
	mk := func(m int) rigCell {
		return rigCell{
			Scenario: "S8-zombie-herd",
			Name:     fmt.Sprintf("S8-M%d", m),
			Duration: 30 * time.Second,
			Cache:    rigCacheConfig{Indexed: true, PinCapacity: rigCapDeep, Seed: 5000},
			Writers:  rigWriterConfig{Rate: 850, Keys: 5000, Nodes: 10, StopBeforeEnd: 3 * time.Second},
			Watchers: []rigWatcherGroup{
				// zombies open a watch and never read a single event, and never
				// reconnect (a wedged process that still holds its connection)
				{Name: "zombie", Count: m, Kind: "all", Reconnect: "none",
					Stalls: []rigStall{{At: 0, Dur: 60 * time.Second}}},
				{Name: "control", Count: 20, Kind: "all", Reconnect: "immediate"},
			},
			Params:  map[string]float64{"W": 850, "M_zombie": float64(m), "controls": 20},
			Predict: "gate off: each zombie is force-closed once (~2*1000/850=2.4s in) and then costs nothing; gate on: zombies stay in the fan-out and each costs the dispatcher a failed add + poke per event, control p99 unaffected, RSS grows with the herd's pinned buffers",
		}
	}
	return []rigCell{mk(100), mk(1000), mk(5000)}
}
