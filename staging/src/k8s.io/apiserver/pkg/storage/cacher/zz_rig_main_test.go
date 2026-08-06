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
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
)

var (
	rigScenarioFlag    = flag.String("rig.scenario", "", "rig scenario name (see -rig.list)")
	rigOutFlag         = flag.String("rig.out", "", "rig output directory (JSONL + summary.tsv + klog)")
	rigRepsFlag        = flag.Int("rig.reps", 3, "repetitions per cell")
	rigCellsFlag       = flag.String("rig.cells", "", "regexp filter on cell names")
	rigListFlag        = flag.Bool("rig.list", false, "list scenarios and cells, then exit")
	rigDurFlag         = flag.Duration("rig.duration", 0, "override cell duration (0 = scenario default)")
	rigStallResumeFlag = flag.String("rig.stallresume", "", "if set (\"on\"/\"off\"), force the WatchCacheStallResume feature gate for the run before any cacher is built")
)

// TestRig is the entry point for the white-box watch-cache rig. It is inert
// unless -rig.scenario (or -rig.list) is given, so a plain `go test -tags
// watchrig` run of the package does nothing extra.
func TestRig(t *testing.T) {
	if *rigListFlag {
		names := make([]string, 0, len(rigScenarios))
		for n := range rigScenarios {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			s := rigScenarios[n]
			cells := s.Cells()
			fmt.Printf("%-24s %s (%d cells)\n", s.Name, s.Description, len(cells))
			for _, c := range cells {
				fmt.Printf("    %s  dur=%v\n", c.Name, c.Duration)
			}
		}
		return
	}
	name := *rigScenarioFlag
	if name == "" {
		t.Skip("no -rig.scenario given; rig is inert")
	}
	// The gate is read once per Cacher at construction, so set it before any
	// cell builds one; "" leaves the compiled-in default.
	switch *rigStallResumeFlag {
	case "on":
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.WatchCacheStallResume, true)
	case "off":
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.WatchCacheStallResume, false)
	case "":
	default:
		t.Fatalf("-rig.stallresume must be \"on\", \"off\" or empty, got %q", *rigStallResumeFlag)
	}
	t.Logf("WatchCacheStallResume=%v", utilfeature.DefaultFeatureGate.Enabled(features.WatchCacheStallResume))
	scen, ok := rigScenarios[name]
	if !ok {
		t.Fatalf("unknown scenario %q; use -rig.list", name)
	}
	outdir := *rigOutFlag
	if outdir == "" {
		outdir = t.TempDir()
	}
	if err := os.MkdirAll(outdir, 0o755); err != nil {
		t.Fatal(err)
	}
	var filter *regexp.Regexp
	if *rigCellsFlag != "" {
		filter = regexp.MustCompile(*rigCellsFlag)
	}
	cells := scen.Cells()
	t.Logf("scenario %s: %d cells, %d reps, out=%s", scen.Name, len(cells), *rigRepsFlag, outdir)

	var results []*rigCellResult
	for _, cell := range cells {
		if filter != nil && !filter.MatchString(cell.Name) {
			continue
		}
		if *rigDurFlag > 0 {
			cell.Duration = *rigDurFlag
		}
		for rep := 0; rep < *rigRepsFlag; rep++ {
			start := time.Now()
			res, err := runCell(cell, rep, outdir)
			if err != nil {
				t.Fatalf("cell %s rep %d: %v", cell.Name, rep, err)
			}
			results = append(results, res)
			t.Logf("cell %s rep %d done in %v: writes=%d rate=%.0f/s terms(counter)=%.0f forcing=%d incomingHWM=%d %s",
				cell.Name, rep, time.Since(start).Round(time.Millisecond),
				res.Writes, res.AchievedRate, res.TermCounter, res.ForcingLines, res.IncomingHWM, groupSummary(res))
		}
	}

	// Positive control assertion: the self-test scenario MUST observe a
	// termination or the rig's "no terminations" results are worthless.
	if strings.HasPrefix(scen.Name, "S0") {
		for _, res := range results {
			if res.TermCounter < 1 {
				t.Errorf("S0 positive control FAILED: terminated_watchers_total delta=%v (want >=1) in %s rep %d", res.TermCounter, res.Cell, res.Rep)
			}
			if res.ForcingLines < 1 {
				t.Errorf("S0 positive control FAILED: no 'Forcing ... watcher close' log line in %s rep %d", res.Cell, res.Rep)
			}
			g := res.Groups["stalled"]
			if g == nil || g.Terms < 1 {
				t.Errorf("S0 positive control FAILED: rig did not observe the result-chan close in %s rep %d", res.Cell, res.Rep)
			}
			if g == nil || g.Reconnects < 1 {
				t.Errorf("S0 positive control FAILED: no reconnect issued in %s rep %d", res.Cell, res.Rep)
			}
		}
	}
}

func groupSummary(res *rigCellResult) string {
	names := make([]string, 0, len(res.Groups))
	for n := range res.Groups {
		names = append(names, n)
	}
	sort.Strings(names)
	parts := []string{}
	for _, n := range names {
		g := res.Groups[n]
		parts = append(parts, fmt.Sprintf("[%s ch=%d terms=%d(replay=%d,live=%d) reconn=%d 410=%d relist=%d deliv=%d undeliv=%d holes=%d lag=%d p99=%.1fms]",
			n, g.ChanSize, g.Terms, g.TermReplay, g.TermLive, g.Reconnects, g.Expired410, g.Relists, g.Delivered, g.Undelivered, g.Holes, g.MaxLagAtEnd, g.LatP99Ms))
	}
	return strings.Join(parts, " ")
}
