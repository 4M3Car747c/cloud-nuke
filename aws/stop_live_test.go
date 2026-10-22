package aws

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gruntwork-io/cloud-nuke/config"
	"github.com/gruntwork-io/cloud-nuke/resource"
)

// TestStopStartLive stops and restarts real resources, then checks each one is back in the
// state it started in. It only touches resources carrying the STOP_LIVE_TAG tag.
//
//	STOP_LIVE=1 STOP_LIVE_TAG=mbx-stop-test=1 STOP_LIVE_REGIONS=us-east-1,global \
//	STOP_LIVE_TYPES=asg,ec2 AWS_PROFILE=... go test ./aws -run TestStopStartLive -v -timeout 2h
//
// STOP_LIVE_WAIT (default 20m) bounds how long to wait for a resource to report itself
// stopped, and then restored, after the stop/start calls return. STOP_LIVE_HOLD (default 0)
// keeps everything stopped that long before starting, e.g. to let Auto Scaling health checks run.
func TestStopStartLive(t *testing.T) {
	if os.Getenv("STOP_LIVE") != "1" {
		t.Skip("set STOP_LIVE=1, STOP_LIVE_TAG=key=value and STOP_LIVE_REGIONS (and AWS credentials)")
	}
	key, value, ok := strings.Cut(os.Getenv("STOP_LIVE_TAG"), "=")
	require.True(t, ok && key != "", "STOP_LIVE_TAG must be key=value; it keeps the test away from other resources")
	regions := splitEnv("STOP_LIVE_REGIONS")
	require.NotEmpty(t, regions)
	types := splitEnv("STOP_LIVE_TYPES")
	wait := 20 * time.Minute
	if d, err := time.ParseDuration(os.Getenv("STOP_LIVE_WAIT")); err == nil {
		wait = d
	}

	ctx := context.Background()
	query := &Query{
		Regions:       regions,
		ResourceTypes: types,
		IncludeTags:   map[string]config.Expression{key: {RE: *regexp.MustCompile("^" + regexp.QuoteMeta(value) + "$")}},
		// The account is ours; make sure the tag override path is exercised too.
		IgnoreProtectionTags: true,
	}
	cfg := config.Config{}
	cfg.AddIncludeTags(query.IncludeTags)
	cfg.IgnoreProtectionTags()

	type key3 struct{ typ, region, id string }
	resourcesByKey := map[key3]resource.StoppableResource{}

	// 1. Baseline: what each running resource looks like before the stop.
	baseline := map[key3]json.RawMessage{}
	for _, region := range regions {
		res, err := initStoppable(query, region, types)
		require.NoError(t, err)
		for _, r := range res {
			ids, err := r.ListStoppable(ctx, cfg)
			require.NoError(t, err, r.ResourceName())
			for _, id := range ids {
				k := key3{r.ResourceName(), region, id}
				resourcesByKey[k] = r
				prev, err := r.CaptureState(ctx, id)
				switch {
				case errors.Is(err, resource.ErrAlreadyStopped):
					t.Logf("baseline %v: already stopped", k)
				case resource.IsUnsupported(err):
					t.Logf("baseline %v: unsupported: %v", k, err)
				default:
					require.NoError(t, err, k)
					baseline[k] = prev
					t.Logf("baseline %v: %s", k, prev)
				}
			}
		}
	}
	require.NotEmpty(t, baseline, "no running resources tagged %s=%s", key, value)

	// 2. Stop.
	start := time.Now()
	recs, err := StopAllResources(ctx, query, config.Config{}, nil, nil)
	require.NoError(t, err)
	t.Logf("stop took %s", time.Since(start).Round(time.Second))
	var stopped []StopRecord
	for _, rec := range recs {
		t.Logf("stop %s %s %s: %s %s", rec.ResourceType, rec.Region, rec.ID, rec.Status, rec.Error)
		require.NotEqual(t, StopStatusFailed, rec.Status, rec.Error)
		if rec.Status == StopStatusStopped {
			stopped = append(stopped, rec)
		}
	}
	for k := range baseline {
		found := false
		for _, rec := range stopped {
			found = found || (key3{rec.ResourceType, rec.Region, rec.ID} == k)
		}
		require.True(t, found, "%v was running but not stopped", k)
	}

	// 3. Each stopped resource must now report nothing left to stop.
	for _, rec := range stopped {
		k := key3{rec.ResourceType, rec.Region, rec.ID}
		eventually(t, wait, k, func() (bool, string) {
			prev, err := resourcesByKey[k].CaptureState(ctx, rec.ID)
			if errors.Is(err, resource.ErrAlreadyStopped) {
				return true, ""
			}
			return false, string(prev) + errString(err)
		})
	}

	if hold, err := time.ParseDuration(os.Getenv("STOP_LIVE_HOLD")); err == nil && hold > 0 {
		t.Logf("holding stopped for %s", hold)
		time.Sleep(hold)
	}

	// 4. Start, then each resource must look exactly like its baseline again.
	start = time.Now()
	recs, err = StartResources(ctx, query, stopped, nil, nil)
	require.NoError(t, err)
	t.Logf("start took %s", time.Since(start).Round(time.Second))
	for _, rec := range recs {
		t.Logf("start %s %s %s: %s %s", rec.ResourceType, rec.Region, rec.ID, rec.Status, rec.Error)
		require.Equal(t, StopStatusRestored, rec.Status, rec.Error)
	}
	for k, want := range baseline {
		eventually(t, wait, k, func() (bool, string) {
			got, err := resourcesByKey[k].CaptureState(ctx, k.id)
			if err != nil {
				return false, err.Error()
			}
			return jsonEqual(want, got), string(got)
		})
	}
}

func eventually(t *testing.T, limit time.Duration, what any, check func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for {
		ok, detail := check()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%v: still not in the expected state after %s: %s", what, limit, detail)
		}
		time.Sleep(15 * time.Second)
	}
}

func jsonEqual(a, b json.RawMessage) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	xa, _ := json.Marshal(x)
	ya, _ := json.Marshal(y)
	return string(xa) == string(ya)
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return " " + err.Error()
}

func splitEnv(name string) []string {
	var out []string
	for _, s := range strings.Split(os.Getenv(name), ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}
