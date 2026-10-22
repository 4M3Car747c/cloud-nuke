package aws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gruntwork-io/cloud-nuke/config"
	"github.com/gruntwork-io/cloud-nuke/resource"
)

// fakeStoppable is an in-memory StoppableResource that logs every call.
type fakeStoppable struct {
	name     string
	region   string
	ids      []string
	capture  map[string]error // per-id capture error
	stopErr  map[string]error
	startErr map[string]error
	log      *callLog
}

type callLog struct {
	mu    sync.Mutex
	calls []string
}

func (l *callLog) add(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, s)
}

func (l *callLog) get() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.calls)
}

func (f *fakeStoppable) ResourceName() string { return f.name }
func (f *fakeStoppable) Stoppable() bool      { return true }
func (f *fakeStoppable) ListStoppable(context.Context, config.Config) ([]string, error) {
	return f.ids, nil
}
func (f *fakeStoppable) CaptureState(_ context.Context, id string) (json.RawMessage, error) {
	if err := f.capture[id]; err != nil {
		return nil, err
	}
	return json.RawMessage(fmt.Sprintf(`{"id":%q}`, id)), nil
}
func (f *fakeStoppable) StopResource(_ context.Context, id string, _ json.RawMessage) error {
	f.log.add(fmt.Sprintf("stop %s %s %s", f.region, f.name, id))
	return f.stopErr[id]
}
func (f *fakeStoppable) StartResource(_ context.Context, id string, _ json.RawMessage) error {
	f.log.add(fmt.Sprintf("start %s %s %s", f.region, f.name, id))
	return f.startErr[id]
}

// fakeFactory returns, per region, fakes for the listed types (in the given order).
func fakeFactory(log *callLog, perRegion map[string][]*fakeStoppable) stoppableFactory {
	return func(_ *Query, region string, types []string) ([]resource.StoppableResource, error) {
		var out []resource.StoppableResource
		for _, f := range perRegion[region] {
			f.region, f.log = region, log
			if IsNukeable(f.name, types) {
				out = append(out, f)
			}
		}
		return out, nil
	}
}

func byStatus(recs []StopRecord) map[string]StopStatus {
	m := map[string]StopStatus{}
	for _, r := range recs {
		m[r.ResourceType+"/"+r.ID] = r.Status
	}
	return m
}

func TestStopOrderCoversStoppableTypes(t *testing.T) {
	// Every type with stop hooks must have an explicit place in StopOrder.
	for _, name := range StoppableResourceTypes() {
		assert.Contains(t, StopOrder, name)
	}
	// And the order puts Auto Scaling groups before the instances they manage.
	assert.Less(t, slices.Index(StopOrder, "asg"), slices.Index(StopOrder, "ec2"))
}

func TestStopAllResources_OrderAndStatuses(t *testing.T) {
	log := &callLog{}
	factory := fakeFactory(log, map[string][]*fakeStoppable{
		"us-east-1": {
			{name: "asg", ids: []string{"g1"}},
			{name: "ec2", ids: []string{"i-run", "i-off", "i-store", "i-err", "i-spot"},
				capture: map[string]error{
					"i-off":   resource.ErrAlreadyStopped,
					"i-store": resource.Unsupported("instance-store root volume"),
					"i-err":   errors.New("boom"),
				},
				stopErr: map[string]error{"i-spot": resource.Unsupported("one-time spot")},
			},
		},
		GlobalRegion: {{name: "cloudfront-distribution", ids: []string{"E1"}}},
	})

	var mu sync.Mutex
	var seen []StopRecord
	onRecord := func(r StopRecord) error {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, r)
		return nil
	}
	q := &Query{Regions: []string{GlobalRegion, "us-east-1"}}
	recs, err := stopAll(context.Background(), q, config.Config{}, onRecord, nil, factory)
	require.NoError(t, err)

	assert.Equal(t, map[string]StopStatus{
		"asg/g1":                     StopStatusStopped,
		"ec2/i-run":                  StopStatusStopped,
		"ec2/i-off":                  StopStatusSkipped,
		"ec2/i-store":                StopStatusUnsupported,
		"ec2/i-err":                  StopStatusFailed,
		"ec2/i-spot":                 StopStatusUnsupported,
		"cloudfront-distribution/E1": StopStatusStopped,
	}, byStatus(recs))

	calls := log.get()
	// ASG before any EC2 instance; global (CloudFront) after the regions.
	asg := slices.Index(calls, "stop us-east-1 asg g1")
	require.GreaterOrEqual(t, asg, 0)
	for i, c := range calls {
		if c != "stop us-east-1 asg g1" && c != "stop global cloudfront-distribution E1" {
			assert.Greater(t, i, asg, c)
		}
	}
	assert.Equal(t, "stop global cloudfront-distribution E1", calls[len(calls)-1])

	// Every stopped resource was reported as stopping (with its state) before the stop call.
	for _, id := range []string{"i-run", "i-spot", "g1", "E1"} {
		idx := slices.IndexFunc(seen, func(r StopRecord) bool { return r.ID == id && r.Status == StopStatusStopping })
		require.GreaterOrEqual(t, idx, 0, id)
		assert.JSONEq(t, fmt.Sprintf(`{"id":%q}`, id), string(seen[idx].Prev))
	}
	// Failed stops keep the captured state so a caller can still try to start them.
	for _, r := range recs {
		if r.ID == "i-spot" {
			assert.NotEmpty(t, r.Prev)
		}
	}
}

func TestStopAllResources_SaveFailureSkipsStop(t *testing.T) {
	log := &callLog{}
	factory := fakeFactory(log, map[string][]*fakeStoppable{
		"us-east-1": {{name: "ec2", ids: []string{"i-1"}}},
	})
	onRecord := func(r StopRecord) error {
		if r.Status == StopStatusStopping {
			return errors.New("db down")
		}
		return nil
	}
	recs, err := stopAll(context.Background(), &Query{Regions: []string{"us-east-1"}}, config.Config{}, onRecord, nil, factory)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, StopStatusFailed, recs[0].Status)
	assert.Contains(t, recs[0].Error, "db down")
	assert.Empty(t, log.get(), "must not stop a resource whose state was not saved")
}

func TestStopAllResources_ResourceTypesFilter(t *testing.T) {
	log := &callLog{}
	factory := fakeFactory(log, map[string][]*fakeStoppable{
		"us-east-1": {{name: "asg", ids: []string{"g1"}}, {name: "ec2", ids: []string{"i-1"}}},
	})
	q := &Query{Regions: []string{"us-east-1"}, ResourceTypes: []string{"ec2"}}
	recs, err := stopAll(context.Background(), q, config.Config{}, nil, nil, factory)
	require.NoError(t, err)
	assert.Equal(t, map[string]StopStatus{"ec2/i-1": StopStatusStopped}, byStatus(recs))
}

func TestStartResources_ReverseOrderAndStatuses(t *testing.T) {
	log := &callLog{}
	factory := fakeFactory(log, map[string][]*fakeStoppable{
		"us-east-1": {
			{name: "asg"},
			{name: "ec2", startErr: map[string]error{"i-gone": resource.ErrGone, "i-bad": errors.New("capacity")}},
		},
		GlobalRegion: {{name: "cloudfront-distribution"}},
	})
	prev := json.RawMessage(`{}`)
	records := []StopRecord{
		{ResourceType: "asg", Region: "us-east-1", ID: "g1", Prev: prev, Status: StopStatusStopped},
		{ResourceType: "ec2", Region: "us-east-1", ID: "i-1", Prev: prev, Status: StopStatusStopped},
		{ResourceType: "ec2", Region: "us-east-1", ID: "i-gone", Prev: prev, Status: StopStatusStopped},
		{ResourceType: "ec2", Region: "us-east-1", ID: "i-bad", Prev: prev, Status: StopStatusStopped},
		{ResourceType: "ec2", Region: "us-east-1", ID: "i-noprev", Status: StopStatusStopped},
		{ResourceType: "lambda", Region: "us-east-1", ID: "fn", Prev: prev, Status: StopStatusStopped},
		{ResourceType: "cloudfront-distribution", Region: GlobalRegion, ID: "E1", Prev: prev, Status: StopStatusStopped},
	}
	recs, err := startAll(context.Background(), &Query{}, records, nil, nil, factory)
	require.NoError(t, err)
	assert.Equal(t, map[string]StopStatus{
		"asg/g1":                     StopStatusRestored,
		"ec2/i-1":                    StopStatusRestored,
		"ec2/i-gone":                 StopStatusGone,
		"ec2/i-bad":                  StopStatusFailed,
		"ec2/i-noprev":               StopStatusFailed,
		"lambda/fn":                  StopStatusFailed, // no such stoppable type in the fake region
		"cloudfront-distribution/E1": StopStatusRestored,
	}, byStatus(recs))

	calls := log.get()
	assert.Equal(t, "start global cloudfront-distribution E1", calls[0], "global first on start")
	assert.Equal(t, "start us-east-1 asg g1", calls[len(calls)-1], "ASG resumed after its instances")
}
