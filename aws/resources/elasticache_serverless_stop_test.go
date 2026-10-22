package resources

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/gruntwork-io/cloud-nuke/config"
	"github.com/gruntwork-io/cloud-nuke/resource"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stopServerlessMock keeps serverless caches and applies ModifyServerlessCache like AWS does:
// a 0 removes a limit, a minimum above its maximum is rejected.
type stopServerlessMock struct {
	ElasticCacheServerlessAPI
	caches   map[string]*types.ServerlessCache
	busy     map[string]int
	modifies int
}

func newStopServerlessMock() *stopServerlessMock {
	return &stopServerlessMock{caches: map[string]*types.ServerlessCache{}, busy: map[string]int{}}
}

func (m *stopServerlessMock) add(name, status string, limits *types.CacheUsageLimits) {
	m.caches[name] = &types.ServerlessCache{
		ServerlessCacheName: aws.String(name),
		ARN:                 aws.String("arn:aws:elasticache:us-east-2:1:serverlesscache:" + name),
		Status:              aws.String(status),
		CacheUsageLimits:    limits,
	}
}

func (m *stopServerlessMock) DescribeServerlessCaches(_ context.Context, in *elasticache.DescribeServerlessCachesInput, _ ...func(*elasticache.Options)) (*elasticache.DescribeServerlessCachesOutput, error) {
	var out []types.ServerlessCache
	for name, sc := range m.caches {
		if in.ServerlessCacheName != nil && *in.ServerlessCacheName != name {
			continue
		}
		cp := *sc
		if m.busy[name] > 0 {
			m.busy[name]--
			cp.Status = aws.String("modifying")
		}
		out = append(out, cp)
	}
	if in.ServerlessCacheName != nil && len(out) == 0 {
		return nil, &types.ServerlessCacheNotFoundFault{Message: aws.String("not found")}
	}
	return &elasticache.DescribeServerlessCachesOutput{ServerlessCaches: out}, nil
}

func (m *stopServerlessMock) ListTagsForResource(context.Context, *elasticache.ListTagsForResourceInput, ...func(*elasticache.Options)) (*elasticache.ListTagsForResourceOutput, error) {
	return &elasticache.ListTagsForResourceOutput{}, nil
}

func (m *stopServerlessMock) ModifyServerlessCache(_ context.Context, in *elasticache.ModifyServerlessCacheInput, _ ...func(*elasticache.Options)) (*elasticache.ModifyServerlessCacheOutput, error) {
	m.modifies++
	name := aws.ToString(in.ServerlessCacheName)
	sc := m.caches[name]
	if aws.ToString(sc.Status) != "available" || m.busy[name] > 0 {
		return nil, apiErr("InvalidServerlessCacheStateFault", "not available")
	}
	nz := func(v *int32) *int32 {
		if aws.ToInt32(v) == 0 {
			return nil
		}
		return aws.Int32(*v)
	}
	if l := in.CacheUsageLimits; l != nil {
		out := &types.CacheUsageLimits{}
		if sc.CacheUsageLimits != nil {
			*out = *sc.CacheUsageLimits
		}
		if ds := l.DataStorage; ds != nil {
			if ds.Unit == "" {
				return nil, apiErr("InvalidParameterValue", "Unit is required")
			}
			if aws.ToInt32(ds.Maximum) != 0 && aws.ToInt32(ds.Minimum) > aws.ToInt32(ds.Maximum) {
				return nil, apiErr("InvalidParameterCombination", "minimum above maximum")
			}
			out.DataStorage = &types.DataStorage{Maximum: nz(ds.Maximum), Minimum: nz(ds.Minimum), Unit: ds.Unit}
			if out.DataStorage.Maximum == nil && out.DataStorage.Minimum == nil {
				out.DataStorage = nil
			}
		}
		if e := l.ECPUPerSecond; e != nil {
			out.ECPUPerSecond = &types.ECPUPerSecond{Maximum: nz(e.Maximum), Minimum: nz(e.Minimum)}
			if out.ECPUPerSecond.Maximum == nil && out.ECPUPerSecond.Minimum == nil {
				out.ECPUPerSecond = nil
			}
		}
		if out.DataStorage == nil && out.ECPUPerSecond == nil {
			out = nil
		}
		sc.CacheUsageLimits = out
	}
	m.busy[name] = 2
	return &elasticache.ModifyServerlessCacheOutput{}, nil
}

func serverlessRoundTrip(t *testing.T, m *stopServerlessMock, name string) string {
	t.Helper()
	ctx := context.Background()
	prev, err := captureElasticCacheServerless(ctx, m, aws.String(name))
	require.NoError(t, err)

	require.NoError(t, stopElasticCacheServerless(ctx, m, aws.String(name), prev))
	l := m.caches[name].CacheUsageLimits
	require.NotNil(t, l)
	assert.Equal(t, int32(1), aws.ToInt32(l.DataStorage.Maximum))
	assert.Nil(t, l.DataStorage.Minimum)
	assert.Equal(t, int32(1000), aws.ToInt32(l.ECPUPerSecond.Maximum))
	assert.Nil(t, l.ECPUPerSecond.Minimum)
	_, err = captureElasticCacheServerless(ctx, m, aws.String(name))
	require.ErrorIs(t, err, resource.ErrAlreadyStopped)

	n := m.modifies
	require.NoError(t, stopElasticCacheServerless(ctx, m, aws.String(name), prev))
	assert.Equal(t, n, m.modifies, "second stop must not modify")

	require.NoError(t, startElasticCacheServerless(ctx, m, aws.String(name), prev))
	again, err := captureElasticCacheServerless(ctx, m, aws.String(name))
	require.NoError(t, err)
	assert.JSONEq(t, string(prev), string(again))

	n = m.modifies
	require.NoError(t, startElasticCacheServerless(ctx, m, aws.String(name), prev))
	assert.Equal(t, n, m.modifies, "second start must not modify")
	return string(prev)
}

func TestServerlessStop_RoundTripWithLimits(t *testing.T) {
	m := newStopServerlessMock()
	m.add("sl", "available", &types.CacheUsageLimits{
		DataStorage:   &types.DataStorage{Maximum: aws.Int32(5), Minimum: aws.Int32(2), Unit: types.DataStorageUnitGb},
		ECPUPerSecond: &types.ECPUPerSecond{Maximum: aws.Int32(5000), Minimum: aws.Int32(2000)},
	})
	m.busy["sl"] = 2 // capture waits for it to settle
	prev := serverlessRoundTrip(t, m, "sl")
	assert.JSONEq(t, `{"data_storage_max":5,"data_storage_min":2,"data_storage_unit":"GB","ecpu_max":5000,"ecpu_min":2000}`, prev)
}

func TestServerlessStop_RoundTripWithoutLimits(t *testing.T) {
	m := newStopServerlessMock()
	m.add("sl", "available", nil)
	prev := serverlessRoundTrip(t, m, "sl")
	assert.JSONEq(t, `{}`, prev)
	assert.Nil(t, m.caches["sl"].CacheUsageLimits, "limits are removed again")
}

func TestServerlessStop_RoundTripOnlyECPULimit(t *testing.T) {
	m := newStopServerlessMock()
	m.add("sl", "available", &types.CacheUsageLimits{ECPUPerSecond: &types.ECPUPerSecond{Maximum: aws.Int32(20000)}})
	prev := serverlessRoundTrip(t, m, "sl")
	assert.JSONEq(t, `{"ecpu_max":20000}`, prev)
	assert.Nil(t, m.caches["sl"].CacheUsageLimits.DataStorage)
}

func TestServerlessStop_AlreadyStopped(t *testing.T) {
	m := newStopServerlessMock()
	m.add("low", "available", &types.CacheUsageLimits{
		DataStorage:   &types.DataStorage{Maximum: aws.Int32(1), Unit: types.DataStorageUnitGb},
		ECPUPerSecond: &types.ECPUPerSecond{Maximum: aws.Int32(1000)},
	})
	m.add("deleting", "deleting", nil)
	for _, name := range []string{"low", "deleting", "missing"} {
		_, err := captureElasticCacheServerless(context.Background(), m, aws.String(name))
		assert.ErrorIs(t, err, resource.ErrAlreadyStopped, name)
	}
	// Only one of the two limits at target is not stopped.
	m.add("half", "available", &types.CacheUsageLimits{DataStorage: &types.DataStorage{Maximum: aws.Int32(1), Unit: types.DataStorageUnitGb}})
	_, err := captureElasticCacheServerless(context.Background(), m, aws.String("half"))
	assert.NoError(t, err)
}

func TestServerlessStop_StartGone(t *testing.T) {
	m := newStopServerlessMock()
	err := startElasticCacheServerless(context.Background(), m, aws.String("missing"), []byte(`{}`))
	assert.ErrorIs(t, err, resource.ErrGone)
}

func TestServerlessStop_ListKeepsModifying(t *testing.T) {
	m := newStopServerlessMock()
	m.add("a", "available", nil)
	m.add("b", "modifying", nil)
	m.add("c", "deleting", nil)
	ids, err := listStoppableElasticCacheServerless(context.Background(), m, resource.Scope{}, config.ResourceType{})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b"}, aws.ToStringSlice(ids))
}
