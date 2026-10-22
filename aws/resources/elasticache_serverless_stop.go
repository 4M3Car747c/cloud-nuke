package resources

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/gruntwork-io/cloud-nuke/config"
	"github.com/gruntwork-io/cloud-nuke/logging"
	"github.com/gruntwork-io/cloud-nuke/resource"
	"github.com/gruntwork-io/cloud-nuke/util"
	goerrors "github.com/gruntwork-io/go-commons/errors"
)

// ElastiCache Serverless cannot be stopped either. "Stopping" a serverless cache lowers its
// usage limits (CacheUsageLimits) to the smallest maximums AWS allows; "starting" restores the
// previous limits. Data above the new storage limit may be evicted.

// The lowest maximums AWS accepts.
const (
	serverlessMinDataStorageMaxGB = 1
	serverlessMinECPUMax          = 1000
)

// serverlessPrev is what Start needs: the cache's usage limits. Nil fields were not set.
type serverlessPrev struct {
	DataStorageMax  *int32 `json:"data_storage_max,omitempty"`
	DataStorageMin  *int32 `json:"data_storage_min,omitempty"`
	DataStorageUnit string `json:"data_storage_unit,omitempty"`
	ECPUMax         *int32 `json:"ecpu_max,omitempty"`
	ECPUMin         *int32 `json:"ecpu_min,omitempty"`
}

func serverlessStopHooks() *resource.StopHooks[ElasticCacheServerlessAPI] {
	return &resource.StopHooks[ElasticCacheServerlessAPI]{
		List:    listStoppableElasticCacheServerless,
		Capture: captureElasticCacheServerless,
		Stop:    stopElasticCacheServerless,
		Start:   startElasticCacheServerless,
	}
}

// listStoppableElasticCacheServerless lists serverless caches like the nuke lister but also
// keeps caches that are creating or modifying: they must still be stopped (Capture waits).
func listStoppableElasticCacheServerless(ctx context.Context, client ElasticCacheServerlessAPI, _ resource.Scope, cfg config.ResourceType) ([]*string, error) {
	var out []*string
	paginator := elasticache.NewDescribeServerlessCachesPaginator(client, &elasticache.DescribeServerlessCachesInput{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, goerrors.WithStackTrace(err)
		}
		for _, sc := range page.ServerlessCaches {
			if serverlessGoneStatus(aws.ToString(sc.Status)) {
				continue
			}
			tags, err := client.ListTagsForResource(ctx, &elasticache.ListTagsForResourceInput{ResourceName: sc.ARN})
			if err != nil {
				logging.Debugf("Failed to fetch tags for ElastiCache serverless cache %s: %s", aws.ToString(sc.ServerlessCacheName), err)
				continue
			}
			if cfg.ShouldInclude(config.ResourceValue{
				Name: sc.ServerlessCacheName,
				Time: sc.CreateTime,
				Tags: util.ConvertElastiCacheTagsToMap(tags.TagList),
			}) {
				out = append(out, sc.ServerlessCacheName)
			}
		}
	}
	return out, nil
}

func serverlessGoneStatus(status string) bool {
	switch strings.ToLower(status) {
	case "deleting", "deleted", "create-failed":
		return true
	}
	return false
}

// describeServerlessCache returns the cache, or nil if it does not exist.
func describeServerlessCache(ctx context.Context, client ElasticCacheServerlessAPI, name string) (*types.ServerlessCache, error) {
	out, err := client.DescribeServerlessCaches(ctx, &elasticache.DescribeServerlessCachesInput{ServerlessCacheName: aws.String(name)})
	if err != nil {
		var nf *types.ServerlessCacheNotFoundFault
		if errors.As(err, &nf) || hasErrorCode(err, "ServerlessCacheNotFoundFault") {
			return nil, nil
		}
		return nil, goerrors.WithStackTrace(err)
	}
	for i := range out.ServerlessCaches {
		if aws.ToString(out.ServerlessCaches[i].ServerlessCacheName) == name {
			return &out.ServerlessCaches[i], nil
		}
	}
	return nil, nil
}

// waitServerlessCache polls until the cache is available and done (if set) holds for it.
// The SDK has no waiter for serverless caches.
func waitServerlessCache(ctx context.Context, client ElasticCacheServerlessAPI, name, what string, done func(*types.ServerlessCache) bool) (*types.ServerlessCache, error) {
	logging.Debugf("Waiting for ElastiCache serverless cache %s: %s", name, what)
	ctx, cancel := context.WithTimeout(ctx, elasticacheWaitTimeout)
	defer cancel()
	delay := elasticacheWaitMinDelay
	for {
		sc, err := describeServerlessCache(ctx, client, name)
		if err != nil {
			return nil, fmt.Errorf("waiting for serverless cache %s (%s): %w", name, what, err)
		}
		if sc == nil {
			return nil, fmt.Errorf("serverless cache %s disappeared while waiting (%s)", name, what)
		}
		status := strings.ToLower(aws.ToString(sc.Status))
		if status == "available" && (done == nil || done(sc)) {
			return sc, nil
		}
		if serverlessGoneStatus(status) {
			return nil, fmt.Errorf("serverless cache %s is %s", name, status)
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("waiting for serverless cache %s (%s): %w", name, what, ctx.Err())
		case <-time.After(delay):
		}
		delay = min(delay*2, elasticacheWaitMaxDelay)
	}
}

func serverlessLimitsOf(sc *types.ServerlessCache) serverlessPrev {
	var p serverlessPrev
	if l := sc.CacheUsageLimits; l != nil {
		if ds := l.DataStorage; ds != nil {
			p.DataStorageMax = positiveOrNil(ds.Maximum)
			p.DataStorageMin = positiveOrNil(ds.Minimum)
			if p.DataStorageMax != nil || p.DataStorageMin != nil {
				p.DataStorageUnit = string(ds.Unit)
			}
		}
		if e := l.ECPUPerSecond; e != nil {
			p.ECPUMax = positiveOrNil(e.Maximum)
			p.ECPUMin = positiveOrNil(e.Minimum)
		}
	}
	return p
}

// positiveOrNil treats 0 like "not set": AWS uses 0 to remove a limit.
func positiveOrNil(v *int32) *int32 {
	if v == nil || *v <= 0 {
		return nil
	}
	return aws.Int32(*v)
}

// serverlessAtTarget reports whether both maximums are already at (or below) the lowest allowed.
func serverlessAtTarget(p serverlessPrev) bool {
	return p.DataStorageMax != nil && *p.DataStorageMax <= serverlessMinDataStorageMaxGB &&
		(p.DataStorageUnit == "" || p.DataStorageUnit == string(types.DataStorageUnitGb)) &&
		p.ECPUMax != nil && *p.ECPUMax <= serverlessMinECPUMax
}

func int32Equal(a, b *int32) bool {
	return aws.ToInt32(a) == aws.ToInt32(b)
}

func serverlessLimitsEqual(a, b serverlessPrev) bool {
	return int32Equal(a.DataStorageMax, b.DataStorageMax) && int32Equal(a.DataStorageMin, b.DataStorageMin) &&
		int32Equal(a.ECPUMax, b.ECPUMax) && int32Equal(a.ECPUMin, b.ECPUMin)
}

// serverlessTarget is the stopped configuration: the lowest maximums and no minimums.
// A minimum of 0 removes it (minimums must not exceed maximums, and are billed).
func serverlessTarget() serverlessPrev {
	return serverlessPrev{
		DataStorageMax:  aws.Int32(serverlessMinDataStorageMaxGB),
		DataStorageUnit: string(types.DataStorageUnitGb),
		ECPUMax:         aws.Int32(serverlessMinECPUMax),
	}
}

// serverlessUsageLimits converts p to the API shape. Unset values are sent as 0, which is how
// the API removes a limit (Minimum=0 and Maximum=0 removes both).
func serverlessUsageLimits(p serverlessPrev) *types.CacheUsageLimits {
	unit := types.DataStorageUnit(p.DataStorageUnit)
	if unit == "" {
		unit = types.DataStorageUnitGb
	}
	return &types.CacheUsageLimits{
		DataStorage: &types.DataStorage{
			Maximum: aws.Int32(aws.ToInt32(p.DataStorageMax)),
			Minimum: aws.Int32(aws.ToInt32(p.DataStorageMin)),
			Unit:    unit,
		},
		ECPUPerSecond: &types.ECPUPerSecond{
			Maximum: aws.Int32(aws.ToInt32(p.ECPUMax)),
			Minimum: aws.Int32(aws.ToInt32(p.ECPUMin)),
		},
	}
}

func captureElasticCacheServerless(ctx context.Context, client ElasticCacheServerlessAPI, id *string) (json.RawMessage, error) {
	name := aws.ToString(id)
	sc, err := describeServerlessCache(ctx, client, name)
	if err != nil {
		return nil, err
	}
	if sc == nil || serverlessGoneStatus(aws.ToString(sc.Status)) {
		return nil, resource.ErrAlreadyStopped
	}
	if !strings.EqualFold(aws.ToString(sc.Status), "available") {
		if sc, err = waitServerlessCache(ctx, client, name, "settle before capture", nil); err != nil {
			return nil, err
		}
	}
	p := serverlessLimitsOf(sc)
	if serverlessAtTarget(p) {
		return nil, resource.ErrAlreadyStopped
	}
	return marshalPrev(p)
}

// setServerlessLimits changes the usage limits to want unless they already are, and waits.
func setServerlessLimits(ctx context.Context, client ElasticCacheServerlessAPI, name, what string, want serverlessPrev) error {
	sc, err := waitServerlessCache(ctx, client, name, "settle before "+what, nil)
	if err != nil {
		return err
	}
	if serverlessLimitsEqual(serverlessLimitsOf(sc), want) {
		return nil
	}
	if _, err := client.ModifyServerlessCache(ctx, &elasticache.ModifyServerlessCacheInput{
		ServerlessCacheName: aws.String(name),
		CacheUsageLimits:    serverlessUsageLimits(want),
	}); err != nil {
		return fmt.Errorf("%s: changing usage limits of serverless cache %s: %w", what, name, err)
	}
	_, err = waitServerlessCache(ctx, client, name, what, func(sc *types.ServerlessCache) bool {
		return serverlessLimitsEqual(serverlessLimitsOf(sc), want)
	})
	return err
}

func stopElasticCacheServerless(ctx context.Context, client ElasticCacheServerlessAPI, id *string, _ json.RawMessage) error {
	name := aws.ToString(id)
	sc, err := describeServerlessCache(ctx, client, name)
	if err != nil {
		return err
	}
	if sc == nil || serverlessGoneStatus(aws.ToString(sc.Status)) {
		return nil
	}
	if serverlessAtTarget(serverlessLimitsOf(sc)) {
		return nil
	}
	return setServerlessLimits(ctx, client, name, "stop", serverlessTarget())
}

func startElasticCacheServerless(ctx context.Context, client ElasticCacheServerlessAPI, id *string, prevRaw json.RawMessage) error {
	var prev serverlessPrev
	if err := unmarshalPrev(prevRaw, &prev); err != nil {
		return err
	}
	name := aws.ToString(id)
	sc, err := describeServerlessCache(ctx, client, name)
	if err != nil {
		return err
	}
	if sc == nil || serverlessGoneStatus(aws.ToString(sc.Status)) {
		return resource.ErrGone
	}
	return setServerlessLimits(ctx, client, name, "start", prev)
}
