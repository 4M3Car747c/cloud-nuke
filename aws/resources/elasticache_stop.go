package resources

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/gruntwork-io/cloud-nuke/logging"
	"github.com/gruntwork-io/cloud-nuke/resource"
	goerrors "github.com/gruntwork-io/go-commons/errors"
)

// ElastiCache has no stop API. "Stopping" a cluster shrinks it to the cheapest configuration
// that still holds its data and keeps its endpoint; "starting" grows it back exactly:
//
//   - Valkey/Redis replication groups: Multi-AZ and automatic failover off, replicas removed,
//     node type reduced to a small burstable type that fits the data.
//   - Memcached clusters: reduced to one node (memcached cannot change node type).
//   - Single-node Valkey/Redis clusters outside a replication group: node type reduced.
//
// Every change is asynchronous, so the hooks wait for the cluster to become available again
// after each step. Each step is decided from the current state, so a rerun after a crash
// continues where the previous run stopped.

// Waiter settings. Variables so tests can shorten them.
var (
	elasticacheWaitTimeout  = 45 * time.Minute
	elasticacheWaitMinDelay = 15 * time.Second
	elasticacheWaitMaxDelay = 60 * time.Second
)

const (
	elasticacheKindReplicationGroup = "replication-group"
	elasticacheKindMemcached        = "memcached"
	elasticacheKindCacheCluster     = "cache-cluster"

	// elasticacheMemoryHeadroom is the share of a node's memory the data may fill.
	elasticacheMemoryHeadroom = 0.8
)

// elasticacheStopClient is the client the elasticache resource uses: the ElastiCache client
// plus CloudWatch's GetMetricStatistics, which picks the target node type. Only the
// ElastiCache client is embedded because both have a ListTagsForResource method.
type elasticacheStopClient struct {
	*elasticache.Client
	cw *cloudwatch.Client
}

func newElasticacheStopClient(cfg aws.Config) *elasticacheStopClient {
	return &elasticacheStopClient{Client: elasticache.NewFromConfig(cfg), cw: cloudwatch.NewFromConfig(cfg)}
}

func (c *elasticacheStopClient) GetMetricStatistics(ctx context.Context, params *cloudwatch.GetMetricStatisticsInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricStatisticsOutput, error) {
	return c.cw.GetMetricStatistics(ctx, params, optFns...)
}

// elasticachePrev is what Start needs to restore a cluster.
type elasticachePrev struct {
	Kind string `json:"kind"`
	// Replication groups and single-node Valkey/Redis clusters.
	NodeType       string `json:"node_type,omitempty"`
	TargetNodeType string `json:"target_node_type,omitempty"`
	// Replication groups only.
	ClusterMode       bool             `json:"cluster_mode,omitempty"`
	AutomaticFailover bool             `json:"automatic_failover,omitempty"`
	MultiAZ           bool             `json:"multi_az,omitempty"`
	Replicas          map[string]int32 `json:"replicas,omitempty"` // node group ID -> replica count
	// Memcached only.
	NumNodes int32 `json:"num_nodes,omitempty"`
}

func elasticacheStopHooks() *resource.StopHooks[ElasticachesAPI] {
	return &resource.StopHooks[ElasticachesAPI]{
		Capture: captureElasticache,
		Stop:    stopElasticache,
		Start:   startElasticache,
	}
}

// ---------------------------------------------------------------------------------------------
// Node type selection

// elasticacheSmallNode describes the burstable node types a cluster can be shrunk to.
type elasticacheSmallNode struct {
	size   string  // micro, small, medium
	rank   int     // ordering by size
	memGiB float64 // memory available for data
}

// elasticacheSmallNodes lists the burstable node types; any other type counts as bigger.
var elasticacheSmallNodes = map[string]elasticacheSmallNode{
	"cache.t1.micro":   {"micro", 0, 0.213},
	"cache.t2.micro":   {"micro", 0, 0.555},
	"cache.t3.micro":   {"micro", 0, 0.5},
	"cache.t4g.micro":  {"micro", 0, 0.5},
	"cache.t2.small":   {"small", 1, 1.55},
	"cache.t3.small":   {"small", 1, 1.37},
	"cache.t4g.small":  {"small", 1, 1.37},
	"cache.t2.medium":  {"medium", 2, 3.22},
	"cache.t3.medium":  {"medium", 2, 3.09},
	"cache.t4g.medium": {"medium", 2, 3.09},
}

// elasticacheTargetSizes are tried in order; the first whose memory fits the data wins.
var elasticacheTargetSizes = []string{"micro", "small", "medium"}

// elasticacheNeedsResize reports whether a node of type current must be changed to reach
// target. Nodes that are already burstable and no bigger than target are kept (never grown).
func elasticacheNeedsResize(current, target string) bool {
	cur, ok := elasticacheSmallNodes[current]
	if !ok {
		return true
	}
	return cur.rank > elasticacheSmallNodes[target].rank
}

// elasticacheTargetNodeType picks the smallest burstable type whose memory × 0.8 holds
// usedBytes. It returns an *UnsupportedError when even the largest does not.
func elasticacheTargetNodeType(engine, engineVersion string, usedBytes float64) (string, error) {
	family := "t4g"
	if !elasticacheSupportsGraviton(engine, engineVersion) {
		family = "t3"
	}
	for _, size := range elasticacheTargetSizes {
		nodeType := "cache." + family + "." + size
		if elasticacheSmallNodes[nodeType].memGiB*(1<<30)*elasticacheMemoryHeadroom >= usedBytes {
			return nodeType, nil
		}
	}
	largest := elasticacheSmallNodes["cache."+family+".medium"].memGiB
	return "", resource.Unsupported("data (%.2f GiB) larger than %.0f%% of a cache.%s.medium node (%.2f GiB)",
		usedBytes/(1<<30), elasticacheMemoryHeadroom*100, family, largest)
}

// elasticacheSupportsGraviton reports whether t4g nodes can run engine/version: any Valkey,
// and Redis 5.0.6 or later.
func elasticacheSupportsGraviton(engine, version string) bool {
	switch strings.ToLower(engine) {
	case "valkey":
		return true
	case "redis":
		return compareEngineVersion(version, "5.0.6") >= 0
	}
	return false
}

// compareEngineVersion compares dotted versions; non-numeric parts (like "x" in "6.x") count as 0.
func compareEngineVersion(a, b string) int {
	pa, pb := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; i < max(len(pa), len(pb)); i++ {
		var x, y int
		if i < len(pa) {
			x, _ = strconv.Atoi(pa[i])
		}
		if i < len(pb) {
			y, _ = strconv.Atoi(pb[i])
		}
		if x != y {
			if x < y {
				return -1
			}
			return 1
		}
	}
	return 0
}

// elasticacheGravitonFallback returns the t3 equivalent of a t4g node type, or "".
func elasticacheGravitonFallback(nodeType string) string {
	if size, ok := strings.CutPrefix(nodeType, "cache.t4g."); ok {
		return "cache.t3." + size
	}
	return ""
}

// elasticacheNodeTypeRejected reports whether err looks like AWS refusing a node type.
func elasticacheNodeTypeRejected(err error) bool {
	return hasErrorCode(err, "InvalidParameterCombination", "InvalidParameterValue")
}

// elasticacheBytesUsed returns the largest BytesUsedForCache of any of the clusters over the
// last hour. Clusters without datapoints count as empty.
func elasticacheBytesUsed(ctx context.Context, client ElasticachesAPI, clusterIDs []string) (float64, error) {
	end := time.Now()
	var maxBytes float64
	for _, id := range clusterIDs {
		out, err := client.GetMetricStatistics(ctx, &cloudwatch.GetMetricStatisticsInput{
			Namespace:  aws.String("AWS/ElastiCache"),
			MetricName: aws.String("BytesUsedForCache"),
			Dimensions: []cwtypes.Dimension{{Name: aws.String("CacheClusterId"), Value: aws.String(id)}},
			StartTime:  aws.Time(end.Add(-time.Hour)),
			EndTime:    aws.Time(end),
			Period:     aws.Int32(300),
			Statistics: []cwtypes.Statistic{cwtypes.StatisticMaximum},
		})
		if err != nil {
			return 0, fmt.Errorf("reading BytesUsedForCache of %s: %w", id, err)
		}
		for _, dp := range out.Datapoints {
			maxBytes = max(maxBytes, aws.ToFloat64(dp.Maximum))
		}
	}
	return maxBytes, nil
}

// ---------------------------------------------------------------------------------------------
// Describe and wait helpers

// describeReplicationGroup returns the group, or nil if it does not exist.
func describeReplicationGroup(ctx context.Context, client ElasticachesAPI, id string) (*types.ReplicationGroup, error) {
	out, err := client.DescribeReplicationGroups(ctx, &elasticache.DescribeReplicationGroupsInput{ReplicationGroupId: aws.String(id)})
	if err != nil {
		var nf *types.ReplicationGroupNotFoundFault
		if errors.As(err, &nf) || hasErrorCode(err, "ReplicationGroupNotFoundFault") {
			return nil, nil
		}
		return nil, goerrors.WithStackTrace(err)
	}
	for i := range out.ReplicationGroups {
		if aws.ToString(out.ReplicationGroups[i].ReplicationGroupId) == id {
			return &out.ReplicationGroups[i], nil
		}
	}
	return nil, nil
}

// describeCacheCluster returns the cluster with its nodes, or nil if it does not exist.
func describeCacheCluster(ctx context.Context, client ElasticachesAPI, id string) (*types.CacheCluster, error) {
	out, err := client.DescribeCacheClusters(ctx, &elasticache.DescribeCacheClustersInput{
		CacheClusterId:    aws.String(id),
		ShowCacheNodeInfo: aws.Bool(true),
	})
	if err != nil {
		var nf *types.CacheClusterNotFoundFault
		if errors.As(err, &nf) || hasErrorCode(err, "CacheClusterNotFound") {
			return nil, nil
		}
		return nil, goerrors.WithStackTrace(err)
	}
	for i := range out.CacheClusters {
		if aws.ToString(out.CacheClusters[i].CacheClusterId) == id {
			return &out.CacheClusters[i], nil
		}
	}
	return nil, nil
}

func elasticacheFailedStatus(status string) bool {
	switch status {
	case "deleting", "deleted", "create-failed", "incompatible-network", "restore-failed":
		return true
	}
	return false
}

// rgMemberClustersSettled reports whether every cache cluster of the group is available.
// A group reports "available" while the clusters removed by DecreaseReplicaCount are still
// deleting (and new replicas may still be creating); AWS rejects the next modification then
// with InvalidCacheClusterState.
func rgMemberClustersSettled(ctx context.Context, client ElasticachesAPI, id string) (bool, error) {
	paginator := elasticache.NewDescribeCacheClustersPaginator(client, &elasticache.DescribeCacheClustersInput{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return false, goerrors.WithStackTrace(err)
		}
		for _, cc := range page.CacheClusters {
			if aws.ToString(cc.ReplicationGroupId) == id && aws.ToString(cc.CacheClusterStatus) != "available" {
				return false, nil
			}
		}
	}
	return true, nil
}

// waitReplicationGroup waits until the group and all its cache clusters are available and
// done (if set) holds for the group.
func waitReplicationGroup(ctx context.Context, client ElasticachesAPI, id string, what string, done func(*types.ReplicationGroup) bool) (*types.ReplicationGroup, error) {
	waiter := elasticache.NewReplicationGroupAvailableWaiter(client, func(o *elasticache.ReplicationGroupAvailableWaiterOptions) {
		o.MinDelay = elasticacheWaitMinDelay
		o.MaxDelay = elasticacheWaitMaxDelay
		o.Retryable = func(ctx context.Context, _ *elasticache.DescribeReplicationGroupsInput, out *elasticache.DescribeReplicationGroupsOutput, err error) (bool, error) {
			if err != nil {
				return false, err
			}
			if len(out.ReplicationGroups) == 0 {
				return false, fmt.Errorf("replication group %s disappeared", id)
			}
			rg := &out.ReplicationGroups[0]
			status := aws.ToString(rg.Status)
			if status == "available" {
				if done != nil && !done(rg) {
					return true, nil
				}
				settled, err := rgMemberClustersSettled(ctx, client, id)
				return !settled, err
			}
			if elasticacheFailedStatus(status) {
				return false, fmt.Errorf("replication group %s is %s", id, status)
			}
			return true, nil
		}
	})
	logging.Debugf("Waiting for ElastiCache replication group %s: %s", id, what)
	out, err := waiter.WaitForOutput(ctx, &elasticache.DescribeReplicationGroupsInput{ReplicationGroupId: aws.String(id)}, elasticacheWaitTimeout)
	if err != nil {
		return nil, fmt.Errorf("waiting for replication group %s (%s): %w", id, what, err)
	}
	return &out.ReplicationGroups[0], nil
}

// waitCacheCluster waits until the cluster is available and done (if set) holds for it.
func waitCacheCluster(ctx context.Context, client ElasticachesAPI, id string, what string, done func(*types.CacheCluster) bool) (*types.CacheCluster, error) {
	waiter := elasticache.NewCacheClusterAvailableWaiter(client, func(o *elasticache.CacheClusterAvailableWaiterOptions) {
		o.MinDelay = elasticacheWaitMinDelay
		o.MaxDelay = elasticacheWaitMaxDelay
		o.Retryable = func(_ context.Context, _ *elasticache.DescribeCacheClustersInput, out *elasticache.DescribeCacheClustersOutput, err error) (bool, error) {
			if err != nil {
				return false, err
			}
			if len(out.CacheClusters) == 0 {
				return false, fmt.Errorf("cache cluster %s disappeared", id)
			}
			cc := &out.CacheClusters[0]
			status := aws.ToString(cc.CacheClusterStatus)
			if status == "available" {
				return done != nil && !done(cc), nil
			}
			if elasticacheFailedStatus(status) {
				return false, fmt.Errorf("cache cluster %s is %s", id, status)
			}
			return true, nil
		}
	})
	logging.Debugf("Waiting for ElastiCache cluster %s: %s", id, what)
	out, err := waiter.WaitForOutput(ctx, &elasticache.DescribeCacheClustersInput{
		CacheClusterId:    aws.String(id),
		ShowCacheNodeInfo: aws.Bool(true),
	}, elasticacheWaitTimeout)
	if err != nil {
		return nil, fmt.Errorf("waiting for cache cluster %s (%s): %w", id, what, err)
	}
	return &out.CacheClusters[0], nil
}

// elasticacheBusyCodes are returned while the group or one of its clusters is still changing.
var elasticacheBusyCodes = []string{
	"InvalidCacheClusterState", "InvalidCacheClusterStateFault",
	"InvalidReplicationGroupState", "InvalidReplicationGroupStateFault",
}

// elasticacheRetryBusy runs op again while AWS says the cluster is busy (for example a
// replica that is still being deleted although the group already reports available).
func elasticacheRetryBusy(ctx context.Context, op func() error) error {
	deadline := time.Now().Add(elasticacheWaitTimeout)
	delay := elasticacheWaitMinDelay
	for {
		err := op()
		if err == nil || !hasErrorCode(err, elasticacheBusyCodes...) || time.Now().After(deadline) {
			return err
		}
		logging.Debugf("ElastiCache is busy, retrying in %s: %v", delay, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay = min(delay*2, elasticacheWaitMaxDelay)
	}
}

func retryModifyReplicationGroup(ctx context.Context, client ElasticachesAPI, in *elasticache.ModifyReplicationGroupInput) error {
	return elasticacheRetryBusy(ctx, func() error { _, err := client.ModifyReplicationGroup(ctx, in); return err })
}

func retryDecreaseReplicaCount(ctx context.Context, client ElasticachesAPI, in *elasticache.DecreaseReplicaCountInput) error {
	return elasticacheRetryBusy(ctx, func() error { _, err := client.DecreaseReplicaCount(ctx, in); return err })
}

func retryIncreaseReplicaCount(ctx context.Context, client ElasticachesAPI, in *elasticache.IncreaseReplicaCountInput) error {
	return elasticacheRetryBusy(ctx, func() error { _, err := client.IncreaseReplicaCount(ctx, in); return err })
}

func retryModifyCacheCluster(ctx context.Context, client ElasticachesAPI, in *elasticache.ModifyCacheClusterInput) error {
	return elasticacheRetryBusy(ctx, func() error { _, err := client.ModifyCacheCluster(ctx, in); return err })
}

// ---------------------------------------------------------------------------------------------
// Hooks: dispatch on the kind of cluster

func captureElasticache(ctx context.Context, client ElasticachesAPI, id *string) (json.RawMessage, error) {
	rg, err := describeReplicationGroup(ctx, client, aws.ToString(id))
	if err != nil {
		return nil, err
	}
	if rg != nil {
		return captureReplicationGroup(ctx, client, rg)
	}
	cc, err := describeCacheCluster(ctx, client, aws.ToString(id))
	if err != nil {
		return nil, err
	}
	if cc == nil || cc.ReplicationGroupId != nil {
		// Gone, or a member of a replication group, which is handled under the group's ID.
		return nil, resource.ErrAlreadyStopped
	}
	if strings.EqualFold(aws.ToString(cc.Engine), "memcached") {
		return captureMemcached(ctx, client, cc)
	}
	return captureSingleNodeCluster(ctx, client, cc)
}

func stopElasticache(ctx context.Context, client ElasticachesAPI, id *string, prevRaw json.RawMessage) error {
	var prev elasticachePrev
	if err := unmarshalPrev(prevRaw, &prev); err != nil {
		return err
	}
	switch prev.Kind {
	case elasticacheKindReplicationGroup:
		return stopReplicationGroup(ctx, client, aws.ToString(id), prev)
	case elasticacheKindMemcached:
		return stopMemcached(ctx, client, aws.ToString(id))
	case elasticacheKindCacheCluster:
		return stopSingleNodeCluster(ctx, client, aws.ToString(id), prev)
	}
	return fmt.Errorf("unknown elasticache kind %q in saved state", prev.Kind)
}

func startElasticache(ctx context.Context, client ElasticachesAPI, id *string, prevRaw json.RawMessage) error {
	var prev elasticachePrev
	if err := unmarshalPrev(prevRaw, &prev); err != nil {
		return err
	}
	switch prev.Kind {
	case elasticacheKindReplicationGroup:
		return startReplicationGroup(ctx, client, aws.ToString(id), prev)
	case elasticacheKindMemcached:
		return startMemcached(ctx, client, aws.ToString(id), prev)
	case elasticacheKindCacheCluster:
		return startSingleNodeCluster(ctx, client, aws.ToString(id), prev)
	}
	return fmt.Errorf("unknown elasticache kind %q in saved state", prev.Kind)
}

// ---------------------------------------------------------------------------------------------
// Replication groups

// rgReplicas returns the replica count of each node group (shard).
func rgReplicas(rg *types.ReplicationGroup) map[string]int32 {
	out := map[string]int32{}
	for _, ng := range rg.NodeGroups {
		out[aws.ToString(ng.NodeGroupId)] = int32(max(len(ng.NodeGroupMembers)-1, 0))
	}
	return out
}

func rgClusterMode(rg *types.ReplicationGroup) bool {
	return aws.ToBool(rg.ClusterEnabled)
}

func rgFailoverOn(rg *types.ReplicationGroup) bool {
	return rg.AutomaticFailover == types.AutomaticFailoverStatusEnabled || rg.AutomaticFailover == types.AutomaticFailoverStatusEnabling
}

func rgMultiAZOn(rg *types.ReplicationGroup) bool {
	return rg.MultiAZ == types.MultiAZStatusEnabled
}

// rgHighAvailabilityOff reports whether Multi-AZ and (outside cluster mode, where AWS requires
// it) automatic failover are off.
func rgHighAvailabilityOff(rg *types.ReplicationGroup) bool {
	if rgMultiAZOn(rg) {
		return false
	}
	return rgClusterMode(rg) || rg.AutomaticFailover == types.AutomaticFailoverStatusDisabled
}

func rgNoReplicas(rg *types.ReplicationGroup) bool {
	for _, n := range rgReplicas(rg) {
		if n > 0 {
			return false
		}
	}
	return true
}

// rgTargetNodeType reads the group's engine version and data size and picks the target type.
func rgTargetNodeType(ctx context.Context, client ElasticachesAPI, rg *types.ReplicationGroup) (string, error) {
	if len(rg.MemberClusters) == 0 {
		return "", fmt.Errorf("replication group %s has no member clusters", aws.ToString(rg.ReplicationGroupId))
	}
	member, err := describeCacheCluster(ctx, client, rg.MemberClusters[0])
	if err != nil {
		return "", err
	}
	if member == nil {
		return "", fmt.Errorf("member cluster %s of %s not found", rg.MemberClusters[0], aws.ToString(rg.ReplicationGroupId))
	}
	used, err := elasticacheBytesUsed(ctx, client, rg.MemberClusters)
	if err != nil {
		return "", err
	}
	return elasticacheTargetNodeType(aws.ToString(rg.Engine), aws.ToString(member.EngineVersion), used)
}

func captureReplicationGroup(ctx context.Context, client ElasticachesAPI, rg *types.ReplicationGroup) (json.RawMessage, error) {
	id := aws.ToString(rg.ReplicationGroupId)
	status := aws.ToString(rg.Status)
	if elasticacheFailedStatus(status) {
		return nil, resource.ErrAlreadyStopped
	}
	if status != "available" {
		var err error
		if rg, err = waitReplicationGroup(ctx, client, id, "settle before capture", nil); err != nil {
			return nil, err
		}
	}
	if rg.DataTiering == types.DataTieringStatusEnabled {
		return nil, resource.Unsupported("data tiering is enabled")
	}
	if rg.GlobalReplicationGroupInfo != nil && aws.ToString(rg.GlobalReplicationGroupInfo.GlobalReplicationGroupId) != "" {
		return nil, resource.Unsupported("member of global datastore %s", aws.ToString(rg.GlobalReplicationGroupInfo.GlobalReplicationGroupId))
	}
	target, err := rgTargetNodeType(ctx, client, rg)
	if err != nil {
		return nil, err
	}
	nodeType := aws.ToString(rg.CacheNodeType)
	if !elasticacheNeedsResize(nodeType, target) && rgNoReplicas(rg) && rgHighAvailabilityOff(rg) {
		return nil, resource.ErrAlreadyStopped
	}
	return marshalPrev(elasticachePrev{
		Kind:              elasticacheKindReplicationGroup,
		NodeType:          nodeType,
		TargetNodeType:    target,
		ClusterMode:       rgClusterMode(rg),
		AutomaticFailover: rgFailoverOn(rg),
		MultiAZ:           rgMultiAZOn(rg),
		Replicas:          rgReplicas(rg),
	})
}

// modifyReplicationGroupNodeType changes the node type, falling back from t4g to t3 if AWS
// rejects t4g. It returns the node type actually requested.
func modifyReplicationGroupNodeType(ctx context.Context, client ElasticachesAPI, id, nodeType string) (string, error) {
	err := retryModifyReplicationGroup(ctx, client, &elasticache.ModifyReplicationGroupInput{
		ReplicationGroupId: aws.String(id),
		CacheNodeType:      aws.String(nodeType),
		ApplyImmediately:   aws.Bool(true),
	})
	if err != nil && elasticacheNodeTypeRejected(err) {
		if fallback := elasticacheGravitonFallback(nodeType); fallback != "" {
			logging.Debugf("ElastiCache replication group %s rejected %s (%v); trying %s", id, nodeType, err, fallback)
			nodeType = fallback
			err = retryModifyReplicationGroup(ctx, client, &elasticache.ModifyReplicationGroupInput{
				ReplicationGroupId: aws.String(id),
				CacheNodeType:      aws.String(nodeType),
				ApplyImmediately:   aws.Bool(true),
			})
		}
	}
	if err != nil {
		return "", fmt.Errorf("changing node type of %s to %s: %w", id, nodeType, err)
	}
	return nodeType, nil
}

func stopReplicationGroup(ctx context.Context, client ElasticachesAPI, id string, prev elasticachePrev) error {
	rg, err := describeReplicationGroup(ctx, client, id)
	if err != nil {
		return err
	}
	if rg == nil {
		return nil // nothing left to stop
	}
	if rg, err = waitReplicationGroup(ctx, client, id, "settle before stop", nil); err != nil {
		return err
	}

	// 1. Multi-AZ and automatic failover off; replicas cannot be removed while they are on.
	//    Cluster mode enabled requires automatic failover, so only Multi-AZ is turned off there.
	if !rgHighAvailabilityOff(rg) {
		in := &elasticache.ModifyReplicationGroupInput{
			ReplicationGroupId: aws.String(id),
			MultiAZEnabled:     aws.Bool(false),
			ApplyImmediately:   aws.Bool(true),
		}
		if !rgClusterMode(rg) {
			in.AutomaticFailoverEnabled = aws.Bool(false)
		}
		if err := retryModifyReplicationGroup(ctx, client, in); err != nil {
			return fmt.Errorf("disabling Multi-AZ/automatic failover on %s: %w", id, err)
		}
		if rg, err = waitReplicationGroup(ctx, client, id, "Multi-AZ/failover off", rgHighAvailabilityOff); err != nil {
			return err
		}
	}

	// 2. Remove every replica.
	if !rgNoReplicas(rg) {
		in := &elasticache.DecreaseReplicaCountInput{
			ReplicationGroupId: aws.String(id),
			ApplyImmediately:   aws.Bool(true),
		}
		if rgClusterMode(rg) {
			for ngID, n := range rgReplicas(rg) {
				if n > 0 {
					in.ReplicaConfiguration = append(in.ReplicaConfiguration, types.ConfigureShard{
						NodeGroupId:     aws.String(ngID),
						NewReplicaCount: aws.Int32(0),
					})
				}
			}
			slices.SortFunc(in.ReplicaConfiguration, func(a, b types.ConfigureShard) int {
				return strings.Compare(aws.ToString(a.NodeGroupId), aws.ToString(b.NodeGroupId))
			})
		} else {
			in.NewReplicaCount = aws.Int32(0)
		}
		if err := retryDecreaseReplicaCount(ctx, client, in); err != nil {
			return fmt.Errorf("removing replicas of %s: %w", id, err)
		}
		if rg, err = waitReplicationGroup(ctx, client, id, "replicas removed", rgNoReplicas); err != nil {
			return err
		}
	}

	// 3. Shrink the node type.
	if elasticacheNeedsResize(aws.ToString(rg.CacheNodeType), prev.TargetNodeType) {
		nodeType, err := modifyReplicationGroupNodeType(ctx, client, id, prev.TargetNodeType)
		if err != nil {
			return err
		}
		if _, err := waitReplicationGroup(ctx, client, id, "node type "+nodeType, func(rg *types.ReplicationGroup) bool {
			return aws.ToString(rg.CacheNodeType) == nodeType
		}); err != nil {
			return err
		}
	}
	return nil
}

func startReplicationGroup(ctx context.Context, client ElasticachesAPI, id string, prev elasticachePrev) error {
	rg, err := describeReplicationGroup(ctx, client, id)
	if err != nil {
		return err
	}
	if rg == nil || elasticacheFailedStatus(aws.ToString(rg.Status)) {
		return resource.ErrGone
	}
	if rg, err = waitReplicationGroup(ctx, client, id, "settle before start", nil); err != nil {
		return err
	}

	// 1. Node type back.
	if aws.ToString(rg.CacheNodeType) != prev.NodeType {
		if err := retryModifyReplicationGroup(ctx, client, &elasticache.ModifyReplicationGroupInput{
			ReplicationGroupId: aws.String(id),
			CacheNodeType:      aws.String(prev.NodeType),
			ApplyImmediately:   aws.Bool(true),
		}); err != nil {
			return fmt.Errorf("restoring node type of %s to %s: %w", id, prev.NodeType, err)
		}
		if rg, err = waitReplicationGroup(ctx, client, id, "node type "+prev.NodeType, func(rg *types.ReplicationGroup) bool {
			return aws.ToString(rg.CacheNodeType) == prev.NodeType
		}); err != nil {
			return err
		}
	}

	// 2. Replicas back.
	replicasRestored := func(rg *types.ReplicationGroup) bool {
		cur := rgReplicas(rg)
		for ngID, want := range prev.Replicas {
			if cur[ngID] < want {
				return false
			}
		}
		return true
	}
	if !replicasRestored(rg) {
		in := &elasticache.IncreaseReplicaCountInput{
			ReplicationGroupId: aws.String(id),
			ApplyImmediately:   aws.Bool(true),
		}
		if prev.ClusterMode {
			cur := rgReplicas(rg)
			for ngID, want := range prev.Replicas {
				if _, exists := cur[ngID]; exists && cur[ngID] < want {
					in.ReplicaConfiguration = append(in.ReplicaConfiguration, types.ConfigureShard{
						NodeGroupId:     aws.String(ngID),
						NewReplicaCount: aws.Int32(want),
					})
				}
			}
			slices.SortFunc(in.ReplicaConfiguration, func(a, b types.ConfigureShard) int {
				return strings.Compare(aws.ToString(a.NodeGroupId), aws.ToString(b.NodeGroupId))
			})
		} else {
			var want int32
			for _, n := range prev.Replicas {
				want = max(want, n)
			}
			in.NewReplicaCount = aws.Int32(want)
		}
		if in.NewReplicaCount != nil || len(in.ReplicaConfiguration) > 0 {
			if err := retryIncreaseReplicaCount(ctx, client, in); err != nil {
				return fmt.Errorf("restoring replicas of %s: %w", id, err)
			}
			if rg, err = waitReplicationGroup(ctx, client, id, "replicas restored", func(rg *types.ReplicationGroup) bool {
				cur := rgReplicas(rg)
				for _, c := range in.ReplicaConfiguration {
					if cur[aws.ToString(c.NodeGroupId)] < aws.ToInt32(c.NewReplicaCount) {
						return false
					}
				}
				return in.NewReplicaCount == nil || replicasRestored(rg)
			}); err != nil {
				return err
			}
		}
	}

	// 3. Automatic failover and Multi-AZ back.
	if (prev.AutomaticFailover && !rgFailoverOn(rg)) || (prev.MultiAZ && !rgMultiAZOn(rg)) {
		in := &elasticache.ModifyReplicationGroupInput{
			ReplicationGroupId: aws.String(id),
			ApplyImmediately:   aws.Bool(true),
		}
		if prev.AutomaticFailover {
			in.AutomaticFailoverEnabled = aws.Bool(true)
		}
		if prev.MultiAZ {
			in.MultiAZEnabled = aws.Bool(true)
		}
		if err := retryModifyReplicationGroup(ctx, client, in); err != nil {
			return fmt.Errorf("re-enabling Multi-AZ/automatic failover on %s: %w", id, err)
		}
		if _, err := waitReplicationGroup(ctx, client, id, "Multi-AZ/failover on", func(rg *types.ReplicationGroup) bool {
			return (!prev.AutomaticFailover || rg.AutomaticFailover == types.AutomaticFailoverStatusEnabled) &&
				(!prev.MultiAZ || rgMultiAZOn(rg))
		}); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------------------------
// Memcached clusters

func captureMemcached(ctx context.Context, client ElasticachesAPI, cc *types.CacheCluster) (json.RawMessage, error) {
	id := aws.ToString(cc.CacheClusterId)
	status := aws.ToString(cc.CacheClusterStatus)
	if elasticacheFailedStatus(status) {
		return nil, resource.ErrAlreadyStopped
	}
	if status != "available" {
		var err error
		if cc, err = waitCacheCluster(ctx, client, id, "settle before capture", nil); err != nil {
			return nil, err
		}
	}
	if aws.ToInt32(cc.NumCacheNodes) <= 1 {
		return nil, resource.ErrAlreadyStopped
	}
	return marshalPrev(elasticachePrev{Kind: elasticacheKindMemcached, NumNodes: aws.ToInt32(cc.NumCacheNodes)})
}

func stopMemcached(ctx context.Context, client ElasticachesAPI, id string) error {
	cc, err := describeCacheCluster(ctx, client, id)
	if err != nil {
		return err
	}
	if cc == nil {
		return nil
	}
	if cc, err = waitCacheCluster(ctx, client, id, "settle before stop", nil); err != nil {
		return err
	}
	if aws.ToInt32(cc.NumCacheNodes) <= 1 {
		return nil
	}
	var nodeIDs []string
	for _, n := range cc.CacheNodes {
		nodeIDs = append(nodeIDs, aws.ToString(n.CacheNodeId))
	}
	slices.Sort(nodeIDs)
	if len(nodeIDs) < 2 {
		return fmt.Errorf("memcached cluster %s reports %d nodes but lists %d", id, aws.ToInt32(cc.NumCacheNodes), len(nodeIDs))
	}
	if err := retryModifyCacheCluster(ctx, client, &elasticache.ModifyCacheClusterInput{
		CacheClusterId:       aws.String(id),
		NumCacheNodes:        aws.Int32(1),
		CacheNodeIdsToRemove: nodeIDs[1:],
		ApplyImmediately:     aws.Bool(true),
	}); err != nil {
		return fmt.Errorf("removing nodes of %s: %w", id, err)
	}
	_, err = waitCacheCluster(ctx, client, id, "one node", func(cc *types.CacheCluster) bool {
		return aws.ToInt32(cc.NumCacheNodes) == 1
	})
	return err
}

func startMemcached(ctx context.Context, client ElasticachesAPI, id string, prev elasticachePrev) error {
	cc, err := describeCacheCluster(ctx, client, id)
	if err != nil {
		return err
	}
	if cc == nil || elasticacheFailedStatus(aws.ToString(cc.CacheClusterStatus)) {
		return resource.ErrGone
	}
	if cc, err = waitCacheCluster(ctx, client, id, "settle before start", nil); err != nil {
		return err
	}
	if aws.ToInt32(cc.NumCacheNodes) >= prev.NumNodes {
		return nil
	}
	if err := retryModifyCacheCluster(ctx, client, &elasticache.ModifyCacheClusterInput{
		CacheClusterId:   aws.String(id),
		NumCacheNodes:    aws.Int32(prev.NumNodes),
		ApplyImmediately: aws.Bool(true),
	}); err != nil {
		return fmt.Errorf("restoring %d nodes of %s: %w", prev.NumNodes, id, err)
	}
	_, err = waitCacheCluster(ctx, client, id, "nodes restored", func(cc *types.CacheCluster) bool {
		return aws.ToInt32(cc.NumCacheNodes) >= prev.NumNodes
	})
	return err
}

// ---------------------------------------------------------------------------------------------
// Single-node Valkey/Redis clusters outside a replication group

func captureSingleNodeCluster(ctx context.Context, client ElasticachesAPI, cc *types.CacheCluster) (json.RawMessage, error) {
	id := aws.ToString(cc.CacheClusterId)
	status := aws.ToString(cc.CacheClusterStatus)
	if elasticacheFailedStatus(status) {
		return nil, resource.ErrAlreadyStopped
	}
	if status != "available" {
		var err error
		if cc, err = waitCacheCluster(ctx, client, id, "settle before capture", nil); err != nil {
			return nil, err
		}
	}
	used, err := elasticacheBytesUsed(ctx, client, []string{id})
	if err != nil {
		return nil, err
	}
	target, err := elasticacheTargetNodeType(aws.ToString(cc.Engine), aws.ToString(cc.EngineVersion), used)
	if err != nil {
		return nil, err
	}
	nodeType := aws.ToString(cc.CacheNodeType)
	if !elasticacheNeedsResize(nodeType, target) {
		return nil, resource.ErrAlreadyStopped
	}
	return marshalPrev(elasticachePrev{Kind: elasticacheKindCacheCluster, NodeType: nodeType, TargetNodeType: target})
}

func modifyCacheClusterNodeType(ctx context.Context, client ElasticachesAPI, id, nodeType string, allowFallback bool) (string, error) {
	err := retryModifyCacheCluster(ctx, client, &elasticache.ModifyCacheClusterInput{
		CacheClusterId:   aws.String(id),
		CacheNodeType:    aws.String(nodeType),
		ApplyImmediately: aws.Bool(true),
	})
	if err != nil && allowFallback && elasticacheNodeTypeRejected(err) {
		if fallback := elasticacheGravitonFallback(nodeType); fallback != "" {
			logging.Debugf("ElastiCache cluster %s rejected %s (%v); trying %s", id, nodeType, err, fallback)
			nodeType = fallback
			err = retryModifyCacheCluster(ctx, client, &elasticache.ModifyCacheClusterInput{
				CacheClusterId:   aws.String(id),
				CacheNodeType:    aws.String(nodeType),
				ApplyImmediately: aws.Bool(true),
			})
		}
	}
	if err != nil {
		return "", fmt.Errorf("changing node type of %s to %s: %w", id, nodeType, err)
	}
	return nodeType, nil
}

func stopSingleNodeCluster(ctx context.Context, client ElasticachesAPI, id string, prev elasticachePrev) error {
	cc, err := describeCacheCluster(ctx, client, id)
	if err != nil {
		return err
	}
	if cc == nil {
		return nil
	}
	if cc, err = waitCacheCluster(ctx, client, id, "settle before stop", nil); err != nil {
		return err
	}
	if !elasticacheNeedsResize(aws.ToString(cc.CacheNodeType), prev.TargetNodeType) {
		return nil
	}
	nodeType, err := modifyCacheClusterNodeType(ctx, client, id, prev.TargetNodeType, true)
	if err != nil {
		return err
	}
	_, err = waitCacheCluster(ctx, client, id, "node type "+nodeType, func(cc *types.CacheCluster) bool {
		return aws.ToString(cc.CacheNodeType) == nodeType
	})
	return err
}

func startSingleNodeCluster(ctx context.Context, client ElasticachesAPI, id string, prev elasticachePrev) error {
	cc, err := describeCacheCluster(ctx, client, id)
	if err != nil {
		return err
	}
	if cc == nil || elasticacheFailedStatus(aws.ToString(cc.CacheClusterStatus)) {
		return resource.ErrGone
	}
	if cc, err = waitCacheCluster(ctx, client, id, "settle before start", nil); err != nil {
		return err
	}
	if aws.ToString(cc.CacheNodeType) == prev.NodeType {
		return nil
	}
	if _, err := modifyCacheClusterNodeType(ctx, client, id, prev.NodeType, false); err != nil {
		return err
	}
	_, err = waitCacheCluster(ctx, client, id, "node type "+prev.NodeType, func(cc *types.CacheCluster) bool {
		return aws.ToString(cc.CacheNodeType) == prev.NodeType
	})
	return err
}
