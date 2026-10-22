package resources

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/aws/smithy-go"
	"github.com/gruntwork-io/cloud-nuke/resource"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	// The mocks change state instantly; keep the waiters from sleeping long.
	elasticacheWaitMinDelay = time.Millisecond
	elasticacheWaitMaxDelay = time.Millisecond
	elasticacheWaitTimeout = 5 * time.Second
}

const gib = float64(1 << 30)

// stopCacheMock is a stateful ElastiCache + CloudWatch fake that enforces the AWS rules the
// hooks depend on (no replica removal with Multi-AZ on, no failover without replicas outside
// cluster mode, ...).
type stopCacheMock struct {
	ElasticachesAPI
	groups    map[string]*types.ReplicationGroup
	clusters  map[string]*types.CacheCluster
	bytes     map[string]float64 // BytesUsedForCache per cache cluster ID
	busy      map[string]int     // describes left that report "modifying"
	deleting  map[string]int     // removed clusters: cluster list calls left before they vanish
	rejectT4g bool
	busyErrs  int // ModifyReplicationGroup calls left that fail with InvalidReplicationGroupState
	calls     []string
	nextNode  int
}

func newStopCacheMock() *stopCacheMock {
	return &stopCacheMock{
		groups:   map[string]*types.ReplicationGroup{},
		clusters: map[string]*types.CacheCluster{},
		bytes:    map[string]float64{},
		busy:     map[string]int{},
		deleting: map[string]int{},
		nextNode: 100,
	}
}

func apiErr(code, msg string) error {
	return &smithy.GenericAPIError{Code: code, Message: msg}
}

// addGroup adds a Valkey replication group with shards × (1 + replicas) nodes.
func (m *stopCacheMock) addGroup(id, nodeType string, clusterMode bool, shards, replicas int, failover, multiAZ bool) *types.ReplicationGroup {
	rg := &types.ReplicationGroup{
		ReplicationGroupId: aws.String(id),
		ARN:                aws.String("arn:aws:elasticache:us-east-2:1:replicationgroup:" + id),
		Status:             aws.String("available"),
		Engine:             aws.String("valkey"),
		CacheNodeType:      aws.String(nodeType),
		ClusterEnabled:     aws.Bool(clusterMode),
		AutomaticFailover:  types.AutomaticFailoverStatusDisabled,
		MultiAZ:            types.MultiAZStatusDisabled,
		DataTiering:        types.DataTieringStatusDisabled,
	}
	if failover {
		rg.AutomaticFailover = types.AutomaticFailoverStatusEnabled
	}
	if multiAZ {
		rg.MultiAZ = types.MultiAZStatusEnabled
	}
	for s := 1; s <= shards; s++ {
		ng := types.NodeGroup{NodeGroupId: aws.String(fmt.Sprintf("%04d", s)), Status: aws.String("available")}
		rg.NodeGroups = append(rg.NodeGroups, ng)
	}
	m.groups[id] = rg
	for i := range rg.NodeGroups {
		for n := 0; n <= replicas; n++ {
			m.addMember(rg, i)
		}
	}
	return rg
}

func (m *stopCacheMock) addMember(rg *types.ReplicationGroup, ngIndex int) {
	m.nextNode++
	cid := fmt.Sprintf("%s-%s-%03d", aws.ToString(rg.ReplicationGroupId), aws.ToString(rg.NodeGroups[ngIndex].NodeGroupId), m.nextNode)
	role := "replica"
	if len(rg.NodeGroups[ngIndex].NodeGroupMembers) == 0 {
		role = "primary"
	}
	rg.NodeGroups[ngIndex].NodeGroupMembers = append(rg.NodeGroups[ngIndex].NodeGroupMembers, types.NodeGroupMember{
		CacheClusterId: aws.String(cid), CacheNodeId: aws.String("0001"), CurrentRole: aws.String(role),
	})
	rg.MemberClusters = append(rg.MemberClusters, cid)
	m.clusters[cid] = &types.CacheCluster{
		CacheClusterId:     aws.String(cid),
		CacheClusterStatus: aws.String("available"),
		Engine:             aws.String("valkey"),
		EngineVersion:      aws.String("8.0.1"),
		CacheNodeType:      rg.CacheNodeType,
		NumCacheNodes:      aws.Int32(1),
		ReplicationGroupId: rg.ReplicationGroupId,
	}
}

func (m *stopCacheMock) removeReplicas(rg *types.ReplicationGroup, ngIndex int, keep int) {
	ng := &rg.NodeGroups[ngIndex]
	for len(ng.NodeGroupMembers) > keep+1 {
		last := ng.NodeGroupMembers[len(ng.NodeGroupMembers)-1]
		ng.NodeGroupMembers = ng.NodeGroupMembers[:len(ng.NodeGroupMembers)-1]
		rg.MemberClusters = slices.DeleteFunc(rg.MemberClusters, func(s string) bool { return s == aws.ToString(last.CacheClusterId) })
		// Like AWS, the removed cluster lingers as "deleting" after the group is available.
		m.deleting[aws.ToString(last.CacheClusterId)] = 4
	}
}

func (m *stopCacheMock) addCluster(id, engine, version, nodeType string, nodes int) *types.CacheCluster {
	cc := &types.CacheCluster{
		CacheClusterId:     aws.String(id),
		CacheClusterStatus: aws.String("available"),
		Engine:             aws.String(engine),
		EngineVersion:      aws.String(version),
		CacheNodeType:      aws.String(nodeType),
		NumCacheNodes:      aws.Int32(int32(nodes)),
	}
	for i := 1; i <= nodes; i++ {
		cc.CacheNodes = append(cc.CacheNodes, types.CacheNode{CacheNodeId: aws.String(fmt.Sprintf("%04d", i))})
	}
	m.clusters[id] = cc
	return cc
}

func (m *stopCacheMock) status(id string) string {
	if m.busy[id] > 0 {
		m.busy[id]--
		return "modifying"
	}
	return "available"
}

func (m *stopCacheMock) DescribeReplicationGroups(_ context.Context, in *elasticache.DescribeReplicationGroupsInput, _ ...func(*elasticache.Options)) (*elasticache.DescribeReplicationGroupsOutput, error) {
	id := aws.ToString(in.ReplicationGroupId)
	rg, ok := m.groups[id]
	if !ok {
		return nil, &types.ReplicationGroupNotFoundFault{Message: aws.String("not found")}
	}
	cp := *rg
	cp.Status = aws.String(m.status(id))
	cp.NodeGroups = slices.Clone(rg.NodeGroups)
	for i := range cp.NodeGroups {
		cp.NodeGroups[i].NodeGroupMembers = slices.Clone(rg.NodeGroups[i].NodeGroupMembers)
	}
	return &elasticache.DescribeReplicationGroupsOutput{ReplicationGroups: []types.ReplicationGroup{cp}}, nil
}

func (m *stopCacheMock) DescribeCacheClusters(_ context.Context, in *elasticache.DescribeCacheClustersInput, _ ...func(*elasticache.Options)) (*elasticache.DescribeCacheClustersOutput, error) {
	id := aws.ToString(in.CacheClusterId)
	if id == "" {
		// Listing: removed replicas show as deleting for a while.
		var out []types.CacheCluster
		for cid, cc := range m.clusters {
			cp := *cc
			if n, ok := m.deleting[cid]; ok {
				cp.CacheClusterStatus = aws.String("deleting")
				if n <= 1 {
					delete(m.deleting, cid)
					delete(m.clusters, cid)
				} else {
					m.deleting[cid] = n - 1
				}
			}
			out = append(out, cp)
		}
		return &elasticache.DescribeCacheClustersOutput{CacheClusters: out}, nil
	}
	cc, ok := m.clusters[id]
	if !ok || m.deleting[id] > 0 {
		return nil, &types.CacheClusterNotFoundFault{Message: aws.String("not found")}
	}
	cp := *cc
	cp.CacheClusterStatus = aws.String(m.status(id))
	cp.CacheNodes = slices.Clone(cc.CacheNodes)
	return &elasticache.DescribeCacheClustersOutput{CacheClusters: []types.CacheCluster{cp}}, nil
}

func (m *stopCacheMock) GetMetricStatistics(_ context.Context, in *cloudwatch.GetMetricStatisticsInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricStatisticsOutput, error) {
	id := aws.ToString(in.Dimensions[0].Value)
	b, ok := m.bytes[id]
	if !ok {
		return &cloudwatch.GetMetricStatisticsOutput{}, nil
	}
	return &cloudwatch.GetMetricStatisticsOutput{Datapoints: []cwtypes.Datapoint{{Maximum: aws.Float64(b / 2)}, {Maximum: aws.Float64(b)}}}, nil
}

func (m *stopCacheMock) replicas(rg *types.ReplicationGroup) int {
	n := 0
	for _, ng := range rg.NodeGroups {
		n = max(n, len(ng.NodeGroupMembers)-1)
	}
	return n
}

func (m *stopCacheMock) checkMembersSettled(rgID string) error {
	for cid := range m.deleting {
		if aws.ToString(m.clusters[cid].ReplicationGroupId) == rgID {
			return apiErr("InvalidCacheClusterState", "Cache cluster '"+cid+"' is not in available state.")
		}
	}
	return nil
}

func (m *stopCacheMock) checkNodeType(nodeType string) error {
	if m.rejectT4g && strings.HasPrefix(nodeType, "cache.t4g.") {
		return apiErr("InvalidParameterCombination", "node type not supported for this engine version")
	}
	return nil
}

func (m *stopCacheMock) ModifyReplicationGroup(_ context.Context, in *elasticache.ModifyReplicationGroupInput, _ ...func(*elasticache.Options)) (*elasticache.ModifyReplicationGroupOutput, error) {
	id := aws.ToString(in.ReplicationGroupId)
	m.calls = append(m.calls, "ModifyReplicationGroup")
	rg, ok := m.groups[id]
	if !ok {
		return nil, &types.ReplicationGroupNotFoundFault{}
	}
	if err := m.checkMembersSettled(id); err != nil {
		return nil, err
	}
	if m.busyErrs > 0 {
		m.busyErrs--
		return nil, apiErr("InvalidReplicationGroupState", "Replication group must be in available state")
	}
	cme := aws.ToBool(rg.ClusterEnabled)
	if in.AutomaticFailoverEnabled != nil {
		if !*in.AutomaticFailoverEnabled && cme {
			return nil, apiErr("InvalidParameterCombination", "cluster mode requires automatic failover")
		}
		if *in.AutomaticFailoverEnabled && !cme && m.replicas(rg) == 0 {
			return nil, apiErr("InvalidParameterCombination", "automatic failover needs a replica")
		}
	}
	if aws.ToBool(in.MultiAZEnabled) && m.replicas(rg) == 0 {
		return nil, apiErr("InvalidParameterCombination", "Multi-AZ needs a replica")
	}
	if in.CacheNodeType != nil {
		if err := m.checkNodeType(*in.CacheNodeType); err != nil {
			return nil, err
		}
		rg.CacheNodeType = in.CacheNodeType
		m.calls[len(m.calls)-1] += " " + *in.CacheNodeType
	}
	if in.AutomaticFailoverEnabled != nil {
		rg.AutomaticFailover = types.AutomaticFailoverStatusDisabled
		if *in.AutomaticFailoverEnabled {
			rg.AutomaticFailover = types.AutomaticFailoverStatusEnabled
		}
	}
	if in.MultiAZEnabled != nil {
		rg.MultiAZ = types.MultiAZStatusDisabled
		if *in.MultiAZEnabled {
			rg.MultiAZ = types.MultiAZStatusEnabled
		}
	}
	m.busy[id] = 2
	return &elasticache.ModifyReplicationGroupOutput{}, nil
}

func (m *stopCacheMock) DecreaseReplicaCount(_ context.Context, in *elasticache.DecreaseReplicaCountInput, _ ...func(*elasticache.Options)) (*elasticache.DecreaseReplicaCountOutput, error) {
	id := aws.ToString(in.ReplicationGroupId)
	m.calls = append(m.calls, "DecreaseReplicaCount")
	rg := m.groups[id]
	if err := m.checkMembersSettled(id); err != nil {
		return nil, err
	}
	if rg.MultiAZ == types.MultiAZStatusEnabled {
		return nil, apiErr("InvalidParameterValue", "Multi-AZ requires a replica")
	}
	if !aws.ToBool(rg.ClusterEnabled) && rg.AutomaticFailover != types.AutomaticFailoverStatusDisabled {
		return nil, apiErr("InvalidParameterValue", "automatic failover requires a replica")
	}
	if aws.ToBool(rg.ClusterEnabled) {
		if in.NewReplicaCount != nil {
			return nil, apiErr("InvalidParameterValue", "use ReplicaConfiguration in cluster mode")
		}
		for _, c := range in.ReplicaConfiguration {
			for i := range rg.NodeGroups {
				if aws.ToString(rg.NodeGroups[i].NodeGroupId) == aws.ToString(c.NodeGroupId) {
					m.removeReplicas(rg, i, int(aws.ToInt32(c.NewReplicaCount)))
				}
			}
		}
	} else {
		m.removeReplicas(rg, 0, int(aws.ToInt32(in.NewReplicaCount)))
	}
	m.busy[id] = 2
	return &elasticache.DecreaseReplicaCountOutput{}, nil
}

func (m *stopCacheMock) IncreaseReplicaCount(_ context.Context, in *elasticache.IncreaseReplicaCountInput, _ ...func(*elasticache.Options)) (*elasticache.IncreaseReplicaCountOutput, error) {
	id := aws.ToString(in.ReplicationGroupId)
	m.calls = append(m.calls, "IncreaseReplicaCount")
	rg := m.groups[id]
	if err := m.checkMembersSettled(id); err != nil {
		return nil, err
	}
	grow := func(i int, want int) {
		for len(rg.NodeGroups[i].NodeGroupMembers) < want+1 {
			m.addMember(rg, i)
		}
	}
	if aws.ToBool(rg.ClusterEnabled) {
		for _, c := range in.ReplicaConfiguration {
			for i := range rg.NodeGroups {
				if aws.ToString(rg.NodeGroups[i].NodeGroupId) == aws.ToString(c.NodeGroupId) {
					grow(i, int(aws.ToInt32(c.NewReplicaCount)))
				}
			}
		}
	} else {
		grow(0, int(aws.ToInt32(in.NewReplicaCount)))
	}
	m.busy[id] = 2
	return &elasticache.IncreaseReplicaCountOutput{}, nil
}

func (m *stopCacheMock) ModifyCacheCluster(_ context.Context, in *elasticache.ModifyCacheClusterInput, _ ...func(*elasticache.Options)) (*elasticache.ModifyCacheClusterOutput, error) {
	id := aws.ToString(in.CacheClusterId)
	m.calls = append(m.calls, "ModifyCacheCluster")
	cc, ok := m.clusters[id]
	if !ok {
		return nil, &types.CacheClusterNotFoundFault{}
	}
	if in.CacheNodeType != nil {
		if err := m.checkNodeType(*in.CacheNodeType); err != nil {
			return nil, err
		}
		cc.CacheNodeType = in.CacheNodeType
	}
	if in.NumCacheNodes != nil {
		want := int(*in.NumCacheNodes)
		if want < len(cc.CacheNodes) {
			if len(in.CacheNodeIdsToRemove) != len(cc.CacheNodes)-want {
				return nil, apiErr("InvalidParameterValue", "wrong number of CacheNodeIdsToRemove")
			}
			cc.CacheNodes = slices.DeleteFunc(cc.CacheNodes, func(n types.CacheNode) bool {
				return slices.Contains(in.CacheNodeIdsToRemove, aws.ToString(n.CacheNodeId))
			})
		}
		for len(cc.CacheNodes) < want {
			m.nextNode++
			cc.CacheNodes = append(cc.CacheNodes, types.CacheNode{CacheNodeId: aws.String(fmt.Sprintf("%04d", m.nextNode))})
		}
		cc.NumCacheNodes = aws.Int32(int32(len(cc.CacheNodes)))
	}
	m.busy[id] = 2
	return &elasticache.ModifyCacheClusterOutput{}, nil
}

func decodeCachePrev(t *testing.T, raw json.RawMessage) elasticachePrev {
	t.Helper()
	var p elasticachePrev
	require.NoError(t, json.Unmarshal(raw, &p))
	return p
}

// roundTrip runs capture → stop → capture (stopped) → start → capture (equal) and returns prev.
func cacheRoundTrip(t *testing.T, m *stopCacheMock, id string) json.RawMessage {
	t.Helper()
	ctx := context.Background()
	prev, err := captureElasticache(ctx, m, aws.String(id))
	require.NoError(t, err)

	require.NoError(t, stopElasticache(ctx, m, aws.String(id), prev))
	_, err = captureElasticache(ctx, m, aws.String(id))
	require.ErrorIs(t, err, resource.ErrAlreadyStopped)

	// Stop again (rerun after a crash) changes nothing.
	calls := len(m.calls)
	require.NoError(t, stopElasticache(ctx, m, aws.String(id), prev))
	assert.Len(t, m.calls, calls, "second stop must not modify anything")

	require.NoError(t, startElasticache(ctx, m, aws.String(id), prev))
	again, err := captureElasticache(ctx, m, aws.String(id))
	require.NoError(t, err)
	assert.JSONEq(t, string(prev), string(again))

	// Start again changes nothing.
	calls = len(m.calls)
	require.NoError(t, startElasticache(ctx, m, aws.String(id), prev))
	assert.Len(t, m.calls, calls, "second start must not modify anything")
	return prev
}

func TestElasticacheStop_ReplicationGroupClusterModeDisabled(t *testing.T) {
	m := newStopCacheMock()
	rg := m.addGroup("rg", "cache.r6g.large", false, 1, 2, true, true)
	for _, c := range rg.MemberClusters {
		m.bytes[c] = 100 << 20
	}
	m.busy["rg"] = 3 // capture waits for a modifying group to settle

	prev := cacheRoundTrip(t, m, "rg")
	assert.JSONEq(t, `{"kind":"replication-group","node_type":"cache.r6g.large","target_node_type":"cache.t4g.micro",
		"automatic_failover":true,"multi_az":true,"replicas":{"0001":2}}`, string(prev))
	assert.Equal(t, []string{
		"ModifyReplicationGroup", "DecreaseReplicaCount", "ModifyReplicationGroup cache.t4g.micro",
		"ModifyReplicationGroup cache.r6g.large", "IncreaseReplicaCount", "ModifyReplicationGroup",
	}, m.calls)

	got := m.groups["rg"]
	assert.Equal(t, "cache.r6g.large", aws.ToString(got.CacheNodeType))
	assert.Len(t, got.NodeGroups[0].NodeGroupMembers, 3)
	assert.Equal(t, types.AutomaticFailoverStatusEnabled, got.AutomaticFailover)
	assert.Equal(t, types.MultiAZStatusEnabled, got.MultiAZ)
}

func TestElasticacheStop_ReplicationGroupStoppedState(t *testing.T) {
	ctx := context.Background()
	m := newStopCacheMock()
	m.addGroup("rg", "cache.m7g.xlarge", false, 1, 1, true, false)
	prev, err := captureElasticache(ctx, m, aws.String("rg"))
	require.NoError(t, err)
	require.NoError(t, stopElasticache(ctx, m, aws.String("rg"), prev))

	got := m.groups["rg"]
	assert.Equal(t, "cache.t4g.micro", aws.ToString(got.CacheNodeType)) // no metrics = empty
	assert.Len(t, got.NodeGroups[0].NodeGroupMembers, 1)
	assert.Equal(t, types.AutomaticFailoverStatusDisabled, got.AutomaticFailover)
	assert.Equal(t, types.MultiAZStatusDisabled, got.MultiAZ)

	require.NoError(t, startElasticache(ctx, m, aws.String("rg"), prev))
	assert.Equal(t, "cache.m7g.xlarge", aws.ToString(got.CacheNodeType))
	assert.Len(t, got.NodeGroups[0].NodeGroupMembers, 2)
	assert.Equal(t, types.AutomaticFailoverStatusEnabled, got.AutomaticFailover)
	assert.Equal(t, types.MultiAZStatusDisabled, got.MultiAZ) // was off, stays off
}

func TestElasticacheStop_ReplicationGroupClusterModeEnabled(t *testing.T) {
	m := newStopCacheMock()
	m.addGroup("cme", "cache.r7g.large", true, 3, 2, true, true)
	prev := cacheRoundTrip(t, m, "cme")
	assert.JSONEq(t, `{"kind":"replication-group","node_type":"cache.r7g.large","target_node_type":"cache.t4g.micro",
		"cluster_mode":true,"automatic_failover":true,"multi_az":true,"replicas":{"0001":2,"0002":2,"0003":2}}`, string(prev))
	got := m.groups["cme"]
	for _, ng := range got.NodeGroups {
		assert.Len(t, ng.NodeGroupMembers, 3)
	}
}

func TestElasticacheStop_ClusterModeEnabledKeepsFailoverWhileStopped(t *testing.T) {
	ctx := context.Background()
	m := newStopCacheMock()
	m.addGroup("cme", "cache.t4g.small", true, 2, 1, true, true)
	prev, err := captureElasticache(ctx, m, aws.String("cme"))
	require.NoError(t, err)
	require.NoError(t, stopElasticache(ctx, m, aws.String("cme"), prev))
	got := m.groups["cme"]
	assert.Equal(t, types.AutomaticFailoverStatusEnabled, got.AutomaticFailover)
	assert.Equal(t, types.MultiAZStatusDisabled, got.MultiAZ)
	assert.Equal(t, "cache.t4g.micro", aws.ToString(got.CacheNodeType))
	for _, ng := range got.NodeGroups {
		assert.Len(t, ng.NodeGroupMembers, 1)
	}
}

func TestElasticacheStop_ResumesPartialStop(t *testing.T) {
	ctx := context.Background()
	m := newStopCacheMock()
	m.addGroup("rg", "cache.r6g.large", false, 1, 2, true, true)
	prev, err := captureElasticache(ctx, m, aws.String("rg"))
	require.NoError(t, err)

	// A previous run turned failover off and then crashed.
	m.groups["rg"].AutomaticFailover = types.AutomaticFailoverStatusDisabled
	m.groups["rg"].MultiAZ = types.MultiAZStatusDisabled
	require.NoError(t, stopElasticache(ctx, m, aws.String("rg"), prev))
	assert.Equal(t, []string{"DecreaseReplicaCount", "ModifyReplicationGroup cache.t4g.micro"}, m.calls)

	// Start after a crash half-way through a start: node type already restored.
	m.groups["rg"].CacheNodeType = aws.String("cache.r6g.large")
	m.calls = nil
	require.NoError(t, startElasticache(ctx, m, aws.String("rg"), prev))
	assert.Equal(t, []string{"IncreaseReplicaCount", "ModifyReplicationGroup"}, m.calls)
}

func TestElasticacheStop_TargetBySize(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		bytes float64
		want  string
	}{
		{0, "cache.t4g.micro"},
		{0.39 * gib, "cache.t4g.micro"},
		{0.41 * gib, "cache.t4g.small"},
		{1.09 * gib, "cache.t4g.small"},
		{1.2 * gib, "cache.t4g.medium"},
		{2.47 * gib, "cache.t4g.medium"},
	} {
		m := newStopCacheMock()
		rg := m.addGroup("rg", "cache.r6g.xlarge", false, 1, 1, false, false)
		m.bytes[rg.MemberClusters[1]] = tc.bytes // the largest node decides
		prev, err := captureElasticache(ctx, m, aws.String("rg"))
		require.NoError(t, err)
		assert.Equal(t, tc.want, decodeCachePrev(t, prev).TargetNodeType, "%.2f GiB", tc.bytes/gib)
	}
}

func TestElasticacheStop_TooLargeIsUnsupported(t *testing.T) {
	m := newStopCacheMock()
	rg := m.addGroup("rg", "cache.r6g.xlarge", false, 1, 1, true, true)
	m.bytes[rg.MemberClusters[0]] = 2.6 * gib
	_, err := captureElasticache(context.Background(), m, aws.String("rg"))
	require.True(t, resource.IsUnsupported(err), "%v", err)
	assert.Contains(t, err.Error(), "data")
}

func TestElasticacheStop_DataTieringAndGlobalDatastoreUnsupported(t *testing.T) {
	m := newStopCacheMock()
	m.addGroup("tier", "cache.r6gd.xlarge", false, 1, 1, true, true).DataTiering = types.DataTieringStatusEnabled
	m.addGroup("global", "cache.r6g.large", false, 1, 1, true, true).GlobalReplicationGroupInfo = &types.GlobalReplicationGroupInfo{
		GlobalReplicationGroupId: aws.String("ldgnf-global"), GlobalReplicationGroupMemberRole: aws.String("PRIMARY"),
	}
	for _, id := range []string{"tier", "global"} {
		_, err := captureElasticache(context.Background(), m, aws.String(id))
		assert.True(t, resource.IsUnsupported(err), "%s: %v", id, err)
	}
}

func TestElasticacheStop_NeverGrowsSmallNodes(t *testing.T) {
	ctx := context.Background()
	m := newStopCacheMock()
	// Already on a micro node holding more than 80% of it: the target is small, but the node
	// is kept; only the replica and failover are removed.
	rg := m.addGroup("rg", "cache.t3.micro", false, 1, 1, true, false)
	m.bytes[rg.MemberClusters[0]] = 0.45 * gib
	prev, err := captureElasticache(ctx, m, aws.String("rg"))
	require.NoError(t, err)
	assert.Equal(t, "cache.t4g.small", decodeCachePrev(t, prev).TargetNodeType)
	require.NoError(t, stopElasticache(ctx, m, aws.String("rg"), prev))
	assert.Equal(t, "cache.t3.micro", aws.ToString(m.groups["rg"].CacheNodeType))
	_, err = captureElasticache(ctx, m, aws.String("rg"))
	assert.ErrorIs(t, err, resource.ErrAlreadyStopped)
}

func TestElasticacheStop_AlreadyStopped(t *testing.T) {
	m := newStopCacheMock()
	m.addGroup("rg", "cache.t4g.micro", false, 1, 0, false, false)
	m.addGroup("cme", "cache.t4g.micro", true, 2, 0, true, false) // failover is required in cluster mode
	m.addCluster("mc", "memcached", "1.6.22", "cache.r6g.large", 1)
	m.addCluster("single", "redis", "7.1.0", "cache.t4g.micro", 1)
	for _, id := range []string{"rg", "cme", "mc", "single", "missing"} {
		_, err := captureElasticache(context.Background(), m, aws.String(id))
		assert.ErrorIs(t, err, resource.ErrAlreadyStopped, id)
	}
}

func TestElasticacheStop_GravitonRejectedFallsBackToT3(t *testing.T) {
	m := newStopCacheMock()
	m.addGroup("rg", "cache.r5.large", false, 1, 1, true, true)
	m.rejectT4g = true
	prev := cacheRoundTrip(t, m, "rg")
	assert.Equal(t, "cache.t4g.micro", decodeCachePrev(t, prev).TargetNodeType)
	assert.Contains(t, m.calls, "ModifyReplicationGroup cache.t3.micro")
}

func TestElasticacheStop_OldRedisUsesT3(t *testing.T) {
	assert.True(t, elasticacheSupportsGraviton("valkey", "7.2"))
	assert.True(t, elasticacheSupportsGraviton("redis", "5.0.6"))
	assert.True(t, elasticacheSupportsGraviton("redis", "6.x"))
	assert.False(t, elasticacheSupportsGraviton("redis", "5.0.5"))
	assert.False(t, elasticacheSupportsGraviton("redis", "4.0.10"))

	m := newStopCacheMock()
	m.addCluster("old", "redis", "4.0.10", "cache.m5.large", 1)
	prev := cacheRoundTrip(t, m, "old")
	assert.JSONEq(t, `{"kind":"cache-cluster","node_type":"cache.m5.large","target_node_type":"cache.t3.micro"}`, string(prev))
}

func TestElasticacheStop_SingleNodeCluster(t *testing.T) {
	m := newStopCacheMock()
	m.addCluster("single", "valkey", "8.0", "cache.r7g.large", 1)
	m.bytes["single"] = 0.9 * gib
	prev := cacheRoundTrip(t, m, "single")
	assert.JSONEq(t, `{"kind":"cache-cluster","node_type":"cache.r7g.large","target_node_type":"cache.t4g.small"}`, string(prev))
	assert.Equal(t, []string{"ModifyCacheCluster", "ModifyCacheCluster"}, m.calls)
}

func TestElasticacheStop_Memcached(t *testing.T) {
	m := newStopCacheMock()
	m.addCluster("mc", "memcached", "1.6.22", "cache.t4g.micro", 3)
	m.busy["mc"] = 2
	prev := cacheRoundTrip(t, m, "mc")
	assert.JSONEq(t, `{"kind":"memcached","num_nodes":3}`, string(prev))
	assert.Equal(t, 3, len(m.clusters["mc"].CacheNodes))
	assert.Equal(t, "0001", aws.ToString(m.clusters["mc"].CacheNodes[0].CacheNodeId), "the first node is kept")
}

func TestElasticacheStop_StartGone(t *testing.T) {
	m := newStopCacheMock()
	for _, prev := range []string{
		`{"kind":"replication-group","node_type":"cache.r6g.large","target_node_type":"cache.t4g.micro","replicas":{"0001":1}}`,
		`{"kind":"memcached","num_nodes":2}`,
		`{"kind":"cache-cluster","node_type":"cache.r6g.large","target_node_type":"cache.t4g.micro"}`,
	} {
		err := startElasticache(context.Background(), m, aws.String("missing"), json.RawMessage(prev))
		assert.ErrorIs(t, err, resource.ErrGone, prev)
	}
}

func TestElasticacheStop_RetriesWhileBusy(t *testing.T) {
	ctx := context.Background()
	m := newStopCacheMock()
	m.addGroup("rg", "cache.r6g.large", false, 1, 1, true, true)
	prev, err := captureElasticache(ctx, m, aws.String("rg"))
	require.NoError(t, err)
	m.busyErrs = 3
	require.NoError(t, stopElasticache(ctx, m, aws.String("rg"), prev))
	assert.Equal(t, "cache.t4g.micro", aws.ToString(m.groups["rg"].CacheNodeType))
}
