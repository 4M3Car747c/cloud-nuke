package resources

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/gruntwork-io/cloud-nuke/resource"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stopEKSNodegroup is the mock's state for one managed node group.
type stopEKSNodegroup struct {
	status  ekstypes.NodegroupStatus
	scaling eksNodegroupScaling
	// settleAfter is how many more DescribeNodegroup calls report the transitional status
	// before the group becomes ACTIVE.
	settleAfter int
	// busy is how many more UpdateNodegroupConfig calls fail with ResourceInUseException.
	busy int
}

// stopEKSMock keeps clusters, their node groups and Fargate profiles.
type stopEKSMock struct {
	EKSClustersAPI
	clusters map[string]map[string]*stopEKSNodegroup
	fargate  map[string][]string
	updates  int
}

func (m *stopEKSMock) DescribeCluster(_ context.Context, in *eks.DescribeClusterInput, _ ...func(*eks.Options)) (*eks.DescribeClusterOutput, error) {
	if _, ok := m.clusters[aws.ToString(in.Name)]; !ok {
		return nil, &ekstypes.ResourceNotFoundException{Message: aws.String("no cluster")}
	}
	return &eks.DescribeClusterOutput{Cluster: &ekstypes.Cluster{Name: in.Name, Status: ekstypes.ClusterStatusActive}}, nil
}

func (m *stopEKSMock) ListNodegroups(_ context.Context, in *eks.ListNodegroupsInput, _ ...func(*eks.Options)) (*eks.ListNodegroupsOutput, error) {
	ngs, ok := m.clusters[aws.ToString(in.ClusterName)]
	if !ok {
		return nil, &ekstypes.ResourceNotFoundException{Message: aws.String("no cluster")}
	}
	var names []string
	for n := range ngs {
		names = append(names, n)
	}
	sort.Strings(names)
	return &eks.ListNodegroupsOutput{Nodegroups: names}, nil
}

func (m *stopEKSMock) ListFargateProfiles(_ context.Context, in *eks.ListFargateProfilesInput, _ ...func(*eks.Options)) (*eks.ListFargateProfilesOutput, error) {
	return &eks.ListFargateProfilesOutput{FargateProfileNames: m.fargate[aws.ToString(in.ClusterName)]}, nil
}

func (m *stopEKSMock) nodegroup(cluster, name *string) (*stopEKSNodegroup, error) {
	ng, ok := m.clusters[aws.ToString(cluster)][aws.ToString(name)]
	if !ok {
		return nil, &ekstypes.ResourceNotFoundException{Message: aws.String("no node group")}
	}
	return ng, nil
}

func (m *stopEKSMock) DescribeNodegroup(_ context.Context, in *eks.DescribeNodegroupInput, _ ...func(*eks.Options)) (*eks.DescribeNodegroupOutput, error) {
	ng, err := m.nodegroup(in.ClusterName, in.NodegroupName)
	if err != nil {
		return nil, err
	}
	out := &eks.DescribeNodegroupOutput{Nodegroup: &ekstypes.Nodegroup{
		NodegroupName: in.NodegroupName,
		Status:        ng.status,
		ScalingConfig: &ekstypes.NodegroupScalingConfig{
			MinSize:     aws.Int32(ng.scaling.Min),
			MaxSize:     aws.Int32(ng.scaling.Max),
			DesiredSize: aws.Int32(ng.scaling.Desired),
		},
	}}
	if eksNodegroupTransitional(ng.status) {
		if ng.settleAfter <= 0 {
			ng.status = ekstypes.NodegroupStatusActive
		}
		ng.settleAfter--
	}
	return out, nil
}

func (m *stopEKSMock) UpdateNodegroupConfig(_ context.Context, in *eks.UpdateNodegroupConfigInput, _ ...func(*eks.Options)) (*eks.UpdateNodegroupConfigOutput, error) {
	ng, err := m.nodegroup(in.ClusterName, in.NodegroupName)
	if err != nil {
		return nil, err
	}
	if ng.busy > 0 || eksNodegroupTransitional(ng.status) {
		ng.busy--
		return nil, &ekstypes.ResourceInUseException{Message: aws.String("update in progress")}
	}
	c := in.ScalingConfig
	ng.scaling = eksNodegroupScaling{Min: aws.ToInt32(c.MinSize), Max: aws.ToInt32(c.MaxSize), Desired: aws.ToInt32(c.DesiredSize)}
	ng.status = ekstypes.NodegroupStatusUpdating
	ng.settleAfter = 1
	m.updates++
	return &eks.UpdateNodegroupConfigOutput{}, nil
}

func eksNG(min, desired, max int32) *stopEKSNodegroup {
	return &stopEKSNodegroup{status: ekstypes.NodegroupStatusActive, scaling: eksNodegroupScaling{Min: min, Max: max, Desired: desired}}
}

func fastEKSPolling(t *testing.T) {
	old := eksNodegroupPollDelay
	eksNodegroupPollDelay = time.Millisecond
	t.Cleanup(func() { eksNodegroupPollDelay = old })
}

func TestEKSStop_RoundTrip(t *testing.T) {
	fastEKSPolling(t)
	ctx := context.Background()
	creating := eksNG(0, 1, 1)
	creating.status = ekstypes.NodegroupStatusCreating
	creating.settleAfter = 2
	m := &stopEKSMock{clusters: map[string]map[string]*stopEKSNodegroup{"c1": {
		"a":    eksNG(1, 2, 3),
		"idle": eksNG(0, 0, 2), // already scaled to zero: not touched, not restored
		"new":  creating,       // waited for before reading its config
	}}}
	id := aws.String("c1")

	prev, err := captureEKSCluster(ctx, m, id)
	require.NoError(t, err)
	assert.JSONEq(t, `{"nodegroups":{"a":{"min":1,"max":3,"desired":2},"new":{"min":0,"max":1,"desired":1}}}`, string(prev))

	require.NoError(t, stopEKSCluster(ctx, m, id, prev))
	assert.Equal(t, eksNodegroupScaling{Min: 0, Max: 3, Desired: 0}, m.clusters["c1"]["a"].scaling)
	assert.Equal(t, eksNodegroupScaling{Min: 0, Max: 1, Desired: 0}, m.clusters["c1"]["new"].scaling)
	assert.Equal(t, 2, m.updates)

	_, err = captureEKSCluster(ctx, m, id)
	assert.ErrorIs(t, err, resource.ErrAlreadyStopped)

	// Stopping again changes nothing.
	require.NoError(t, stopEKSCluster(ctx, m, id, prev))
	assert.Equal(t, 2, m.updates)

	require.NoError(t, startEKSCluster(ctx, m, id, prev))
	assert.Equal(t, eksNodegroupScaling{Min: 0, Max: 2, Desired: 0}, m.clusters["c1"]["idle"].scaling)
	got, err := captureEKSCluster(ctx, m, id)
	require.NoError(t, err)
	assert.JSONEq(t, string(prev), string(got))

	// Starting again changes nothing.
	require.NoError(t, startEKSCluster(ctx, m, id, prev))
	assert.Equal(t, 4, m.updates)
}

func TestEKSStop_RetriesWhileUpdateInProgress(t *testing.T) {
	fastEKSPolling(t)
	ctx := context.Background()
	ng := eksNG(1, 1, 2)
	ng.busy = 2
	m := &stopEKSMock{clusters: map[string]map[string]*stopEKSNodegroup{"c1": {"a": ng}}}
	id := aws.String("c1")

	prev, err := captureEKSCluster(ctx, m, id)
	require.NoError(t, err)
	require.NoError(t, stopEKSCluster(ctx, m, id, prev))
	assert.Equal(t, eksNodegroupScaling{Min: 0, Max: 2, Desired: 0}, ng.scaling)

	// Start right away, while the stop update is still UPDATING.
	ng.busy = 1
	require.NoError(t, startEKSCluster(ctx, m, id, prev))
	assert.Equal(t, eksNodegroupScaling{Min: 1, Max: 2, Desired: 1}, ng.scaling)
}

func TestEKSStop_GivesUpWhenAlwaysBusy(t *testing.T) {
	fastEKSPolling(t)
	ng := eksNG(1, 1, 2)
	ng.busy = 100
	m := &stopEKSMock{clusters: map[string]map[string]*stopEKSNodegroup{"c1": {"a": ng}}}
	err := stopEKSCluster(context.Background(), m, aws.String("c1"), []byte(`{"nodegroups":{"a":{"min":1,"max":2,"desired":1}}}`))
	assert.Error(t, err)
	assert.True(t, hasErrorCode(err, "ResourceInUseException"))
}

func TestEKSStop_AlreadyStopped(t *testing.T) {
	ctx := context.Background()
	m := &stopEKSMock{clusters: map[string]map[string]*stopEKSNodegroup{
		"empty":  {},
		"zeroed": {"a": eksNG(0, 0, 3)},
	}}
	for _, c := range []string{"empty", "zeroed", "missing"} {
		_, err := captureEKSCluster(ctx, m, aws.String(c))
		assert.ErrorIs(t, err, resource.ErrAlreadyStopped, c)
	}
}

func TestEKSStop_FargateOnlyIsUnsupported(t *testing.T) {
	m := &stopEKSMock{
		clusters: map[string]map[string]*stopEKSNodegroup{"c1": {}},
		fargate:  map[string][]string{"c1": {"default"}},
	}
	_, err := captureEKSCluster(context.Background(), m, aws.String("c1"))
	assert.True(t, resource.IsUnsupported(err), err)
}

func TestEKSStop_StartGone(t *testing.T) {
	ctx := context.Background()
	prev := []byte(`{"nodegroups":{"a":{"min":1,"max":2,"desired":1},"b":{"min":0,"max":2,"desired":2}}}`)

	m := &stopEKSMock{clusters: map[string]map[string]*stopEKSNodegroup{}}
	assert.ErrorIs(t, startEKSCluster(ctx, m, aws.String("c1"), prev), resource.ErrGone)

	// The cluster exists but every saved node group was deleted.
	m.clusters["c1"] = map[string]*stopEKSNodegroup{"other": eksNG(0, 0, 1)}
	assert.ErrorIs(t, startEKSCluster(ctx, m, aws.String("c1"), prev), resource.ErrGone)
	assert.Equal(t, 0, m.updates)

	// One of them still exists: restore it, skip the other.
	m.clusters["c1"]["b"] = eksNG(0, 0, 2)
	require.NoError(t, startEKSCluster(ctx, m, aws.String("c1"), prev))
	assert.Equal(t, eksNodegroupScaling{Min: 0, Max: 2, Desired: 2}, m.clusters["c1"]["b"].scaling)
}

func TestEKSStop_StopSkipsDeletedNodegroup(t *testing.T) {
	m := &stopEKSMock{clusters: map[string]map[string]*stopEKSNodegroup{"c1": {"b": eksNG(1, 1, 1)}}}
	prev := []byte(`{"nodegroups":{"a":{"min":1,"max":2,"desired":1},"b":{"min":1,"max":1,"desired":1}}}`)
	require.NoError(t, stopEKSCluster(context.Background(), m, aws.String("c1"), prev))
	assert.Equal(t, eksNodegroupScaling{Min: 0, Max: 1, Desired: 0}, m.clusters["c1"]["b"].scaling)
}
