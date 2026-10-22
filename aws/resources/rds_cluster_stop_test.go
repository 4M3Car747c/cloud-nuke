package resources

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/gruntwork-io/cloud-nuke/resource"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stopRDSClusterMock simulates DB clusters whose status changes with stop/start calls.
type stopRDSClusterMock struct {
	DBClustersAPI
	clusters map[string]*types.DBCluster
	members  map[string]string // member instance id -> status (all in every cluster)
	stopErr  error
	stopped  []string
	started  []string
}

func (m *stopRDSClusterMock) DescribeDBClusters(_ context.Context, in *rds.DescribeDBClustersInput, _ ...func(*rds.Options)) (*rds.DescribeDBClustersOutput, error) {
	var out []types.DBCluster
	for id, c := range m.clusters {
		if in.DBClusterIdentifier != nil && *in.DBClusterIdentifier != id {
			continue
		}
		out = append(out, *c)
		if next, ok := rdsTransitions[aws.ToString(c.Status)]; ok {
			c.Status = aws.String(next)
		}
	}
	if in.DBClusterIdentifier != nil && len(out) == 0 {
		return nil, &types.DBClusterNotFoundFault{Message: aws.String("not found")}
	}
	return &rds.DescribeDBClustersOutput{DBClusters: out}, nil
}

func (m *stopRDSClusterMock) DescribeDBInstances(_ context.Context, in *rds.DescribeDBInstancesInput, _ ...func(*rds.Options)) (*rds.DescribeDBInstancesOutput, error) {
	var out []types.DBInstance
	for id, status := range m.members {
		out = append(out, types.DBInstance{DBInstanceIdentifier: aws.String(id), DBInstanceStatus: aws.String(status)})
		if next, ok := rdsTransitions[status]; ok {
			m.members[id] = next
		}
	}
	return &rds.DescribeDBInstancesOutput{DBInstances: out}, nil
}

func (m *stopRDSClusterMock) StopDBCluster(_ context.Context, in *rds.StopDBClusterInput, _ ...func(*rds.Options)) (*rds.StopDBClusterOutput, error) {
	if m.stopErr != nil {
		return nil, m.stopErr
	}
	id := aws.ToString(in.DBClusterIdentifier)
	m.stopped = append(m.stopped, id)
	m.clusters[id].Status = aws.String("stopping")
	return &rds.StopDBClusterOutput{}, nil
}

func (m *stopRDSClusterMock) StartDBCluster(_ context.Context, in *rds.StartDBClusterInput, _ ...func(*rds.Options)) (*rds.StartDBClusterOutput, error) {
	id := aws.ToString(in.DBClusterIdentifier)
	m.started = append(m.started, id)
	m.clusters[id].Status = aws.String("starting")
	return &rds.StartDBClusterOutput{}, nil
}

func rdsCluster(id, status string) *types.DBCluster {
	return &types.DBCluster{
		DBClusterIdentifier: aws.String(id),
		Status:              aws.String(status),
		Engine:              aws.String("aurora-postgresql"),
		EngineMode:          aws.String("provisioned"),
	}
}

func TestRDSClusterStop_RoundTrip(t *testing.T) {
	fastRDSPolling(t)
	m := &stopRDSClusterMock{clusters: map[string]*types.DBCluster{"cl": rdsCluster("cl", "available")}}
	ctx := context.Background()
	id := aws.String("cl")

	prev, err := captureDBCluster(ctx, m, id)
	require.NoError(t, err)
	assert.JSONEq(t, `{"status":"available"}`, string(prev))

	require.NoError(t, stopDBCluster(ctx, m, id, prev))
	require.NoError(t, stopDBCluster(ctx, m, id, prev))
	assert.Equal(t, []string{"cl"}, m.stopped, "second stop is a no-op")

	_, err = captureDBCluster(ctx, m, id)
	assert.ErrorIs(t, err, resource.ErrAlreadyStopped)

	require.NoError(t, startDBCluster(ctx, m, id, prev))
	assert.Equal(t, []string{"cl"}, m.started)

	again, err := captureDBCluster(ctx, m, id)
	require.NoError(t, err)
	assert.JSONEq(t, string(prev), string(again))

	require.NoError(t, startDBCluster(ctx, m, id, prev))
	assert.Len(t, m.started, 1)
}

func TestRDSClusterStop_StartWhileStopping(t *testing.T) {
	fastRDSPolling(t)
	m := &stopRDSClusterMock{clusters: map[string]*types.DBCluster{"cl": rdsCluster("cl", "stopping")}}
	require.NoError(t, startDBCluster(context.Background(), m, aws.String("cl"), json.RawMessage(`{"status":"available"}`)))
	assert.Equal(t, []string{"cl"}, m.started)
}

func TestRDSClusterStop_CaptureStates(t *testing.T) {
	fastRDSPolling(t)
	ctx := context.Background()
	for _, status := range []string{"stopped", "stopping", "deleting"} {
		m := &stopRDSClusterMock{clusters: map[string]*types.DBCluster{"cl": rdsCluster("cl", status)}}
		_, err := captureDBCluster(ctx, m, aws.String("cl"))
		assert.ErrorIs(t, err, resource.ErrAlreadyStopped, status)
	}
	m := &stopRDSClusterMock{clusters: map[string]*types.DBCluster{}}
	_, err := captureDBCluster(ctx, m, aws.String("missing"))
	assert.ErrorIs(t, err, resource.ErrAlreadyStopped)

	m = &stopRDSClusterMock{clusters: map[string]*types.DBCluster{"cl": rdsCluster("cl", "creating")}}
	prev, err := captureDBCluster(ctx, m, aws.String("cl"))
	require.NoError(t, err, "creating is waited out")
	assert.JSONEq(t, `{"status":"available"}`, string(prev))

	// The cluster says available while a member instance is still being created.
	withMember := rdsCluster("cl", "available")
	withMember.DBClusterMembers = []types.DBClusterMember{{DBInstanceIdentifier: aws.String("cl-1")}}
	m = &stopRDSClusterMock{clusters: map[string]*types.DBCluster{"cl": withMember}, members: map[string]string{"cl-1": "creating"}}
	_, err = captureDBCluster(ctx, m, aws.String("cl"))
	require.NoError(t, err)
	assert.Equal(t, "available", m.members["cl-1"], "capture waited for the member")

	m.members["cl-1"] = "failed"
	_, err = captureDBCluster(ctx, m, aws.String("cl"))
	require.Error(t, err, "a broken member blocks the stop")

	m = &stopRDSClusterMock{clusters: map[string]*types.DBCluster{"cl": rdsCluster("cl", "inaccessible-encryption-credentials")}}
	_, err = captureDBCluster(ctx, m, aws.String("cl"))
	require.Error(t, err)
	assert.False(t, resource.IsUnsupported(err))
}

func TestRDSClusterStop_Unsupported(t *testing.T) {
	ctx := context.Background()
	v1 := rdsCluster("v1", "available")
	v1.EngineMode = aws.String("serverless")
	replica := rdsCluster("replica", "available")
	replica.ReplicationSourceIdentifier = aws.String("arn:aws:rds:us-east-1:1:cluster:src")
	source := rdsCluster("source", "available")
	source.ReadReplicaIdentifiers = []string{"arn:aws:rds:eu-west-1:1:cluster:replica"}
	m := &stopRDSClusterMock{clusters: map[string]*types.DBCluster{"v1": v1, "replica": replica, "source": source}}
	for id := range m.clusters {
		_, err := captureDBCluster(ctx, m, aws.String(id))
		assert.True(t, resource.IsUnsupported(err), "%s: %v", id, err)
	}

	prev := json.RawMessage(`{"status":"available"}`)
	m = &stopRDSClusterMock{
		clusters: map[string]*types.DBCluster{"g": rdsCluster("g", "available")},
		stopErr:  &types.InvalidDBClusterStateFault{Message: aws.String("Stop-db-cluster is not supported for global clusters with more than one member")},
	}
	err := stopDBCluster(ctx, m, aws.String("g"), prev)
	assert.True(t, resource.IsUnsupported(err), err)

	for _, transient := range []error{
		&types.InvalidDBClusterStateFault{Message: aws.String("DbCluster g is not in available state.")},
		// Real StopDBCluster error while a member is still being created; the identifier
		// must not be mistaken for a reason ("replica").
		&types.InvalidDBInstanceStateFault{Message: aws.String("Instance global-replica-1 is not in available state.")},
	} {
		m.stopErr = transient
		err = stopDBCluster(ctx, m, aws.String("g"), prev)
		require.Error(t, err)
		assert.False(t, resource.IsUnsupported(err), err)
	}
}

func TestRDSClusterStop_StartGone(t *testing.T) {
	ctx := context.Background()
	prev := json.RawMessage(`{"status":"available"}`)
	m := &stopRDSClusterMock{clusters: map[string]*types.DBCluster{}}
	assert.ErrorIs(t, startDBCluster(ctx, m, aws.String("missing"), prev), resource.ErrGone)
	m.clusters["del"] = rdsCluster("del", "deleting")
	assert.ErrorIs(t, startDBCluster(ctx, m, aws.String("del"), prev), resource.ErrGone)
}

func TestDBClusters_IsStoppable(t *testing.T) {
	r, ok := NewDBClusters().(resource.StoppableResource)
	require.True(t, ok)
	assert.True(t, r.Stoppable())
}
